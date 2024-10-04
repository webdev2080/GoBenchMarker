package benchmark

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"gobenchmarker/config"
	"gobenchmarker/progress"

	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/objectstorage"
	"golang.org/x/time/rate"
)

// RunIOPSBenchmark runs the benchmark for PUT, GET, and DELETE operations
func RunIOPSBenchmark(params BenchmarkParams, configFilePath string, namespaceOverride string, hostOverride string, prefixOverride string) {
	// Load OCI config and initialize the ObjectStorage client
	provider, err := config.LoadOCIConfig(configFilePath)
	if err != nil {
		fmt.Printf("Error loading OCI config: %v\n", err)
		return
	}

	// Disable TLS certificate verification (only for development)
	customTransport := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	customClient := &http.Client{Transport: customTransport}

	// Use the custom HTTP client with the OCI SDK
	client, err := objectstorage.NewObjectStorageClientWithConfigurationProvider(provider)
	client.HTTPClient = customClient
	if err != nil {
		panic(err)
	}

	// Use the hostOverride if provided, otherwise use the SDK default
	if hostOverride != "" {
		fmt.Println("Using custom host: ", hostOverride)
		client.Host = hostOverride
	}

	// Generate a prefix or use the provided one
	prefix := prefixOverride
	if prefix == "" {
		prefix = fmt.Sprintf("default-%d/", time.Now().UnixNano())
	}

	// Determine namespace: Use provided namespace, or fetch it via API
	namespace := namespaceOverride
	if namespace == "" {
		// No namespace provided, fetch it via the API
		namespaceResp, err := client.GetNamespace(context.TODO(), objectstorage.GetNamespaceRequest{})
		if err != nil {
			panic(err)
		}
		namespace = *namespaceResp.Value
		fmt.Println("Fetched namespace: ", namespace)
	} else {
		fmt.Println("Using provided namespace: ", namespace)
	}

	// Create log file to track errors
	timestamp := time.Now().Format("20060102_150405")
	logFileName := fmt.Sprintf("iops_logs_%s.txt", timestamp)
	logFile, err := os.Create(logFileName)
	if err != nil {
		panic(fmt.Sprintf("Failed to create log file: %s", err.Error()))
	}
	defer logFile.Close()

	// Initialize progress bar
	pb := progress.NewProgressBar(int64(params.ObjectCount))
	pb.SetCaption("IOPS Operations")

	// Set up concurrency control
	var wg sync.WaitGroup
	var objectIndex int64 // For efficient counting using atomic operations

	// Use a mutex to synchronize access to objectNames
	var mutex sync.Mutex
	objectNames := make([]string, params.ObjectCount) // Track uploaded object names for GET and DELETE

	// Record the start time
	startTime := time.Now()

	// Global context handling: If duration is set, use it; otherwise, use background context
	var globalCtx context.Context
	var globalCancel context.CancelFunc

	if params.Duration > 0 {
		globalCtx, globalCancel = context.WithTimeout(context.Background(), params.Duration)
	} else {
		globalCtx, globalCancel = context.WithCancel(context.Background()) // No timeout; cancel manually after object count is reached
	}
	defer globalCancel()

	// Create rate limiter if specified
	var rateLimiter *rate.Limiter
	if params.RateLimit > 0 {
		rateLimiter = rate.NewLimiter(rate.Limit(params.RateLimit), 1) // Create a rate limiter based on the specified rate
		fmt.Println("Rate limiter: ", params.RateLimit)
	}

	// -------------------
	// PUT Operation Phase
	// -------------------
	fmt.Println("Starting PUT operations...")

	// Ensure that all objects are PUT before GET and DELETE
	for i := 0; i < params.ObjectCount; i++ {
		wg.Add(1)
		go func(currentIndex int) {
			defer wg.Done()

			// Generate object name and prepare data
			objectName, err := GeneratePrefixedObjectName(prefix, 8)
			if err != nil {
				fmt.Fprintf(logFile, "Error generating object name: %v\n", err)
				return
			}

			// Protect access to objectNames with the mutex
			mutex.Lock()
			objectNames[currentIndex] = objectName
			mutex.Unlock()

			// Reuse the buffer from the pool
			data := GetBuffer(int(params.ObjectSize))
			defer PutBuffer(data) // Return buffer to the pool after usage

			request := objectstorage.PutObjectRequest{
				NamespaceName: common.String(namespace),
				BucketName:    common.String(params.BucketName),
				ObjectName:    common.String(objectName),
				PutObjectBody: nopCloser{bytes.NewReader(data[:params.ObjectSize])},
			}

			// Apply rate limiting if specified
			if rateLimiter != nil {
				err := rateLimiter.Wait(globalCtx) // Wait for rate limiter if applied
				if err != nil {
					fmt.Fprintf(logFile, "Rate limiter error: %s\n", err.Error())
					return
				}
			}

			// Use per-request timeout
			reqCtx, reqCancel := context.WithTimeout(globalCtx, 30*time.Second) // Set per-request timeout
			defer reqCancel()

			// Retry logic for uploading objects with detailed error logging
			err = uploadWithRetry(client, request, 5, logFile, reqCtx) // Pass per-request context to handle timeout
			if err != nil {
				// Log specific errors, including potential 429 Too Many Requests
				if serviceErr, ok := common.IsServiceError(err); ok && serviceErr.GetHTTPStatusCode() == 429 {
					fmt.Fprintf(logFile, "429 Too Many Requests: Throttling detected for object %s\n", objectName)
				} else {
					fmt.Fprintf(logFile, "Error uploading object %s after retries: %s\n", objectName, err.Error())
				}
			}

			// Update progress bar
			pb.Increment()

		}(i)
	}
	wg.Wait()

	// Verify that all objectNames are populated
	for i, name := range objectNames {
		if name == "" {
			fmt.Fprintf(logFile, "Error: objectNames[%d] is not populated!\n", i)
			panic(fmt.Sprintf("Error: objectNames[%d] is not populated!\n", i))
		}
	}

	// -------------------
	// GET Operation Phase
	// -------------------
	fmt.Println("Starting GET operations...")

	objectIndex = 0 // Reset the object index
	wg.Add(params.Concurrency)
	for i := 0; i < params.Concurrency; i++ {
		go func() {
			defer wg.Done()
			for {
				select {
				case <-globalCtx.Done():
					// Stop if the global context is canceled (either by timeout or manual stop)
					return
				default:
					// Atomically increment the object index
					currentIndex := atomic.AddInt64(&objectIndex, 1) - 1
					if currentIndex >= int64(params.ObjectCount) && params.ObjectCount > 0 {
						globalCancel() // Manually stop when object count is reached
						return
					}

					// Retrieve the object name (safely using mutex)
					mutex.Lock()
					objectName := objectNames[currentIndex]
					mutex.Unlock()

					// Prepare GET request
					getReq := objectstorage.GetObjectRequest{
						NamespaceName: common.String(namespace),
						BucketName:    common.String(params.BucketName),
						ObjectName:    common.String(objectName),
					}

					// Apply rate limiting if specified
					if rateLimiter != nil {
						err := rateLimiter.Wait(globalCtx) // Wait for rate limiter if applied
						if err != nil {
							fmt.Fprintf(logFile, "Rate limiter error: %s\n", err.Error())
							return
						}
					}

					// Use per-request timeout
					reqCtx, reqCancel := context.WithTimeout(globalCtx, 30*time.Second)
					defer reqCancel()

					// Perform GET operation with retry
					resp, err := client.GetObject(reqCtx, getReq)
					if err != nil {
						// Handle error
						fmt.Fprintf(logFile, "Error getting object %s: %v\n", objectName, err)
						continue
					}
					defer resp.Content.Close()

					// Discard response body (for benchmarking purposes)
					_, err = io.Copy(io.Discard, resp.Content)
					if err != nil {
						fmt.Fprintf(logFile, "Error reading object %s: %v\n", objectName, err)
					}

					// Update progress bar
					pb.Increment()
				}
			}
		}()
	}
	wg.Wait()

	// ----------------------
	// DELETE Operation Phase
	// ----------------------
	fmt.Println("Starting DELETE operations...")

	objectIndex = 0 // Reset object index for DELETE
	wg.Add(params.Concurrency)
	for i := 0; i < params.Concurrency; i++ {
		go func() {
			defer wg.Done()
			for {
				select {
				case <-globalCtx.Done():
					// Stop if the global context is canceled (either by timeout or manual stop)
					return
				default:
					// Atomically increment the object index
					currentIndex := atomic.AddInt64(&objectIndex, 1) - 1
					if currentIndex >= int64(params.ObjectCount) && params.ObjectCount > 0 {
						globalCancel() // Manually stop when object count is reached
						return
					}

					// Retrieve the object name (safely using mutex)
					mutex.Lock()
					objectName := objectNames[currentIndex]
					mutex.Unlock()

					// Prepare DELETE request
					deleteReq := objectstorage.DeleteObjectRequest{
						NamespaceName: common.String(namespace),
						BucketName:    common.String(params.BucketName),
						ObjectName:    common.String(objectName),
					}

					// Apply rate limiting if specified
					if rateLimiter != nil {
						err := rateLimiter.Wait(globalCtx)
						if err != nil {
							fmt.Fprintf(logFile, "Rate limiter error: %s\n", err.Error())
							return
						}
					}

					// Use per-request timeout
					reqCtx, reqCancel := context.WithTimeout(globalCtx, 30*time.Second)
					defer reqCancel()

					// Perform DELETE operation with retry
					_, err := client.DeleteObject(reqCtx, deleteReq)
					if err != nil {
						// Handle error
						fmt.Fprintf(logFile, "Error deleting object %s: %v\n", objectName, err)
					}

					// Update progress bar
					pb.Increment()
				}
			}
		}()
	}
	wg.Wait()

	// Finalize the progress bar
	pb.Finish()

	// Calculate and print results
	elapsedTime := time.Since(startTime)
	dataThroughput := (float64(objectIndex) * float64(params.ObjectSize)) / elapsedTime.Seconds() / (1024 * 1024) // MiB/s
	objectThroughput := float64(objectIndex) / elapsedTime.Seconds()                                              // objects/s

	fmt.Println("\nIOPS Results:")
	fmt.Printf("Duration: %v\n", elapsedTime)
	fmt.Printf("Total Objects Processed: %d\n", objectIndex)
	fmt.Printf("Data Throughput: %.2f MiB/s\n", dataThroughput)
	fmt.Printf("Object Throughput: %.2f objects/s\n", objectThroughput)

	// Check if throttling occurred by scanning the log file for 429 errors
	if CheckLogForThrottling(logFileName) {
		fmt.Println("API Throttled: Check iops_logs.txt for more details.")
	} else {
		fmt.Println("No API throttling detected.")
	}
}
