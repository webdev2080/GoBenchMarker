package benchmark

import (
	"bytes"
	"context"
	"fmt"
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

// RunPutBenchmark runs the PUT benchmark, uploading objects concurrently with retry handling
func RunPutBenchmark(params BenchmarkParams, configFilePath string, namespaceOverride string, hostOverride string, prefixOverride string) {
	// Load OCI config and initialize the ObjectStorage client
	provider, err := config.LoadOCIConfig(configFilePath)
	if err != nil {
		fmt.Printf("Error loading OCI config: %v\n", err)
		return // Gracefully exit if config loading fails
	}

	// Use the optimized HTTP client with HTTP/2 support from httpclient.go
	client, err := objectstorage.NewObjectStorageClientWithConfigurationProvider(provider)
	client.HTTPClient = newHTTPClient() // Use the custom client with HTTP/2
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
	logFileName := fmt.Sprintf("put_logs_%s.txt", timestamp)
	logFile, err := os.Create(logFileName)
	if err != nil {
		panic(fmt.Sprintf("Failed to create log file: %s", err.Error()))
	}
	defer logFile.Close()

	// Initialize progress bar
	pb := progress.NewProgressBar(int64(params.ObjectCount))
	pb.SetCaption("Uploading")

	// Set up concurrency control
	var wg sync.WaitGroup
	var objectIndex int64 // Atomic for efficient counting

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
		rateLimiter = rate.NewLimiter(rate.Limit(params.RateLimit), 10) // Reduce rate limit bursts to handle high concurrency
		fmt.Println("Rate limiter: ", params.RateLimit)
	}

	// Connection warm-up phase to prime the connection pool
	fmt.Println("Warming up connections...")
	for i := 0; i < 100; i++ {
		// Send a few initial requests to prime the system
		objectName, err := GeneratePrefixedObjectName(prefix, 8)
		if err != nil {
			fmt.Fprintf(logFile, "Warm-up: Error generating object name: %v\n", err)
			continue
		}
		data := GetBuffer(int(params.ObjectSize))
		request := objectstorage.PutObjectRequest{
			NamespaceName: common.String(namespace),
			BucketName:    common.String(params.BucketName),
			ObjectName:    common.String(objectName),
			PutObjectBody: nopCloser{bytes.NewReader(data)},
		}
		_, err = client.PutObject(context.Background(), request)
		if err != nil {
			fmt.Fprintf(logFile, "Warm-up: Error uploading object %s: %v\n", objectName, err)
		}
		PutBuffer(data)
	}

	// Goroutines for PUT operation
	for i := 0; i < params.Concurrency; i++ {
		wg.Add(1) // Add to wait group outside goroutine to avoid races
		go func() {
			defer wg.Done()

			// Each worker handles its own local buffer to avoid contention
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

					// Generate object name and prepare data
					objectName, err := GeneratePrefixedObjectName(prefix, 8)
					if err != nil {
						fmt.Fprintf(logFile, "Error generating object name: %v\n", err)
						return
					}

					// Create buffer and reuse for object size
					data := GetBuffer(int(params.ObjectSize))
					defer PutBuffer(data) // Return buffer to the pool after usage

					request := objectstorage.PutObjectRequest{
						NamespaceName: common.String(namespace),
						BucketName:    common.String(params.BucketName),
						ObjectName:    common.String(objectName),
						PutObjectBody: nopCloser{bytes.NewReader(data)},
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
					reqCtx, reqCancel := context.WithTimeout(globalCtx, 15*time.Second) // Lower timeout for faster failover
					defer reqCancel()

					// Retry logic for uploading objects with detailed error logging
					response, err := client.PutObject(reqCtx, request)
					if err != nil {
						// Log specific errors, including potential 429 Too Many Requests
						if serviceErr, ok := common.IsServiceError(err); ok && serviceErr.GetHTTPStatusCode() == 429 {
							fmt.Fprintf(logFile, "429 Too Many Requests: Throttling detected for object %s\n", objectName)
						} else {
							fmt.Fprintf(logFile, "Error uploading object %s after retries: %s\n", objectName, err.Error())
						}
						continue
					}

					// Handle nil response case
					if response.HTTPResponse() == nil {
						fmt.Fprintf(logFile, "Error: Nil HTTP response for object %s\n", objectName)
						continue
					}

					// Update progress bar
					pb.Increment()
				}
			}
		}()
	}

	wg.Wait()
	pb.Finish()

	// Calculate and print results
	elapsedTime := time.Since(startTime)
	dataThroughput := (float64(objectIndex) * float64(params.ObjectSize)) / elapsedTime.Seconds() / (1024 * 1024) // MiB/s
	objectThroughput := float64(objectIndex) / elapsedTime.Seconds()                                              // objects/s

	fmt.Println("\nPUT Results:")
	fmt.Printf("Duration: %v\n", elapsedTime)
	fmt.Printf("Total Objects Processed: %d\n", objectIndex)
	fmt.Printf("Data Throughput: %.2f MiB/s\n", dataThroughput)
	fmt.Printf("Object Throughput: %.2f objects/s\n", objectThroughput)

	// Check if throttling occurred by scanning the log file for 429 errors
	if CheckLogForThrottling(logFileName) {
		fmt.Println("API Throttled: Check put_logs.txt for more details.")
	} else {
		fmt.Println("No API throttling detected.")
	}
}
