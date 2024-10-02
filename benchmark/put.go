package benchmark

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"gobenchmarker/config"
	"gobenchmarker/progress"

	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/objectstorage"
	"golang.org/x/time/rate"
)

// RunPutBenchmark runs the PUT benchmark, uploading objects concurrently with retry handling
func RunPutBenchmark(params BenchmarkParams) {
	// Load OCI config and initialize the ObjectStorage client
	provider, err := config.LoadOCIConfig()
	if err != nil {
		panic(err)
	}
	client, err := objectstorage.NewObjectStorageClientWithConfigurationProvider(provider)
	if err != nil {
		panic(err)
	}

	// Get the namespace for object storage
	namespaceResp, err := client.GetNamespace(context.TODO(), objectstorage.GetNamespaceRequest{})
	if err != nil {
		panic(err)
	}
	namespace := *namespaceResp.Value

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
	var objectIndex int
	var mu sync.Mutex

	wg.Add(params.Concurrency)

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

	// Goroutines for PUT operation
	for i := 0; i < params.Concurrency; i++ {
		go func() {
			defer wg.Done()
			for {
				select {
				case <-globalCtx.Done():
					// Stop if the global context is canceled (either by timeout or manual stop)
					return
				default:
					// Locking for object index
					mu.Lock()
					if objectIndex >= params.ObjectCount && params.ObjectCount > 0 {
						mu.Unlock()
						globalCancel() // Manually stop when object count is reached
						return
					}
					objectIndex++
					mu.Unlock()

					// Generate object name and prepare data
					objectName, err := GenerateRandomName(8)
					if err != nil {
						fmt.Fprintf(logFile, "Error generating random object name: %s\n", err.Error())
						return
					}

					data := make([]byte, params.ObjectSize)
					request := objectstorage.PutObjectRequest{
						NamespaceName: common.String(namespace),
						BucketName:    common.String(params.BucketName),
						ObjectName:    common.String(objectName),
						PutObjectBody: nopCloser{bytes.NewReader(data)},
					}

					// Conditionally apply rate limiting
					if rateLimiter != nil {
						err := rateLimiter.Wait(globalCtx) // Wait for rate limiter if applied
						if err != nil {
							fmt.Fprintf(logFile, "Rate limiter error: %s\n", err.Error())
							return
						}
					}

					// Retry logic for uploading objects with detailed error logging
					err = uploadWithRetry(client, request, 5, logFile, globalCtx) // Pass global context to handle timeout
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
				}
			}
		}()
	}

	wg.Wait()
	pb.Finish()

	// Calculate and print results
	elapsedTime := time.Since(startTime)                                                                          // Use the actual start time to calculate elapsed time
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
