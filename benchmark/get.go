package benchmark

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"gobenchmarker/config"
	"gobenchmarker/progress"

	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/objectstorage"
	"golang.org/x/time/rate"
)

// RunGetBenchmark runs the GET benchmark, downloading objects concurrently with retry handling and optimized pagination
func RunGetBenchmark(params BenchmarkParams) {
	provider, err := config.LoadOCIConfig()
	if err != nil {
		panic(err)
	}

	client, err := objectstorage.NewObjectStorageClientWithConfigurationProvider(provider)
	if err != nil {
		panic(err)
	}

	namespaceResp, err := client.GetNamespace(context.TODO(), objectstorage.GetNamespaceRequest{})
	if err != nil {
		panic(err)
	}
	namespace := *namespaceResp.Value

	// Create log file with timestamp
	timestamp := time.Now().Format("20060102_150405")
	logFileName := fmt.Sprintf("get_logs_%s.txt", timestamp)
	logFile, err := os.Create(logFileName)
	if err != nil {
		panic(fmt.Sprintf("Failed to create log file: %s", err.Error()))
	}
	defer logFile.Close()

	// Fetch only as many objects as needed based on object count
	var allObjects []objectstorage.ObjectSummary
	nextStartWith := (*string)(nil) // Pagination token
	totalObjectsNeeded := params.ObjectCount
	objectsFetched := 0

	for {
		// List the objects in the bucket with pagination
		listReq := objectstorage.ListObjectsRequest{
			NamespaceName: common.String(namespace),
			BucketName:    common.String(params.BucketName),
			Limit:         common.Int(1000), // Limit to 1000 objects per page
			Start:         nextStartWith,    // Pagination token
		}

		listResp, err := client.ListObjects(context.TODO(), listReq)
		if err != nil {
			fmt.Fprintf(logFile, "Error listing objects: %s\n", err.Error())
			return
		}

		allObjects = append(allObjects, listResp.Objects...)
		objectsFetched += len(listResp.Objects)

		// Break if enough objects have been fetched to satisfy the object count requirement
		if objectsFetched >= totalObjectsNeeded {
			break
		}

		// Break if no more pages (i.e., NextStartWith is nil)
		if listResp.NextStartWith == nil {
			break
		}

		nextStartWith = listResp.NextStartWith
	}

	if len(allObjects) == 0 {
		fmt.Println("No objects found in the bucket.")
		return
	}

	// Adjust object count if fewer objects are available
	if len(allObjects) < params.ObjectCount {
		params.ObjectCount = len(allObjects)
	}

	// Initialize progress bar
	pb := progress.NewProgressBar(int64(params.ObjectCount))
	pb.SetCaption("Downloading")

	// Set up concurrency control
	var wg sync.WaitGroup
	var totalBytesDownloaded int64 // To track total data downloaded
	var objectIndex int
	var mu sync.Mutex

	wg.Add(params.Concurrency)

	// Global context handling: If duration is set, use it; otherwise, use background context
	var globalCtx context.Context
	var globalCancel context.CancelFunc
	startTime := time.Now()

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

	// Goroutines for GET operation
	for i := 0; i < params.Concurrency; i++ {
		go func(workerID int) {
			defer wg.Done()
			for {
				select {
				case <-globalCtx.Done():
					return // Exit if duration has been exceeded or object count reached
				default:
					// Locking for object index
					mu.Lock()
					if objectIndex >= params.ObjectCount && params.ObjectCount > 0 {
						mu.Unlock()
						globalCancel() // Manually stop when object count is reached
						return
					}
					objectName := *allObjects[objectIndex%len(allObjects)].Name
					objectIndex++
					mu.Unlock()

					// Conditionally apply rate limiting
					if rateLimiter != nil {
						err := rateLimiter.Wait(globalCtx) // Wait for rate limiter if applied
						if err != nil {
							fmt.Fprintf(logFile, "Rate limiter error: %s\n", err.Error())
							return
						}
					}

					// Set a timeout for the GET request to ensure timely cancellation
					getCtx, cancel := context.WithTimeout(globalCtx, 3*time.Second)
					defer cancel()

					// Download the object
					getResp, err := client.GetObject(getCtx, objectstorage.GetObjectRequest{
						NamespaceName: common.String(namespace),
						BucketName:    common.String(params.BucketName),
						ObjectName:    common.String(objectName),
					})

					if err != nil {
						// Log specific errors, including potential 429 Too Many Requests
						if serviceErr, ok := common.IsServiceError(err); ok && serviceErr.GetHTTPStatusCode() == 429 {
							fmt.Fprintf(logFile, "429 Too Many Requests: Throttling detected for object %s\n", objectName)
						} else {
							fmt.Fprintf(logFile, "Error downloading object %s: %s\n", objectName, err.Error())
						}
						continue
					}

					// Read and discard the object data
					bytesRead, err := io.Copy(io.Discard, getResp.Content)
					if err != nil {
						fmt.Fprintf(logFile, "Error reading object data: %s\n", err.Error())
					}
					getResp.Content.Close()

					// Update the total bytes downloaded
					mu.Lock()
					totalBytesDownloaded += bytesRead
					mu.Unlock()

					// Update progress bar
					pb.Increment()
				}
			}
		}(i)
	}

	wg.Wait()
	pb.Finish()

	// Calculate and print benchmark results
	elapsedTime := time.Since(startTime)                                                      // Use the actual start time for calculating elapsed time
	dataThroughput := (float64(totalBytesDownloaded)) / elapsedTime.Seconds() / (1024 * 1024) // MiB/s
	objectThroughput := float64(objectIndex) / elapsedTime.Seconds()                          // objects/s

	fmt.Println("\nGET Results:")
	fmt.Printf("Duration: %v\n", elapsedTime)
	fmt.Printf("Total Objects Processed: %d\n", objectIndex)
	fmt.Printf("Total Data Downloaded: %.2f MiB\n", float64(totalBytesDownloaded)/(1024*1024))
	fmt.Printf("Data Throughput: %.2f MiB/s\n", dataThroughput)
	fmt.Printf("Object Throughput: %.2f objects/s\n", objectThroughput)
	fmt.Println()

	// Check if throttling occurred based on the log file
	if CheckLogForThrottling(logFileName) {
		fmt.Println("API Throttled: Check get_logs.txt for more details.")
	} else {
		fmt.Println("No API throttling detected.")
	}

	fmt.Println()
}
