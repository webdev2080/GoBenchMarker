package benchmark

import (
	"context"
	"fmt"
	"math"
	"os"
	"sync"
	"time"

	"gobenchmarker/config"
	"gobenchmarker/progress"

	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/objectstorage"
	"golang.org/x/time/rate"
)

// RunDeleteBenchmark runs the DELETE benchmark, removing objects concurrently in paginated mode
func RunDeleteBenchmark(params BenchmarkParams, configFilePath string) {
	// Load OCI configuration
	provider, err := config.LoadOCIConfig(configFilePath)
	if err != nil {
		fmt.Printf("Error loading OCI config: %v\n", err)
		return // Gracefully exit if config loading fails
	}

	client, err := objectstorage.NewObjectStorageClientWithConfigurationProvider(provider)
	if err != nil {
		fmt.Printf("\nError creating Object Storage client: %v\n", err)
		return // Gracefully exit if client creation fails
	}

	namespaceResp, err := client.GetNamespace(context.TODO(), objectstorage.GetNamespaceRequest{})
	if err != nil {
		panic(err)
	}
	namespace := *namespaceResp.Value

	// Create log file with timestamp
	timestamp := time.Now().Format("20060102_150405")
	logFileName := fmt.Sprintf("delete_logs_%s.txt", timestamp)
	logFile, err := os.Create(logFileName)
	if err != nil {
		panic(fmt.Sprintf("Failed to create log file: %s", err.Error()))
	}
	defer logFile.Close()

	// Initialize object count. If object count is not specified, set it to a very large number
	totalObjects := params.ObjectCount
	if totalObjects == 0 {
		totalObjects = math.MaxInt64 // Effectively unlimited when only duration is specified
	}

	// Initialize the progress bar
	pb := progress.NewProgressBar(int64(totalObjects))
	pb.SetCaption("Deleting")

	// Track total deleted objects and concurrency control
	var objectCount int64
	var mu sync.Mutex
	startTime := time.Now()

	useDuration := params.Duration > 0
	durationLimit := startTime.Add(params.Duration)

	// Rate limiter initialization (if required)
	var rateLimiter *rate.Limiter
	if params.RateLimit > 0 {
		rateLimiter = rate.NewLimiter(rate.Limit(params.RateLimit), 1) // Apply rate limit
		fmt.Println("Rate limiter enabled with rate:", params.RateLimit)
	}

	// Function to delete a single object
	deleteObject := func(object objectstorage.ObjectSummary) {
		// Check if we should stop due to object count or duration
		mu.Lock()
		if objectCount >= int64(totalObjects) || (useDuration && time.Now().After(durationLimit)) {
			mu.Unlock()
			return
		}
		objectCount++
		mu.Unlock()

		// Perform object deletion
		_, err := client.DeleteObject(context.TODO(), objectstorage.DeleteObjectRequest{
			NamespaceName: common.String(namespace),
			BucketName:    common.String(params.BucketName),
			ObjectName:    common.String(*object.Name),
		})

		if err != nil {
			fmt.Fprintf(logFile, "Error deleting object %s: %s\n", *object.Name, err.Error())
		}

		// Update progress bar
		pb.Increment()

		// Rate limiting if applied
		if rateLimiter != nil {
			err := rateLimiter.Wait(context.TODO()) // Apply rate limiter delay
			if err != nil {
				fmt.Fprintf(logFile, "Rate limiter error: %s\n", err.Error())
			}
		}
	}

	// Start paginated deletion process
	var nextStartWith *string
	for {
		// Check if the duration limit has been reached before listing the next batch of objects
		if useDuration && time.Now().After(durationLimit) {
			break
		}

		// List objects in pages
		listReq := objectstorage.ListObjectsRequest{
			NamespaceName: common.String(namespace),
			BucketName:    common.String(params.BucketName),
			Limit:         common.Int(1000), // Limit per batch (experiment with increasing this)
			Start:         nextStartWith,    // Pagination
		}

		listResp, err := client.ListObjects(context.TODO(), listReq)
		if err != nil {
			fmt.Fprintf(logFile, "Error listing objects: %s\n", err.Error())
			break
		}

		// If no objects are found in the current batch, stop
		if len(listResp.Objects) == 0 {
			fmt.Println("No more objects to delete.")
			break
		}

		// Delete objects concurrently, each in its own goroutine
		var wg sync.WaitGroup
		for _, object := range listResp.Objects {
			wg.Add(1)
			go func(obj objectstorage.ObjectSummary) {
				defer wg.Done()
				deleteObject(obj) // Deleting each object concurrently
			}(object)
		}

		// Wait for all deletions in the current batch to complete
		wg.Wait()

		// Check if we've hit the object count limit
		mu.Lock()
		if objectCount >= int64(totalObjects) {
			mu.Unlock()
			fmt.Println("Object count limit reached. Stopping deletion.")
			break
		}
		mu.Unlock()

		// Move to the next page of objects
		nextStartWith = listResp.NextStartWith
		if nextStartWith == nil {
			// If no more pages are available, stop the deletion process
			fmt.Println("All pages processed. No more objects to delete.")
			break
		}
	}

	// Final progress bar finish
	pb.Finish()

	// Calculate and print benchmark results
	elapsedTime := time.Since(startTime)
	objectThroughput := float64(objectCount) / elapsedTime.Seconds() // objects/s

	fmt.Println("\nDELETE Results:")
	fmt.Printf("Duration: %v\n", elapsedTime)
	fmt.Printf("Total Objects Processed: %d\n", objectCount)
	fmt.Printf("Object Throughput: %.2f objects/s\n", objectThroughput)

	// Check if throttling occurred based on the log file
	if CheckLogForThrottling(logFileName) {
		fmt.Println("API Throttled: Check delete_logs.txt for more details.")
	} else {
		fmt.Println("No API throttling detected.")
	}

	fmt.Println()
}
