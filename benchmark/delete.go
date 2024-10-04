package benchmark

import (
	"context"
	"crypto/tls"
	"fmt"
	"math"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"gobenchmarker/config"
	"gobenchmarker/progress"

	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/objectstorage"
)

// RunDeleteBenchmark runs the DELETE benchmark, removing objects concurrently in paginated mode with prefix support
func RunDeleteBenchmark(params BenchmarkParams, configFilePath string, namespaceOverride string, hostOverride string, prefixOverride string) {
	// Load OCI configuration
	provider, err := config.LoadOCIConfig(configFilePath)
	if err != nil {
		fmt.Printf("Error loading OCI config: %v\n", err)
		return
	}

	// Initialize custom HTTP client with disabled TLS verification
	customTransport := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	customClient := &http.Client{Transport: customTransport}
	client, err := objectstorage.NewObjectStorageClientWithConfigurationProvider(provider)
	client.HTTPClient = customClient
	if err != nil {
		panic(err)
	}

	// Set host override if provided
	if hostOverride != "" {
		client.Host = hostOverride
	}

	// Determine namespace: Use provided namespace, or fetch it via API
	namespace := namespaceOverride
	if namespace == "" {
		namespaceResp, err := client.GetNamespace(context.TODO(), objectstorage.GetNamespaceRequest{})
		if err != nil {
			panic(err)
		}
		namespace = *namespaceResp.Value
	}

	// Set up log file for errors
	timestamp := time.Now().Format("20060102_150405")
	logFileName := fmt.Sprintf("delete_logs_%s.txt", timestamp)
	logFile, err := os.Create(logFileName)
	if err != nil {
		panic(fmt.Sprintf("Failed to create log file: %s", err.Error()))
	}
	defer logFile.Close()

	// Calculate the total number of objects to delete
	totalObjects := int64(params.ObjectCount)
	if totalObjects == 0 {
		totalObjects = math.MaxInt64 // If object count not specified, delete all
	}

	// Initialize progress bar
	pb := progress.NewProgressBar(totalObjects)
	pb.SetCaption("Deleting")

	// Atomic counter for tracking progress
	var objectCount int64
	startTime := time.Now()

	// Context handling: for duration or manual cancellation
	var globalCtx context.Context
	var globalCancel context.CancelFunc

	if params.Duration > 0 {
		globalCtx, globalCancel = context.WithTimeout(context.Background(), params.Duration)
	} else {
		globalCtx, globalCancel = context.WithCancel(context.Background()) // No timeout; manually cancel when object count is reached
	}
	defer globalCancel()

	// Buffer for pre-fetching object pages
	objectCh := make(chan objectstorage.ObjectSummary, 10000)
	var closeOnce sync.Once // Ensure stopCh is closed only once
	stopCh := make(chan bool)

	// Listing goroutine: Fetches objects by prefix and pushes to objectCh
	go func() {
		defer close(objectCh)
		var nextStartWith *string

		for {
			select {
			case <-globalCtx.Done():
				return // Stop if context is canceled or timed out
			default:
				listReq := objectstorage.ListObjectsRequest{
					NamespaceName: common.String(namespace),
					BucketName:    common.String(params.BucketName),
					Limit:         common.Int(1000), // Max batch size
					StartAfter:    nextStartWith,
					Prefix:        common.String(prefixOverride), // Filter by prefix
				}

				listResp, err := client.ListObjects(globalCtx, listReq)
				if err != nil {
					fmt.Fprintf(logFile, "Error listing objects: %s\n", err.Error())
					return
				}

				if len(listResp.Objects) == 0 {
					return // No more objects to list
				}

				for _, obj := range listResp.Objects {
					select {
					case objectCh <- obj:
					case <-stopCh:
						return // Stop processing when stopCh is closed
					case <-globalCtx.Done():
						return // Stop processing if context times out
					}
				}

				nextStartWith = listResp.NextStartWith
				if nextStartWith == nil {
					return // No more pages
				}
			}
		}
	}()

	// Deletion goroutines: Deleting objects concurrently
	var wg sync.WaitGroup
	for i := 0; i < params.Concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case obj, ok := <-objectCh:
					if !ok {
						return // Channel closed, stop goroutine
					}
					deleteObject(globalCtx, obj, client, namespace, params.BucketName, pb, logFile, &objectCount, totalObjects, stopCh, &closeOnce)
				case <-globalCtx.Done():
					return // Stop deletion if the context times out
				}
			}
		}()
	}

	wg.Wait()
	closeOnce.Do(func() { close(stopCh) }) // Ensure stopCh is closed only once

	// Finalize progress bar
	pb.Finish()

	// Calculate and print results
	elapsedTime := time.Since(startTime)
	objectThroughput := float64(objectCount) / elapsedTime.Seconds()

	fmt.Printf("\nDELETE Results:\nDuration: %v\nTotal Objects Processed: %d\nObject Throughput: %.2f objects/s\n", elapsedTime, objectCount, objectThroughput)
}

// Delete object helper function
func deleteObject(ctx context.Context, object objectstorage.ObjectSummary, client objectstorage.ObjectStorageClient, namespace, bucket string, pb *progress.ProgressBar, logFile *os.File, objectCount *int64, totalObjects int64, stopCh chan bool, closeOnce *sync.Once) {
	_, err := client.DeleteObject(ctx, objectstorage.DeleteObjectRequest{
		NamespaceName: common.String(namespace),
		BucketName:    common.String(bucket),
		ObjectName:    common.String(*object.Name),
	})

	if err != nil {
		fmt.Fprintf(logFile, "Error deleting object %s: %s\n", *object.Name, err.Error())
	}

	// Update object count and progress bar
	atomic.AddInt64(objectCount, 1)
	pb.Increment()

	// Stop if we've reached the target number of objects
	if atomic.LoadInt64(objectCount) >= totalObjects {
		closeOnce.Do(func() { close(stopCh) }) // Close stopCh only once
	}
}
