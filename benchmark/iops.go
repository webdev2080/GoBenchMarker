package benchmark

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"gobenchmarker/config"
	"gobenchmarker/progress"

	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/objectstorage"
)

// downloadWithRetry handles download retries for GET operations with context.
func downloadWithRetry(client objectstorage.ObjectStorageClient, request objectstorage.GetObjectRequest, retries int, logFile *os.File, ctx context.Context) (int64, error) {
	for i := 0; i < retries; i++ {
		getResp, err := client.GetObject(ctx, request)
		if err != nil {
			if ociError, ok := err.(common.ServiceError); ok && (ociError.GetHTTPStatusCode() == 503 || ociError.GetHTTPStatusCode() == 429) {
				fmt.Fprintf(logFile, "Retrying after GET error (%d/%d): %s\n", i+1, retries, err.Error())
				time.Sleep(time.Duration(i+1) * time.Second) // Exponential backoff
				continue
			}
			if ctx.Err() != nil { // Handle context errors like timeouts
				return 0, ctx.Err()
			}
			fmt.Fprintf(logFile, "Error downloading object: %s\n", err.Error())
			return 0, err
		}

		// Read the object data and count bytes read
		buf := new(bytes.Buffer)
		bytesRead, err := io.Copy(buf, getResp.Content)
		if err != nil {
			fmt.Fprintf(logFile, "Error reading object data: %s\n", err.Error())
			return 0, err
		}
		getResp.Content.Close()
		return bytesRead, nil
	}
	return 0, fmt.Errorf("GET failed after %d retries", retries)
}

// RunIOPSBenchmark runs a mixed PUT/GET benchmark focused on measuring IOPS for small objects.
func RunIOPSBenchmark(params BenchmarkParams, configFilePath string) {
	// Load OCI config and initialize the ObjectStorage client
	provider, err := config.LoadOCIConfig(configFilePath)
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
	logFileName := fmt.Sprintf("iops_logs_%s.txt", timestamp)
	logFile, err := os.Create(logFileName)
	if err != nil {
		panic(fmt.Sprintf("Failed to create log file: %s", err.Error()))
	}
	defer logFile.Close()

	// Initialize progress bar
	pb := progress.NewProgressBar(int64(params.ObjectCount))
	pb.SetCaption("Running IOPS Benchmark")

	// Set up concurrency control
	var wg sync.WaitGroup
	var objectIndex int64     // Use atomic integer for object index
	var totalOperations int64 // To track PUT + GET operations using atomic
	var totalBytesTransferred int64
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

	// Create a sync.Pool for reusing byte slices (buffers) to minimize allocations
	var bufPool = sync.Pool{
		New: func() interface{} {
			return make([]byte, params.ObjectSize)
		},
	}

	// Goroutines for PUT/GET operations
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

					// Generate object name
					objectName, err := GenerateRandomName(8)
					if err != nil {
						fmt.Fprintf(logFile, "Error generating random object name: %s\n", err.Error())
						return
					}

					// Prepare data for PUT request using buffer pool
					data := bufPool.Get().([]byte)
					defer bufPool.Put(data)

					putRequest := objectstorage.PutObjectRequest{
						NamespaceName: common.String(namespace),
						BucketName:    common.String(params.BucketName),
						ObjectName:    common.String(objectName),
						PutObjectBody: nopCloser{bytes.NewReader(data)},
					}

					// PUT operation with retry logic
					err = uploadWithRetry(client, putRequest, 5, logFile, globalCtx)
					if err != nil {
						fmt.Fprintf(logFile, "Error uploading object %s after retries: %s\n", objectName, err.Error())
						continue
					}

					// Atomically update the total operations and bytes transferred
					atomic.AddInt64(&totalOperations, 1)
					atomic.AddInt64(&totalBytesTransferred, int64(params.ObjectSize)) // Count uploaded bytes

					// Prepare GET request
					getRequest := objectstorage.GetObjectRequest{
						NamespaceName: common.String(namespace),
						BucketName:    common.String(params.BucketName),
						ObjectName:    common.String(objectName),
					}

					// GET operation with retry logic
					bytesDownloaded, err := downloadWithRetry(client, getRequest, 5, logFile, globalCtx)
					if err != nil {
						fmt.Fprintf(logFile, "Error downloading object %s after retries: %s\n", objectName, err.Error())
						continue
					}

					// Atomically update the total operations and bytes transferred
					atomic.AddInt64(&totalOperations, 1)
					atomic.AddInt64(&totalBytesTransferred, bytesDownloaded)

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
	iops := float64(totalOperations) / elapsedTime.Seconds()                                 // Total operations include both PUT and GET
	dataThroughput := float64(totalBytesTransferred) / elapsedTime.Seconds() / (1024 * 1024) // MiB/s

	fmt.Println("\nIOPS Results:")
	fmt.Printf("Duration: %v\n", elapsedTime)
	fmt.Printf("Total Operations (PUT + GET): %d\n", totalOperations)
	fmt.Printf("IOPS: %.2f operations/s\n", iops)
	fmt.Printf("Data Throughput: %.2f MiB/s\n", dataThroughput)

	// Check if throttling occurred by scanning the log file for 429 errors
	if CheckLogForThrottling(logFileName) {
		fmt.Println("API Throttled: Check iops_logs.txt for more details.")
	} else {
		fmt.Println("No API throttling detected.")
	}
}
