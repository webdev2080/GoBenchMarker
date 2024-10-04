package benchmark

import (
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

// RunCreatePARsBenchmark generates PARs concurrently
func RunCreatePARsBenchmark(params BenchmarkParams, configFilePath string, namespaceOverride string, hostOverride string) {
	// Load OCI config and initialize the ObjectStorage client
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

	// Use the hostOverride if provided, otherwise use the SDK default
	if hostOverride != "" {
		fmt.Println("Using custom host: ", hostOverride)
		client.Host = hostOverride
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
	logFileName := fmt.Sprintf("createpars_logs_%s.txt", timestamp)
	logFile, err := os.Create(logFileName)
	if err != nil {
		panic(fmt.Sprintf("Failed to create log file: %s", err.Error()))
	}
	defer logFile.Close()

	// Initialize progress bar
	pb := progress.NewProgressBar(int64(params.ObjectCount))
	pb.SetCaption("Creating PARs")

	// Set up concurrency control
	var wg sync.WaitGroup
	var parIndex int64 // Atomic counter for PARs

	wg.Add(params.Concurrency)

	// Record the start time
	startTime := time.Now()

	// Global context handling for duration or completion
	var globalCtx context.Context
	var globalCancel context.CancelFunc

	if params.Duration > 0 {
		globalCtx, globalCancel = context.WithTimeout(context.Background(), params.Duration)
	} else {
		globalCtx, globalCancel = context.WithCancel(context.Background()) // No timeout; manually cancel after object count
	}
	defer globalCancel()

	// Create rate limiter if specified
	var rateLimiter *rate.Limiter
	if params.RateLimit > 0 {
		rateLimiter = rate.NewLimiter(rate.Limit(params.RateLimit), 1)
		fmt.Println("Rate limiter set to: ", params.RateLimit)
	}

	// Goroutines for PAR creation
	for i := 0; i < params.Concurrency; i++ {
		go func() {
			defer wg.Done()
			for {
				select {
				case <-globalCtx.Done():
					// Stop if global context is canceled
					return
				default:
					// Atomically increment the PAR index
					currentIndex := atomic.AddInt64(&parIndex, 1) - 1
					if currentIndex >= int64(params.ObjectCount) && params.ObjectCount > 0 {
						globalCancel()
						return
					}

					// Create PAR name or use object names (adjust as per requirement)
					objectName, err := GenerateRandomName(8)
					if err != nil {
						fmt.Fprintf(logFile, "Error generating object name: %s\n", err.Error())
						return
					}

					// Create Preauthenticated Request
					createPARRequest := objectstorage.CreatePreauthenticatedRequestRequest{
						NamespaceName: common.String(namespace),
						BucketName:    common.String(params.BucketName),
						CreatePreauthenticatedRequestDetails: objectstorage.CreatePreauthenticatedRequestDetails{
							Name:        common.String("PAR-BucketAccess"),
							AccessType:  objectstorage.CreatePreauthenticatedRequestDetailsAccessTypeAnyobjectread, // Bucket-level access
							TimeExpires: &common.SDKTime{Time: time.Now().Add(24 * time.Hour)},                     // 24 hours expiration
						},
					}

					// Apply rate limiting if necessary
					if rateLimiter != nil {
						err := rateLimiter.Wait(globalCtx)
						if err != nil {
							fmt.Fprintf(logFile, "Rate limiter error: %s\n", err.Error())
							return
						}
					}

					// Context with timeout for each PAR creation request
					reqCtx, reqCancel := context.WithTimeout(globalCtx, 30*time.Second)
					defer reqCancel()

					// Retry logic for creating PARs
					err = createPARWithRetry(client, createPARRequest, 5, logFile, reqCtx)
					if err != nil {
						// Log detailed errors
						fmt.Fprintf(logFile, "Error creating PAR %s: %s\n", objectName, err.Error())
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
	objectThroughput := float64(parIndex) / elapsedTime.Seconds()

	fmt.Println("\nPAR Creation Results:")
	fmt.Printf("Duration: %v\n", elapsedTime)
	fmt.Printf("Total PARs Created: %d\n", parIndex)
	fmt.Printf("Throughput: %.2f PARs/s\n", objectThroughput)

	// Check if throttling occurred by scanning the log file for 429 errors
	if CheckLogForThrottling(logFileName) {
		fmt.Println("API Throttled: Check createpars_logs.txt for more details.")
	} else {
		fmt.Println("No API throttling detected.")
	}
}

// createPARWithRetry handles retries for creating a PAR
func createPARWithRetry(client objectstorage.ObjectStorageClient, request objectstorage.CreatePreauthenticatedRequestRequest, maxRetries int, logFile *os.File, ctx context.Context) error {
	for retries := 0; retries < maxRetries; retries++ {
		_, err := client.CreatePreauthenticatedRequest(ctx, request)
		if err == nil {
			return nil
		}
		if serviceErr, ok := common.IsServiceError(err); ok && serviceErr.GetHTTPStatusCode() == 429 {
			fmt.Fprintf(logFile, "429 Too Many Requests: Throttling for PAR %s\n", *request.CreatePreauthenticatedRequestDetails.Name)
			time.Sleep(2 * time.Second) // Backoff for throttling
		} else {
			fmt.Fprintf(logFile, "Error creating PAR: %s, attempt %d/%d\n", err.Error(), retries+1, maxRetries)
		}
	}
	return fmt.Errorf("max retries reached for creating PAR")
}
