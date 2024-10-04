package benchmark

import (
	"context"
	"crypto/tls"
	"fmt"
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

// RunDeletePARsBenchmark deletes all PARs for a given bucket
func RunDeletePARsBenchmark(params BenchmarkParams, configFilePath string, namespaceOverride string, hostOverride string) {
	// Load OCI config and initialize the ObjectStorage client
	provider, err := config.LoadOCIConfig(configFilePath)
	if err != nil {
		fmt.Printf("Error loading OCI config: %v\n", err)
		return // Gracefully exit if config loading fails
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
	logFileName := fmt.Sprintf("deletepars_logs_%s.txt", timestamp)
	logFile, err := os.Create(logFileName)
	if err != nil {
		panic(fmt.Sprintf("Failed to create log file: %s", err.Error()))
	}
	defer logFile.Close()

	// Initialize progress bar
	pb := progress.NewProgressBar(0) // We will set the total after fetching all PARs
	pb.SetCaption("Deleting PARs")

	// Track total deleted PARs
	var totalPARs int64 // Atomic counter
	var wg sync.WaitGroup

	// Record the start time
	startTime := time.Now()

	// Global context: use a timeout if `params.Duration` is provided
	var globalCtx context.Context
	var globalCancel context.CancelFunc

	if params.Duration > 0 {
		globalCtx, globalCancel = context.WithTimeout(context.Background(), params.Duration)
	} else {
		globalCtx, globalCancel = context.WithCancel(context.Background())
	}
	defer globalCancel()

	// Function to delete a single PAR
	deletePAR := func(par objectstorage.PreauthenticatedRequestSummary) {
		defer wg.Done()

		// Perform PAR deletion
		deleteRequest := objectstorage.DeletePreauthenticatedRequestRequest{
			NamespaceName: common.String(namespace),
			BucketName:    common.String(params.BucketName),
			ParId:         common.String(*par.Id),
		}

		select {
		case <-globalCtx.Done():
			// Exit if the context has been canceled
			return
		default:
			_, err := client.DeletePreauthenticatedRequest(globalCtx, deleteRequest) // Use globalCtx
			if err != nil {
				fmt.Fprintf(logFile, "Error deleting PAR %s: %s\n", *par.Id, err.Error())
			} else {
				atomic.AddInt64(&totalPARs, 1)
			}
			// Update progress bar
			pb.Increment()
		}
	}

	// Pagination: Fetch all PARs in the bucket in pages
	var nextPage *string
mainLoop:
	for {
		select {
		case <-globalCtx.Done():
			// Exit the loop if the global context has timed out or been canceled
			fmt.Println("Operation timed out or canceled.")
			break mainLoop
		default:
			// List all PARs for the bucket with pagination
			listRequest := objectstorage.ListPreauthenticatedRequestsRequest{
				NamespaceName: common.String(namespace),
				BucketName:    common.String(params.BucketName),
				Limit:         common.Int(100), // Fetch 100 PARs per page
				Page:          nextPage,
			}

			listResp, err := client.ListPreauthenticatedRequests(globalCtx, listRequest) // Use globalCtx
			if err != nil {
				fmt.Fprintf(logFile, "Error listing PARs: %s\n", err.Error())
				break mainLoop
			}

			// If no PARs are found, stop the process
			if len(listResp.Items) == 0 {
				fmt.Println("No more PARs to delete.")
				break
			}

			// Set progress bar total to the number of PARs fetched
			pb.AddTotal(int64(len(listResp.Items)))

			// Delete each PAR concurrently
			for _, par := range listResp.Items {
				select {
				case <-globalCtx.Done():
					// Stop creating more goroutines if the context times out
					fmt.Println("Deletion timed out.")
					break mainLoop
				default:
					wg.Add(1)
					go deletePAR(par)
				}
			}

			// Wait for all the current batch of deletions to complete
			wg.Wait()

			// If there is a next page, fetch the next batch
			if listResp.OpcNextPage == nil {
				break // No more pages, stop
			}
			nextPage = listResp.OpcNextPage
		}
	}

	// Final progress bar update and finish
	pb.Finish()

	// Ensure results are printed even on timeout
	elapsedTime := time.Since(startTime)
	objectThroughput := float64(totalPARs) / elapsedTime.Seconds()

	// Print final results
	fmt.Println("\nPAR Deletion Results:")
	fmt.Printf("Duration: %v\n", elapsedTime)
	fmt.Printf("Total PARs Deleted: %d\n", totalPARs)
	fmt.Printf("Throughput: %.2f PARs/s\n", objectThroughput)
}
