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
)

// RunDeletePARsBenchmark deletes all PARs for a given bucket
func RunDeletePARsBenchmark(params BenchmarkParams, configFilePath string) {
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

	// Function to delete a single PAR
	deletePAR := func(par objectstorage.PreauthenticatedRequestSummary) {
		defer wg.Done()

		// Perform PAR deletion
		deleteRequest := objectstorage.DeletePreauthenticatedRequestRequest{
			NamespaceName: common.String(namespace),
			BucketName:    common.String(params.BucketName),
			ParId:         common.String(*par.Id),
		}

		_, err := client.DeletePreauthenticatedRequest(context.TODO(), deleteRequest)
		if err != nil {
			fmt.Fprintf(logFile, "Error deleting PAR %s: %s\n", *par.Id, err.Error())
		} else {
			atomic.AddInt64(&totalPARs, 1)
			//fmt.Printf("Successfully deleted PAR: %s\n", *par.Id)
		}

		// Update progress bar
		pb.Increment()
	}

	// Pagination: Fetch all PARs in the bucket in pages
	var nextPage *string
	for {
		// List all PARs for the bucket with pagination
		listRequest := objectstorage.ListPreauthenticatedRequestsRequest{
			NamespaceName: common.String(namespace),
			BucketName:    common.String(params.BucketName),
			Limit:         common.Int(100), // Fetch 100 PARs per page
			Page:          nextPage,
		}

		listResp, err := client.ListPreauthenticatedRequests(context.TODO(), listRequest)
		if err != nil {
			fmt.Fprintf(logFile, "Error listing PARs: %s\n", err.Error())
			break
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
			wg.Add(1)
			go deletePAR(par)
		}

		// Wait for all the current batch of deletions to complete
		wg.Wait()

		// If there is a next page, fetch the next batch
		if listResp.OpcNextPage == nil {
			break // No more pages, stop
		}
		nextPage = listResp.OpcNextPage
	}

	// Final progress bar update and finish
	pb.Finish()

	// Calculate and print results
	elapsedTime := time.Since(startTime)
	fmt.Println("\nPAR Deletion Results:")
	fmt.Printf("Duration: %v\n", elapsedTime)
	fmt.Printf("Total PARs Deleted: %d\n", totalPARs)
}
