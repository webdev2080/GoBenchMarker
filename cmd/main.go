package main

import (
	"flag"
	"fmt"
	"gobenchmarker/benchmark"
	"time"
)

func main() {
	// Set system resource limits for high-performance testing
	err := benchmark.SetMaxResources()
	if err != nil {
		fmt.Printf("Error setting resources: %v\n", err)
	}

	// Define command-line flags
	concurrency := flag.Int("concurrency", 10, "Number of concurrent workers")
	objectCount := flag.Int("object-count", 0, "Total number of objects to process")
	objectSize := flag.Int64("object-size", 1024*1024, "Size of each object in bytes")
	bucketName := flag.String("bucket-name", "your-oci-bucket", "Name of the OCI bucket")
	operation := flag.String("operation", "PUT", "Operation to perform: PUT, GET, DELETE, MIXED, IOPS")
	duration := flag.Int("duration", 0, "Duration in seconds for the benchmark to run")
	rateLimit := flag.Int("rate-limit", 0, "Max requests per second (0 means no limit)")

	flag.Parse()

	// Initialize the params struct with the parsed values
	params := benchmark.BenchmarkParams{
		Concurrency: *concurrency,
		ObjectCount: *objectCount,
		ObjectSize:  *objectSize,
		BucketName:  *bucketName,
		Duration:    time.Duration(*duration) * time.Second,
		RateLimit:   *rateLimit,
	}

	// Run the selected benchmark operation
	switch *operation {
	case "PUT":
		fmt.Println("Performing PUT benchmark...")
		benchmark.RunPutBenchmark(params)
	case "GET":
		fmt.Println("Performing GET benchmark...")
		benchmark.RunGetBenchmark(params)
	case "DELETE":
		fmt.Println("Performing DELETE benchmark...")
		benchmark.RunDeleteBenchmark(params)
	case "MIXED":
		fmt.Println("\nPerforming MIXED benchmark (PUT, GET, DELETE)...\n")
		benchmark.RunMixedBenchmark(params)
	case "IOPS":
		fmt.Println("\nPerforming IOPS benchmark (PUT/GET Mixed)...\n")
		benchmark.RunIOPSBenchmark(params)
	default:
		fmt.Println("Unknown operation:", *operation)
	}
}
