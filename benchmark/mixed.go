package benchmark

import (
	"fmt"
	"time"
)

// RunMixedBenchmark runs PUT, GET, and DELETE operations for benchmarking
func RunMixedBenchmark(params BenchmarkParams, configFilePath string, namespace string) {
	start := time.Now() // Start the timer before running benchmarks

	// Adjust the proportions of PUT, GET, and DELETE as needed
	RunPutBenchmark(params, configFilePath, namespace)
	RunGetBenchmark(params, configFilePath, namespace)
	RunDeleteBenchmark(params, configFilePath, namespace)

	elapsed := time.Since(start) // Calculate elapsed time
	fmt.Printf("\nMIXED Benchmark finished in %s\n", elapsed)
	fmt.Println()
}
