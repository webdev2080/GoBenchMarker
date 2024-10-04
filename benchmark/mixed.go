package benchmark

import (
	"fmt"
	"time"
)

// RunMixedBenchmark runs PUT, GET, and DELETE operations for benchmarking
func RunMixedBenchmark(params BenchmarkParams, configFilePath string, namespace string, hostOverride string, prefixOverride string) {
	start := time.Now() // Start the timer before running benchmarks

	// If a prefix is provided, append it to the operations
	if prefixOverride != "" {
		fmt.Printf("Running benchmark with prefix: %s\n", prefixOverride)
	}

	// Adjust the proportions of PUT, GET, and DELETE as needed
	RunPutBenchmark(params, configFilePath, namespace, hostOverride, prefixOverride)
	RunGetBenchmark(params, configFilePath, namespace, hostOverride, prefixOverride)
	RunDeleteBenchmark(params, configFilePath, namespace, hostOverride, prefixOverride)

	elapsed := time.Since(start) // Calculate elapsed time
	fmt.Printf("\nMIXED Benchmark finished in %s\n", elapsed)
	fmt.Println()
}
