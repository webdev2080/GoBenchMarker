package report

import (
	"fmt"
	"time"
)

// DisplayResults shows the summary of benchmark performance
func DisplayResults(operation string, totalObjects int, duration time.Duration, totalData int64) {
	throughput := float64(totalData) / duration.Seconds() / (1024 * 1024) // MiB/s
	objectThroughput := float64(totalObjects) / duration.Seconds()

	fmt.Printf("%s Results:\n", operation)
	fmt.Printf("Duration: %s\n", duration)
	fmt.Printf("Total Objects Processed: %d\n", totalObjects)
	fmt.Printf("Data Throughput: %.2f MiB/s\n", throughput)
	fmt.Printf("Object Throughput: %.2f objects/s\n", objectThroughput)
}
