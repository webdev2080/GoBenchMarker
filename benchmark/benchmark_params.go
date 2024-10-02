package benchmark

import "time"

// BenchmarkParams holds the parameters for all benchmarks
type BenchmarkParams struct {
	Concurrency int           // Number of concurrent workers
	ObjectCount int           // Total number of objects to process
	ObjectSize  int64         // Size of each object in bytes
	BucketName  string        // OCI Bucket name
	Duration    time.Duration // Optional duration for benchmarks
	RateLimit   int           // Rate limit
}
