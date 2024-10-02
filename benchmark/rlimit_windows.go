//go:build windows
// +build windows

package benchmark

import (
	"fmt"
	"runtime/debug"
)

// SetMaxResources adjusts system resource limits on Windows systems.
func SetMaxResources() error {
	// Only set Go runtime's max threads on Windows, as we don't have an equivalent
	// for open file limits and threads as in Unix systems.
	const threadLimit = 10000
	maxThreads := 8000 // Example limit for Windows
	debug.SetMaxThreads(maxThreads)

	fmt.Println("System resources adjusted for high performance on Windows.")
	return nil
}
