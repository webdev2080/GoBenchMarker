//go:build linux
// +build linux

package benchmark

import (
	"fmt"
	"io/ioutil"
	"runtime/debug"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

// SetMaxResources sets system resource limits for Linux systems.
func SetMaxResources() error {
	const threadLimit = 10000
	rLimit := unix.Rlimit{}

	// Get the current max file descriptor limit
	err := unix.Getrlimit(unix.RLIMIT_NOFILE, &rLimit)
	if err != nil {
		return fmt.Errorf("unable to get rlimit: %v", err)
	}

	// Set the open file limit to the system's maximum value
	rLimit.Cur = rLimit.Max
	err = unix.Setrlimit(unix.RLIMIT_NOFILE, &rLimit)
	if err != nil {
		return fmt.Errorf("unable to set open file limit: %v", err)
	}

	// Read the maximum threads value from /proc/sys/kernel/threads-max
	threads, err := readLinuxMaxThreads()
	if err != nil {
		return fmt.Errorf("unable to read max threads: %v", err)
	}

	// Set the Go runtime's max threads to 90% of the system's max thread limit
	maxThreads := (int(threads) * 90) / 100
	if maxThreads > threadLimit {
		debug.SetMaxThreads(maxThreads)
	}

	fmt.Println("System resources adjusted for high performance on Linux.")
	return nil
}

// readLinuxMaxThreads reads the max threads from /proc/sys/kernel/threads-max on Linux.
func readLinuxMaxThreads() (uint32, error) {
	data, err := ioutil.ReadFile("/proc/sys/kernel/threads-max")
	if err != nil {
		return 0, fmt.Errorf("unable to read /proc/sys/kernel/threads-max: %v", err)
	}
	trimmed := strings.TrimSpace(string(data))
	threads, err := strconv.ParseUint(trimmed, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("unable to parse max threads value: %v", err)
	}
	return uint32(threads), nil
}
