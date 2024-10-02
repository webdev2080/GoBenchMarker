//go:build linux || darwin
// +build linux darwin

package benchmark

import (
	"fmt"
	"io/ioutil"
	"os"
	"runtime/debug"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

// SetMaxResources sets system resource limits for Unix-like systems.
func SetMaxResources() error {
	// Increase max number of threads to 90% of system's maximum.
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

	// Set the Go runtime's max threads to 90% of the system max thread limit, if higher than default
	var threads uint32
	if isLinux() {
		// On Linux, read the thread max from /proc/sys/kernel/threads-max
		threads, err = readLinuxMaxThreads()
		if err != nil {
			return fmt.Errorf("unable to read max threads: %v", err)
		}
	} else {
		// On macOS/FreeBSD, use SysctlUint32 (this is supported)
		threads, err = unix.SysctlUint32("kern.threads.max")
		if err != nil {
			return fmt.Errorf("unable to get max threads via sysctl: %v", err)
		}
	}

	maxThreads := (int(threads) * 90) / 100
	if maxThreads > threadLimit {
		debug.SetMaxThreads(maxThreads)
	}

	fmt.Println("System resources adjusted for high performance on Unix.")
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

// isLinux checks if the operating system is Linux.
func isLinux() bool {
	return os.Getenv("GOOS") == "linux" || os.Getenv("OSTYPE") == "linux-gnu"
}
