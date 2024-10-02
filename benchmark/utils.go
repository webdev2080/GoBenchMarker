package benchmark

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/objectstorage"
)

type nopCloser struct {
	io.Reader
}

func (nopCloser) Close() error { return nil }

// GenerateRandomName creates a random hex string for object names
func GenerateRandomName(length int) (string, error) {
	bytes := make([]byte, length)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}

// CheckLogForThrottling scans the log file for "429 TooManyRequests" errors
func CheckLogForThrottling(logFileName string) bool {
	file, err := os.Open(logFileName)
	if err != nil {
		fmt.Printf("Error opening log file: %v\n", err)
		return false
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.Contains(line, "429") && strings.Contains(line, "TooManyRequests") {
			return true
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Printf("Error reading log file: %v\n", err)
	}

	return false
}

// uploadWithRetry attempts to upload an object with retry logic for 503 and 429 errors, now using a context
func uploadWithRetry(client objectstorage.ObjectStorageClient, request objectstorage.PutObjectRequest, retries int, logFile *os.File, ctx context.Context) error {
	for i := 0; i < retries; i++ {
		// Use the context with the request to handle timeouts
		_, err := client.PutObject(ctx, request)
		if err != nil {
			if ociError, ok := err.(common.ServiceError); ok && (ociError.GetHTTPStatusCode() == 503 || ociError.GetHTTPStatusCode() == 429) {
				fmt.Fprintf(logFile, "Retrying after error (%d/%d): %s\n", i+1, retries, err.Error())
				time.Sleep(time.Duration(i+1) * time.Second) // Exponential backoff
				continue
			}
			// Handle if the context itself is canceled or exceeded timeout
			if ctx.Err() != nil {
				return ctx.Err() // Return the context error if it was canceled or timed out
			}
			return err
		}
		return nil // Success
	}
	return fmt.Errorf("failed after %d retries", retries)
}
