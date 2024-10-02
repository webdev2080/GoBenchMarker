package benchmark

import (
	"fmt"
	"net/http"
	"time"

	"golang.org/x/net/http2"
)

// newHTTPClient creates a customized HTTP client with optimized transport settings and HTTP/2 support
func newHTTPClient() *http.Client {
	transport := &http.Transport{
		MaxIdleConns:          200,
		MaxIdleConnsPerHost:   50,
		MaxConnsPerHost:       100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}

	// Enable HTTP/2
	err := http2.ConfigureTransport(transport)
	if err != nil {
		panic(fmt.Sprintf("Failed to configure HTTP/2: %v", err))
	}

	client := &http.Client{
		Transport: transport,
		Timeout:   120 * time.Second, // Request timeout
	}

	return client
}
