package benchmark

import (
	"sync"
)

// BufPool is a global sync.Pool for reusing buffers to avoid excessive memory allocations
var BufPool = sync.Pool{
	New: func() interface{} {
		// You can adjust this default size or set it dynamically when using
		return make([]byte, 4098)
	},
}

// GetBuffer gets a buffer from the pool
func GetBuffer(size int) []byte {
	// Ensure the buffer returned matches the size required
	buf := BufPool.Get().([]byte)
	if cap(buf) < size {
		return make([]byte, size)
	}
	return buf[:size]
}

// PutBuffer returns a buffer to the pool
func PutBuffer(buf []byte) {
	BufPool.Put(buf)
}
