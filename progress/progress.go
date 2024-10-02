package progress

import (
	"sync"
	"time"

	"github.com/cheggaaa/pb/v3"
	"github.com/fatih/color"
	"github.com/minio/pkg/console"
)

// Mutex to protect concurrent writes
var printMu sync.Mutex

// ProgressBar wrapper structure
type ProgressBar struct {
	*pb.ProgressBar
}

// NewProgressBar - instantiate a progress bar.
func NewProgressBar(total int64) *ProgressBar {
	// Progress bar specific theme customization.
	console.SetColor("Bar", color.New(color.FgGreen, color.Bold))

	// Initialize the original progress bar.
	bar := pb.New64(total)

	// Customize the refresh rate and behavior
	bar.SetRefreshRate(time.Millisecond * 125)
	bar.SetTemplateString(`{{counters . }} {{bar . }} {{percent . }} {{speed . }}`)

	// Start the progress bar
	bar.Start()

	return &ProgressBar{ProgressBar: bar}
}

// SetCaption sets the caption of the progress bar.
func (p *ProgressBar) SetCaption(caption string) *ProgressBar {
	p.ProgressBar.Set("prefix", caption)
	return p
}
