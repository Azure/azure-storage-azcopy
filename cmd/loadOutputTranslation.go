// Copyright © Microsoft <wastore@microsoft.com>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package cmd

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/Azure/azure-storage-azcopy/ste"

	"github.com/Azure/azure-storage-azcopy/common"
)

const (
	clfsErrTag            = "ERR"
	clfsInitializationTag = "AZCOPY-CONFIG:"
	clfsInfoTag           = "AZCOPY-INFO:"
	clfsProgressTag       = "AZCOPY:"
	clfsExitTag           = "AZCOPY-FINAL:"
)

type clfsExtensionOutputParser struct {
	lcm          common.LifecycleMgr
	finishSignal chan interface{} // TODO consider using a wait group
}

func newClfsExtensionOutputParser(lcm common.LifecycleMgr) *clfsExtensionOutputParser {
	return &clfsExtensionOutputParser{lcm: lcm, finishSignal: make(chan interface{}, 1)}
}

// should be started in another go-routine
func (c *clfsExtensionOutputParser) startParsing(reader io.Reader) {
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		line := scanner.Text()

		if strings.Contains(line, clfsExitTag) {
			c.processEndOfJob(line)
		} else if strings.Contains(line, clfsInitializationTag) {
			c.processInitialization(line)
		} else if strings.Contains(line, clfsInfoTag) {
			c.processInfo(line)
		} else if strings.Contains(line, clfsProgressTag) {
			c.processProgress(line)
		} else if strings.Contains(line, clfsErrTag) {
			c.processError(line)
		} else {
			//ignore all other output
			//c.lcm.Info("ignored: " + line)
		}
	}

	c.finishSignal <- ""
}

func (c *clfsExtensionOutputParser) finishParsing() {
	<-c.finishSignal
}

func (c *clfsExtensionOutputParser) processInitialization(line string) {
	cleanMessage := strings.TrimPrefix(line, clfsInitializationTag)

	initInfo := clfsInitialization{}
	err := json.Unmarshal([]byte(cleanMessage), &initInfo)
	if err != nil {
		// don't crash if these info are not critical and are only nice-to-have
		return
	}

	c.lcm.Info(fmt.Sprintf("CLFSLoad Extension version: %s", initInfo.Version))
	c.lcm.Info(fmt.Sprintf("CLFSLoad Extension configurations: compression type=%s, preserve hard links=%v",
		initInfo.Compression, initInfo.PreserveHardLinks))

	if initInfo.New {
		c.lcm.Info("Starting a new job.")
	}
}

func (c *clfsExtensionOutputParser) processInfo(line string) {
	cleanMessage := strings.TrimPrefix(line, clfsInfoTag)

	info := clfsInfo{}
	err := json.Unmarshal([]byte(cleanMessage), &info)
	if err != nil {
		// don't crash if these info are not critical and are only nice-to-have
		return
	}

	c.lcm.Info(fmt.Sprintf("%s phase has started.", info.Phase))
}

func (c *clfsExtensionOutputParser) processError(line string) {
	parts := strings.Split(line, clfsErrTag)
	if len(parts) < 2 {
		return
	}

	c.lcm.Info(strings.TrimSpace(parts[1]))
}

func (c *clfsExtensionOutputParser) processProgress(line string) {
	cleanMessage := strings.TrimPrefix(line, clfsProgressTag)

	progress := clfsProgress{}
	err := json.Unmarshal([]byte(cleanMessage), &progress)
	if err != nil {
		// abort, something went wrong
		c.lcm.Error("Unable to handle the output from CLFS extension, please contact the dev team through Github.")
	}

	c.lcm.Progress(func(format common.OutputFormat) string {
		if format == common.EOutputFormat.Text() {
			return progress.parseIntoString()
		} else if format == common.EOutputFormat.Json() {
			// re-marshal it the Go way
			jsonOutput, err := json.Marshal(progress)
			common.PanicIfErr(err)
			return string(jsonOutput)
		}

		return ""
	})
}

func (c *clfsExtensionOutputParser) processEndOfJob(line string) {
	cleanMessage := strings.TrimPrefix(line, clfsExitTag)

	summary := clfsSummary{}
	err := json.Unmarshal([]byte(cleanMessage), &summary)
	if err != nil {
		// abort, something went wrong
		c.lcm.Error("Unable to handle the output from CLFS extension, please contact the dev team through Github.")
	}

	c.lcm.Exit(func(format common.OutputFormat) string {
		if format == common.EOutputFormat.Text() {
			return summary.parseIntoString()
		} else if format == common.EOutputFormat.Json() {
			// re-marshal it the Go way
			jsonOutput, err := json.Marshal(summary)
			common.PanicIfErr(err)
			return string(jsonOutput)
		}

		return ""
	}, common.EExitCode.Success())
}

type clfsInitialization struct {
	Version           string `json:"version"`
	Compression       string `json:"compression"`
	New               bool   `json:"new"`
	PreserveHardLinks bool   `json:"preserve_hardlinks"`
}

type clfsInfo struct {
	Phase string `json:"phase"`
}

type clfsProgress struct {
	Elapsed             float64 `json:"elapsed"`
	FilesFailed         int     `json:"files_completed_failed"`
	FilesCompleted      int     `json:"files_completed_success"`
	FilePending         int     `json:"files_pending"`
	FilesSkipped        int     `json:"files_skipped"`
	FilesTotal          int     `json:"files_total"`
	PercentComplete     float64 `json:"pct_complete"`
	Phase               string  `json:"phase"`
	ThroughputMbps      float64 `json:"throughput_Mbps"`
	ThroughputDeltaSecs float64 `json:"throughput_delta_secs"`
	TimeEta             float64 `json:"time_eta"`
	TimeReport          float64 `json:"time_report"`
}

func (progress clfsProgress) parseIntoString() string {
	return fmt.Sprintf("%.1f %%, %v Done, %v Failed, %v Pending, %v Skipped, %v Total, Throughput (Mb/s): %v",
		progress.PercentComplete,
		progress.FilesCompleted,
		progress.FilesFailed,
		progress.FilePending,
		progress.FilesSkipped,
		progress.FilesTotal,
		progress.ThroughputMbps)
}

type clfsSummary struct {
	Elapsed               float64 `json:"elapsed"`
	FilesFailed           int     `json:"files_completed_failed"`
	FilesCompleted        int     `json:"files_completed_success"`
	FilesSkipped          int     `json:"files_skipped"`
	FilesTotal            int     `json:"files_total"`
	FinalJobStatus        string  `json:"final_job_status"`
	TotalBytesTransferred int     `json:"total_bytes_transferred"`
}

func (summary clfsSummary) parseIntoString() string {
	return fmt.Sprintf(
		`
Elapsed Time (Minutes): %v
Total Number Of Transfers: %v
Number of Transfers Completed: %v
Number of Transfers Failed: %v
Number of Transfers Skipped: %v
TotalBytesTransferred: %v
Final Job Status: %v
`,
		ste.ToFixed(summary.Elapsed/60, 4),
		summary.FilesTotal,
		summary.FilesCompleted,
		summary.FilesFailed,
		summary.FilesSkipped,
		summary.TotalBytesTransferred,
		summary.FinalJobStatus,
	)
}
