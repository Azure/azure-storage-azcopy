// Copyright © 2017 Microsoft <wastore@microsoft.com>
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
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/common/buildmode"
	"github.com/Azure/azure-storage-azcopy/v10/common/parallel"
	"github.com/stretchr/testify/assert"
)

type crawlTestLogger struct {
	common.ILoggerResetable
	mu       sync.Mutex
	messages []string
	levels   []common.LogLevel
}

func (l *crawlTestLogger) ShouldLog(common.LogLevel) bool { return false }
func (l *crawlTestLogger) Log(level common.LogLevel, message string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.messages = append(l.messages, message)
	l.levels = append(l.levels, level)
}
func (l *crawlTestLogger) text() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return strings.Join(l.messages, "\n")
}

type crawlTestLifecycle struct{ common.LifecycleMgr }

func (*crawlTestLifecycle) Info(string) {}
func (*crawlTestLifecycle) Warn(string) {}

func captureCrawlLogs(t *testing.T) *crawlTestLogger {
	t.Helper()
	logger := &crawlTestLogger{}
	oldLogger, oldLCM := azcopyScanningLogger, glcm
	azcopyScanningLogger, glcm = logger, &crawlTestLifecycle{}
	t.Cleanup(func() { azcopyScanningLogger, glcm = oldLogger, oldLCM })
	return logger
}

type crawlStatsTestLifecycle struct {
	common.LifecycleMgr
	messages []string
}

func (l *crawlStatsTestLifecycle) Info(message string) {
	l.messages = append(l.messages, message)
}

func TestLogBlobCrawlStats(t *testing.T) {
	for _, withLogger := range []bool{false, true} {
		t.Run(fmt.Sprintf("withLogger=%t", withLogger), func(t *testing.T) {
			logger := captureCrawlLogs(t)
			lifecycle := &crawlStatsTestLifecycle{}
			glcm = lifecycle
			if !withLogger {
				azcopyScanningLogger = nil
			}
			message := "[CrawlConfig] maxQueueDirectories=100000000"
			logBlobCrawlStats(message)
			assert.Equal(t, []string{"[AzCopy] [INFO] " + message}, lifecycle.messages)
			if withLogger {
				assert.Equal(t, []string{"[INFO] " + message}, logger.messages)
				assert.Equal(t, []common.LogLevel{common.LogError}, logger.levels)
			} else {
				assert.Empty(t, logger.messages)
			}
		})
	}
}

func TestResolveHighPerfMaxQueueDirs(t *testing.T) {
	captureCrawlLogs(t)
	for _, tc := range []struct {
		value string
		want  int
	}{
		{"", 50_000_000},
		{"   ", 50_000_000},
		{"100000000", 100_000_000},
		{" 1234567 ", 1_234_567},
		{"0", 50_000_000},
		{"-1", 50_000_000},
		{"invalid", 50_000_000},
		{"999999999999999999999", 50_000_000},
		{"17", 17}, // A subsequent job's override must be reread.
	} {
		t.Run(tc.value, func(t *testing.T) {
			t.Setenv("MOVER_HIGH_PERF_MAX_QUEUED_DIRS", tc.value)
			want := tc.want
			if !buildmode.HighPerf() {
				want = 0
			}
			assert.Equal(t, want, resolveHighPerfMaxQueueDirs())
		})
	}
}

func TestBlobCrawlStatsLifecycle(t *testing.T) {
	for _, cancelParent := range []bool{false, true} {
		t.Run(fmt.Sprintf("cancelParent=%t", cancelParent), func(t *testing.T) {
			logger := captureCrawlLogs(t)
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				stats := &parallel.CrawlStats{ActiveWorkers: 7, QueuedDirs: 42, PeakQueuedDirs: 123, MaxQueueDirectories: 50_000_000}
				stop := startBlobCrawlStats(ctx, stats, 100, true)
				defer stop()
				synctest.Wait()
				time.Sleep(30 * time.Second)
				synctest.Wait()
				if buildmode.HighPerf() {
					assert.Contains(t, logger.text(), "[CrawlConfig] mode=blob-parallel, crawlParallelism=100, maxQueueDirectories=50000000, randomDequeue=true")
					assert.Contains(t, logger.text(), "[CrawlStats] mode=blob-parallel, activeWorkers=7/100, queuedDirs=42")
					assert.Contains(t, logger.text(), "peakQueuedDirs=123, final=false")
				}
				if cancelParent {
					cancel()
					synctest.Wait()
				}
				stop()
				if buildmode.HighPerf() {
					assert.Contains(t, logger.text(), "peakQueuedDirs=123, final=true")
				} else {
					assert.Empty(t, logger.text())
				}
				before := logger.text()
				time.Sleep(60 * time.Second)
				synctest.Wait()
				assert.Equal(t, before, logger.text(), "monitor must stop when enumeration exits")
			})
		})
	}
}

func TestBlobParallelCrawlConfiguration(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		fmt.Fprint(w, `<?xml version="1.0" encoding="utf-8"?><EnumerationResults><Blobs/><NextMarker/></EnumerationResults>`)
	}))
	defer server.Close()
	client, err := container.NewClientWithNoCredential(server.URL+"/container", nil)
	if !assert.NoError(t, err) {
		return
	}
	oldParallelism := EnumerationParallelism
	EnumerationParallelism = 100
	defer func() { EnumerationParallelism = oldParallelism }()
	for _, override := range []string{"", "12345"} {
		for _, suppress := range []bool{false, true} {
			t.Run(fmt.Sprintf("override=%s/suppress=%t", override, suppress), func(t *testing.T) {
				logger := captureCrawlLogs(t)
				t.Setenv("MOVER_HIGH_PERF_MAX_QUEUED_DIRS", override)
				traverser := newBlobTraverser(server.URL+"/container", nil, context.Background(),
					InitResourceTraverserOptions{Recursive: true, SuppressCrawlStats: suppress})
				err := traverser.parallelList(client, "container", "", "", nil, func(StoredObject) error {
					t.Error("empty listing must not emit objects")
					return nil
				}, nil)
				assert.NoError(t, err)
				if buildmode.HighPerf() && !suppress {
					want := "50000000"
					if override != "" {
						want = override
					}
					assert.Contains(t, logger.text(), "crawlParallelism=100, maxQueueDirectories="+want)
					assert.Contains(t, logger.text(), "activeWorkers=0/100, queuedDirs=0")
					assert.Contains(t, logger.text(), "final=true")
				} else {
					assert.NotContains(t, logger.text(), "[CrawlConfig]")
					assert.NotContains(t, logger.text(), "[CrawlStats]")
				}
			})
		}
	}
}

func newLocalRes(path string) common.ResourceString {
	return common.ResourceString{Value: path}
}

func newRemoteRes(url string) common.ResourceString {
	r, err := SplitResourceString(url, common.ELocation.Blob())
	if err != nil {
		panic("can't parse resource string")
	}
	return r
}

func TestRelativePath(t *testing.T) {
	a := assert.New(t)
	// setup
	cca := CookedCopyCmdArgs{
		Source:      newLocalRes("a/b/"),
		Destination: newLocalRes("y/z/"),
	}

	object := StoredObject{
		name:         "c.txt",
		entityType:   1,
		relativePath: "c.txt",
	}

	// execute
	srcRelPath := cca.MakeEscapedRelativePath(true, false, false, object)
	destRelPath := cca.MakeEscapedRelativePath(false, true, false, object)

	// assert
	a.Equal("/c.txt", srcRelPath)
	a.Equal("/c.txt", destRelPath)
}
