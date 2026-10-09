package ste

import (
	"context"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/common/buildmode"
	"github.com/stretchr/testify/assert"
)

const (
	minConcurrency = 32
	maxConcurrency = 300
)

func TestConcurrencyValue(t *testing.T) {
	a := assert.New(t)
	// weak machines
	for i := 1; i < 5; i++ {
		min, max := getMainPoolSize(i)
		a.Equal(minConcurrency, min)
		a.Equal(minConcurrency, max.Value)
	}

	// moderately powerful machines
	for i := 5; i < 19; i++ {
		min, max := getMainPoolSize(i)
		a.Equal(16*i, min)
		a.Equal(16*i, max.Value)
	}

	// powerful machines
	for i := 19; i < 24; i++ {
		min, max := getMainPoolSize(i)
		a.Equal(maxConcurrency, min)
		a.Equal(maxConcurrency, max.Value)
	}
}

type starvationTestLogger struct {
	common.ILoggerResetable
}

func (starvationTestLogger) Log(common.LogLevel, string) {}

func newStarvationTestJobMgr() *jobMgr {
	return &jobMgr{
		logger: starvationTestLogger{},
		jstm:   &jobStatusManager{},
		xferChannels: XferChannels{
			normalChunckCh:  make(chan chunkFunc, 8),
			lowChunkCh:      make(chan chunkFunc, 8),
			closeTransferCh: make(chan struct{}),
		},
		poolSizingChannels: poolSizingChannels{
			entryNotificationCh: make(chan struct{}, 1),
			exitNotificationCh:  make(chan struct{}, 1),
			scalebackRequestCh:  make(chan struct{}),
		},
	}
}

func waitForStarvationTestSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not signal before timeout")
	}
}

func TestChunkStarvationCounter(t *testing.T) {
	for _, priority := range []string{"normal", "low"} {
		for _, initialState := range []string{"ready", "empty"} {
			t.Run(priority+"/"+initialState, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					jm := newStarvationTestJobMgr()
					queue := jm.xferChannels.normalChunckCh
					if priority == "low" {
						queue = jm.xferChannels.lowChunkCh
					}
					started, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
					var releaseOnce sync.Once
					unblock := func() { releaseOnce.Do(func() { close(release) }) }
					work := func(int) { close(started); <-release }
					if initialState == "ready" {
						queue <- work
					}
					go func() { jm.chunkProcessor(0); close(done) }()
					defer func() {
						close(jm.poolSizingChannels.scalebackRequestCh)
						unblock()
						waitForStarvationTestSignal(t, done)
					}()

					var expected int64
					if buildmode.HighPerf() && (initialState == "empty" || priority == "low") {
						expected = 1
					}
					if initialState == "empty" {
						synctest.Wait()
						time.Sleep(35 * time.Millisecond)
						assert.Equal(t, expected, jm.GetChannelStats().ChunkStarveCount, "blocked workers must not poll")
						queue <- work
					}
					waitForStarvationTestSignal(t, started)
					assert.Equal(t, expected, jm.GetChannelStats().ChunkStarveCount, "only high-perf normal-queue-empty fallbacks are counted")
					unblock()
					if buildmode.HighPerf() {
						expected++
					}
					synctest.Wait()
					time.Sleep(35 * time.Millisecond)
					assert.Equal(t, expected, jm.GetChannelStats().ChunkStarveCount)
					select {
					case jm.poolSizingChannels.scalebackRequestCh <- struct{}{}:
					case <-time.After(5 * time.Second):
						t.Fatal("idle worker did not accept scaleback")
					}
					waitForStarvationTestSignal(t, done)
					assert.Equal(t, expected, jm.GetChannelStats().ChunkStarveCount, "shutdown must not decrement the counter")
				})
			})
		}
	}
}

type starvationTestJobPart struct {
	IJobPartMgr
	started chan struct{}
	release <-chan struct{}
}

func (*starvationTestJobPart) Plan() *JobPartPlanHeader    { return &JobPartPlanHeader{} }
func (*starvationTestJobPart) Log(common.LogLevel, string) {}
func (p *starvationTestJobPart) StartJobXfer(IJobPartTransferMgr) {
	close(p.started)
	<-p.release
}

func TestTransferStarvationCounter(t *testing.T) {
	for _, priority := range []string{"normal", "low"} {
		t.Run(priority, func(t *testing.T) {
			jm := newStarvationTestJobMgr()
			normal, low := make(chan IJobPartTransferMgr, 1), make(chan IJobPartTransferMgr, 1)
			jm.xferChannels.normalTransferCh, jm.xferChannels.lowTransferCh = normal, low
			queue := normal
			if priority == "low" {
				queue = low
			}
			release, done := make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			part := &starvationTestJobPart{started: make(chan struct{}), release: release}
			queue <- &jobPartTransferMgr{ctx: context.Background(), jobPartMgr: part}
			go func() { jm.transferProcessor(0); close(done) }()
			t.Cleanup(func() {
				close(jm.xferChannels.closeTransferCh)
				unblock()
				waitForStarvationTestSignal(t, done)
			})
			waitForStarvationTestSignal(t, part.started)
			assert.Zero(t, jm.GetChannelStats().TransferStarveCount, "ready work must not count as starvation")
			unblock()
			if !assert.Eventually(t, func() bool {
				return jm.GetChannelStats().TransferStarveCount >= 2
			}, 5*time.Second, time.Millisecond) {
				t.FailNow()
			}

			releaseNext := make(chan struct{})
			defer close(releaseNext)
			next := &starvationTestJobPart{started: make(chan struct{}), release: releaseNext}
			queue <- &jobPartTransferMgr{ctx: context.Background(), jobPartMgr: next}
			waitForStarvationTestSignal(t, next.started)
			count := jm.GetChannelStats().TransferStarveCount
			assert.GreaterOrEqual(t, count, int64(2), "processing work must not decrement the counter")
			time.Sleep(35 * time.Millisecond)
			assert.Equal(t, count, jm.GetChannelStats().TransferStarveCount, "executing work must not increment the counter")
		})
	}
}
