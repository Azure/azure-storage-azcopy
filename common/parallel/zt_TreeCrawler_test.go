package parallel

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCrawlContinuesAfterDirectoryErrors(t *testing.T) {
	for _, message := range []string{"403 AuthorizationPermissionMismatch", "directory listing failed"} {
		t.Run(message, func(t *testing.T) {
			const failedDirs, healthyDirs = 1100, 100
			directoryErr := errors.New(message)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			var healthy atomic.Int64
			output, stats := CrawlWithStats(ctx, -1, func(dir Directory, enqueue func(Directory), _ func(DirectoryEntry, error)) error {
				n := dir.(int)
				if n == -1 {
					for i := 0; i < failedDirs+healthyDirs; i++ {
						enqueue(i)
					}
					return nil
				}
				if n < failedDirs {
					return directoryErr
				}
				healthy.Add(1)
				return nil
			}, 64)

			var received int
			for result := range output {
				_, err := result.Item()
				assert.ErrorIs(t, err, directoryErr)
				received++
			}
			require.NoError(t, ctx.Err(), "crawl must finish rather than wait for cancellation")
			assert.Equal(t, failedDirs, received)
			assert.Equal(t, int64(healthyDirs), healthy.Load())
			assert.Zero(t, atomic.LoadInt64(&stats.ActiveWorkers))
			assert.Zero(t, atomic.LoadInt64(&stats.QueuedDirs))
		})
	}
}

func TestCrawlErrorSendStopsOnCancellation(t *testing.T) {
	// Exercise the worker directly so the output stays full until the worker exits.
	// Reading it before exit would hide an uncancellable send.
	const workers = 64
	directoryErr := errors.New("403 AuthorizationPermissionMismatch")
	output := make(chan CrawlResult, 1000)
	for i := 0; i < cap(output); i++ {
		output <- CrawlResult{err: directoryErr}
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan struct{}, workers)
	c := &crawler{
		output:      output,
		parallelism: workers,
		stats:       &CrawlStats{},
		cond:        sync.NewCond(&sync.Mutex{}),
		workerBody: func(Directory, func(Directory), func(DirectoryEntry, error)) error {
			started <- struct{}{}
			return directoryErr
		},
	}
	for i := 0; i < workers+1; i++ {
		c.unstartedDirs = append(c.unstartedDirs, i)
	}
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go c.workerLoop(ctx, &wg, i)
	}
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		// Unblock workers even if the implementation regresses, without leaking goroutines.
		for i := 0; i < workers; i++ {
			select {
			case <-output:
			default:
			}
		}
		<-done
	})
	for i := 0; i < workers; i++ {
		select {
		case <-started:
		case <-time.After(5 * time.Second):
			t.Fatal("workers did not reach directory error handling")
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("workers remained blocked on full output after cancellation")
	}
	assert.Len(t, output, cap(output), "cancellation must not require draining output")
	assert.Zero(t, atomic.LoadInt64(&c.stats.ActiveWorkers))
	assert.Equal(t, int64(1), atomic.LoadInt64(&c.stats.QueuedDirs))
}
