package azcopy

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

type lifecycleTestHost struct {
	common.LifecycleMgr
	prompt func() common.ResponseOption
}

func (*lifecycleTestHost) Info(string) {}
func (h *lifecycleTestHost) Prompt(string, common.PromptDetails) common.ResponseOption {
	if h.prompt != nil {
		return h.prompt()
	}
	return common.EResponseOption.Default()
}
func (*lifecycleTestHost) SanitizeLogMessage(message string) string {
	return common.NewAzCopyLogSanitizer().SanitizeLogMessage(message)
}

type lifecycleTestProgress struct {
	start func()
}

type lifecycleTestLogger struct {
	common.ILoggerResetable
	closed atomic.Bool
}

func (l *lifecycleTestLogger) CloseLog() { l.closed.Store(true) }

func (p *lifecycleTestProgress) Start() {
	if p.start != nil {
		p.start()
	}
}
func (*lifecycleTestProgress) CheckProgress() (uint32, bool) { return 0, false }
func (*lifecycleTestProgress) CompletedEnumeration() bool    { return false }
func (*lifecycleTestProgress) firstPartOrdered() bool        { return false }
func (*lifecycleTestProgress) GetJobID() common.JobID        { return common.JobID{} }
func (*lifecycleTestProgress) GetElapsedTime() time.Duration { return 0 }

func waitForLifecycleTest(t *testing.T, manager *jobLifecycleManager) error {
	t.Helper()
	result := make(chan error, 1)
	go func() { result <- manager.Wait() }()
	select {
	case err := <-result:
		return err
	case <-time.After(2 * time.Second):
		t.Fatal("lifecycle completion did not unblock its waiter")
		return nil
	}
}

func TestJobLifecycleBroadcastCompletion(t *testing.T) {
	manager := NewJobLifecycleManager(&lifecycleTestHost{})
	var calls atomic.Int32
	manager.RegisterCloseFunc(func() {
		manager.Stop()
		_ = manager.GetError()
		manager.RegisterCloseFunc(func() { calls.Add(1) })
		manager.OnComplete()
		calls.Add(1)
	})
	waiters := make(chan error, 8)
	for i := 0; i < cap(waiters); i++ {
		go func() { waiters <- manager.Wait() }()
	}
	manager.OnComplete()
	for i := 0; i < cap(waiters); i++ {
		select {
		case err := <-waiters:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("completion was not broadcast to all waiters")
		}
	}
	if calls.Load() != 2 {
		t.Fatalf("completion callbacks ran %d times, want 2", calls.Load())
	}
	manager.RegisterCloseFunc(func() { calls.Add(1) })
	manager.Error("ignored after successful completion")
	if calls.Load() != 3 || manager.GetError() != nil {
		t.Fatal("late registration or idempotent completion changed the result")
	}
}

func TestJobLifecycleCallbackPanicDoesNotStrandWaiters(t *testing.T) {
	manager := NewJobLifecycleManager(&lifecycleTestHost{})
	var closed atomic.Bool
	manager.RegisterCloseFunc(func() {
		panic("https://account.blob.core.windows.net/container?sig=secret-signature")
	})
	manager.RegisterCloseFunc(func() { closed.Store(true) })
	manager.OnComplete()
	err := waitForLifecycleTest(t, manager)
	if err == nil || !strings.Contains(err.Error(), "completion callback panic") ||
		strings.Contains(err.Error(), "secret-signature") || !closed.Load() {
		t.Fatalf("callback failure did not preserve sanitized error and remaining cleanup: %v", err)
	}
}

func TestJobLifecycleProgressStartupPanic(t *testing.T) {
	manager := NewJobLifecycleManager(&lifecycleTestHost{})
	manager.InitiateProgressReporting(context.Background(), &lifecycleTestProgress{start: func() { panic("startup failed") }})
	err := waitForLifecycleTest(t, manager)
	if err == nil || !strings.Contains(err.Error(), "startup failed") {
		t.Fatalf("progress startup failure was lost: %v", err)
	}
	manager.Stop()
}

func TestJobLifecycleCancelBeforeFirstPart(t *testing.T) {
	var prompts atomic.Int32
	manager := NewJobLifecycleManager(&lifecycleTestHost{prompt: func() common.ResponseOption {
		prompts.Add(1)
		return common.EResponseOption.Default()
	}})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	manager.InitiateProgressReporting(ctx, &lifecycleTestProgress{})
	err := waitForLifecycleTest(t, manager)
	manager.Stop()
	if !errors.Is(err, context.Canceled) || prompts.Load() != 0 {
		t.Fatalf("cancellation before STE work was not completed once: %v, prompts=%d", err, prompts.Load())
	}
}

func TestJobLifecycleDeclinedCancellationIsNotRepeated(t *testing.T) {
	prompts := make(chan struct{}, 2)
	manager := NewJobLifecycleManager(&lifecycleTestHost{prompt: func() common.ResponseOption {
		select {
		case prompts <- struct{}{}:
		default:
		}
		return common.EResponseOption.No()
	}})
	defer manager.Stop()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	requests := make(chan struct{}, 1)
	manager.InitiateProgressReporting(common.WithJobCancellationRequests(ctx, requests, cancel), &lifecycleTestProgress{})
	requests <- struct{}{}
	select {
	case <-prompts:
	case <-time.After(2 * time.Second):
		t.Fatal("cancellation was not offered")
	}
	select {
	case <-prompts:
		t.Fatal("a cancelled context repeatedly prompted the host")
	case <-time.After(25 * time.Millisecond):
	}
	if ctx.Err() != nil {
		t.Fatal("declining cancellation cannot cancel the enumeration context")
	}
	manager.OnComplete()
	if err := waitForLifecycleTest(t, manager); err != nil {
		t.Fatal(err)
	}
}

func TestJobLifecycleApprovedCancellationCancelsContext(t *testing.T) {
	manager := NewJobLifecycleManager(&lifecycleTestHost{prompt: func() common.ResponseOption {
		return common.EResponseOption.Yes()
	}})
	defer manager.Stop()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	requests := make(chan struct{}, 1)
	manager.InitiateProgressReporting(common.WithJobCancellationRequests(ctx, requests, cancel), &lifecycleTestProgress{})
	requests <- struct{}{}
	if err := waitForLifecycleTest(t, manager); !errors.Is(err, context.Canceled) || ctx.Err() == nil {
		t.Fatalf("approved cancellation did not cancel the actual enumeration context: %v", err)
	}
}

func TestJobLifecycleDoesNotCloseOwnerLogger(t *testing.T) {
	logger := &lifecycleTestLogger{}
	manager := NewJobLifecycleManager(&lifecycleTestHost{}, logger)
	if manager.JobLogger() != logger {
		t.Fatal("job-specific logger was not retained")
	}
	manager.OnComplete()
	if err := waitForLifecycleTest(t, manager); err != nil {
		t.Fatal(err)
	}
	manager.Stop()
	if logger.closed.Load() {
		t.Fatal("per-job lifecycle closed a caller-owned logger")
	}
}

func TestJobLifecycleStopContextBoundsBlockedHost(t *testing.T) {
	manager := NewJobLifecycleManager(&lifecycleTestHost{})
	started := make(chan struct{})
	release := make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	go manager.InitiateProgressReporting(context.Background(), &lifecycleTestProgress{start: func() {
		close(started)
		<-release
	}})
	<-started
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := manager.StopContext(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("blocked host ignored the bounded reporter join: %v", err)
	}
	close(release)
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := manager.StopContext(ctx); err != nil {
		t.Fatal(err)
	}
	manager.OnComplete()
	if err := waitForLifecycleTest(t, manager); err != nil {
		t.Fatal(err)
	}
}
