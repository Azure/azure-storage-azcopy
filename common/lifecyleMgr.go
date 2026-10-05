package common

import (
	"context"
	"sync"
	"sync/atomic"
)

type jobCancellationRequestsKey struct{}

type jobCancellationRequests struct {
	requests <-chan struct{}
	cancel   context.CancelFunc
	enabled  *atomic.Bool
}

// WithJobCancellationRequests allows interactive hosts to request cancellation before
// cancelling the job context, so declining an incomplete-enumeration prompt is meaningful.
func WithJobCancellationRequests(ctx context.Context, requests <-chan struct{}, cancel context.CancelFunc) context.Context {
	return context.WithValue(ctx, jobCancellationRequestsKey{}, jobCancellationRequests{requests: requests, cancel: cancel, enabled: &atomic.Bool{}})
}

func JobCancellationRequests(ctx context.Context) (<-chan struct{}, context.CancelFunc) {
	requests, _ := ctx.Value(jobCancellationRequestsKey{}).(jobCancellationRequests)
	return requests.requests, requests.cancel
}

func SetJobCancellationRequestsEnabled(ctx context.Context, enabled bool) {
	requests, _ := ctx.Value(jobCancellationRequestsKey{}).(jobCancellationRequests)
	if requests.enabled != nil {
		requests.enabled.Store(enabled)
	}
}

func JobCancellationRequestsEnabled(ctx context.Context) bool {
	requests, _ := ctx.Value(jobCancellationRequestsKey{}).(jobCancellationRequests)
	return requests.enabled != nil && requests.enabled.Load()
}

// LifecycleMgr is the public Mover contract. The console implementation lives in cmd.
type LifecycleMgr interface {
	Init(OutputBuilder)
	Progress(OutputBuilder)
	Exit(OutputBuilder, ExitCode)
	SanitizeLogMessage(string) string
	Info(string)
	Warn(string)
	Dryrun(OutputBuilder)
	Output(OutputBuilder, OutputMessageType)
	Error(string)
	Prompt(string, PromptDetails) ResponseOption
	SurrenderControl()
	InitiateProgressReporting(WorkController)
	AllowReinitiateProgressReporting()
	SetOutputFormat(OutputFormat)
	EnableInputWatcher()
	EnableCancelFromStdIn()
	E2EAwaitContinue()
	E2EAwaitAllowOpenFiles()
	E2EEnableAwaitAllowOpenFiles(bool)
	RegisterCloseFunc(func())
	SetForceLogging()
	IsForceLoggingDisabled() bool
	MsgHandlerChannel() <-chan *LCMMsg
	ReportAllJobPartsDone()
	SetOutputVerbosity(OutputVerbosity)
}

type WorkController interface {
	Cancel(LifecycleMgr)
	ReportProgressOrExit(LifecycleMgr) uint32
}

type JobErrorHandler interface {
	Error(string)
}

// JobUIHooks supplies the engine's narrow UI contract without a console dependency.
// Fatal errors must never silently disappear when a host omits its error callback.
type JobUIHooks struct {
	Prompt                 func(string, PromptDetails) ResponseOption
	Info                   func(string)
	Warn                   func(string)
	Error                  func(string)
	E2EAwaitAllowOpenFiles func()
}

func NewJobUIHooks() *JobUIHooks {
	return &JobUIHooks{
		Prompt:                 func(string, PromptDetails) ResponseOption { return EResponseOption.Default() },
		Info:                   func(string) {},
		Warn:                   func(string) {},
		Error:                  func(message string) { panic(NewAzCopyLogSanitizer().SanitizeLogMessage(message)) },
		E2EAwaitAllowOpenFiles: func() {},
	}
}

var lifecycleMu sync.RWMutex
var lcm LifecycleMgr = &hookLifecycleMgr{hooks: NewJobUIHooks()}

func GetLifecycleMgr() LifecycleMgr {
	lifecycleMu.RLock()
	defer lifecycleMu.RUnlock()
	return lcm
}

// SetLifecycleMgr installs a host implementation, including progress, cancellation and close hooks.
func SetLifecycleMgr(manager LifecycleMgr) {
	if manager == nil {
		panic("a lifecycle manager is required")
	}
	lifecycleMu.Lock()
	defer lifecycleMu.Unlock()
	lcm = manager
}

func SetUIHooks(hooks *JobUIHooks) {
	normalized := *NewJobUIHooks()
	if hooks != nil {
		if hooks.Prompt != nil {
			normalized.Prompt = hooks.Prompt
		}
		if hooks.Info != nil {
			normalized.Info = hooks.Info
		}
		if hooks.Warn != nil {
			normalized.Warn = hooks.Warn
		}
		if hooks.Error != nil {
			normalized.Error = hooks.Error
		}
		if hooks.E2EAwaitAllowOpenFiles != nil {
			normalized.E2EAwaitAllowOpenFiles = hooks.E2EAwaitAllowOpenFiles
		}
	}
	lifecycleMu.Lock()
	defer lifecycleMu.Unlock()
	base := lcm
	if adapter, ok := base.(*hookLifecycleMgr); ok {
		base = adapter.LifecycleMgr
	}
	lcm = &hookLifecycleMgr{LifecycleMgr: base, hooks: &normalized}
}

// Full lifecycle operations require a registered host. Embedding forwards those operations
// unchanged, while engine-only users can install the narrow callbacks without starting a console.
type hookLifecycleMgr struct {
	LifecycleMgr
	hooks *JobUIHooks
}

func (h *hookLifecycleMgr) Info(message string)  { h.hooks.Info(h.SanitizeLogMessage(message)) }
func (h *hookLifecycleMgr) Warn(message string)  { h.hooks.Warn(h.SanitizeLogMessage(message)) }
func (h *hookLifecycleMgr) Error(message string) { h.hooks.Error(h.SanitizeLogMessage(message)) }
func (h *hookLifecycleMgr) Prompt(message string, details PromptDetails) ResponseOption {
	return h.hooks.Prompt(message, details)
}
func (h *hookLifecycleMgr) E2EAwaitAllowOpenFiles() { h.hooks.E2EAwaitAllowOpenFiles() }
func (h *hookLifecycleMgr) SanitizeLogMessage(message string) string {
	if h.LifecycleMgr != nil {
		return h.LifecycleMgr.SanitizeLogMessage(message)
	}
	return NewAzCopyLogSanitizer().SanitizeLogMessage(message)
}
func (h *hookLifecycleMgr) SetForceLogging() {
	if h.LifecycleMgr != nil {
		h.LifecycleMgr.SetForceLogging()
		return
	}
	SetForceLogging()
}
func (h *hookLifecycleMgr) IsForceLoggingDisabled() bool {
	if h.LifecycleMgr != nil {
		return h.LifecycleMgr.IsForceLoggingDisabled()
	}
	return IsForceLoggingDisabled()
}

func PanicIfErr(err error) {
	if err != nil {
		panic(err)
	}
}
