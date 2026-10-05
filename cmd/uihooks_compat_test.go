package cmd

import (
	"strings"
	"testing"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

type lifecycleCompatHost struct {
	mockedLifecycleManager
	controller     common.WorkController
	closeCallbacks []func()
	cancelEnabled  bool
	allowOpenCalls int
	promptMessage  string
	promptDetails  common.PromptDetails
}

func (h *lifecycleCompatHost) InitiateProgressReporting(controller common.WorkController) {
	h.controller = controller
}

func (h *lifecycleCompatHost) RegisterCloseFunc(callback func()) {
	h.closeCallbacks = append(h.closeCallbacks, callback)
}

func (h *lifecycleCompatHost) EnableCancelFromStdIn()  { h.cancelEnabled = true }
func (h *lifecycleCompatHost) E2EAwaitAllowOpenFiles() { h.allowOpenCalls++ }

func (h *lifecycleCompatHost) Prompt(message string, details common.PromptDetails) common.ResponseOption {
	h.promptMessage, h.promptDetails = message, details
	return common.EResponseOption.Yes()
}

type lifecycleCompatWork struct {
	cancelManager   common.LifecycleMgr
	progressManager common.LifecycleMgr
}

func (w *lifecycleCompatWork) Cancel(manager common.LifecycleMgr) {
	w.cancelManager = manager
}

func (w *lifecycleCompatWork) ReportProgressOrExit(manager common.LifecycleMgr) uint32 {
	w.progressManager = manager
	manager.Progress(func(common.OutputFormat) string { return "host progress" })
	return 17
}

func installLifecycleCompatHost(t *testing.T) *lifecycleCompatHost {
	t.Helper()
	previousCommandManager := GetLifecycleMgr()
	previousCommonManager := common.GetLifecycleMgr()
	previousFormat := OutputFormat
	t.Cleanup(func() {
		glcm = previousCommandManager
		common.SetLifecycleMgr(previousCommonManager)
		OutputFormat = previousFormat
	})

	host := &lifecycleCompatHost{mockedLifecycleManager: mockedLifecycleManager{
		infoLog:     make(chan string, 10),
		warnLog:     make(chan string, 10),
		errorLog:    make(chan string, 10),
		progressLog: make(chan string, 10),
	}}
	SetLifecycleMgr(host)
	return host
}

func expectLifecycleCompatMessage(t *testing.T, messages <-chan string, expected string) {
	t.Helper()
	select {
	case message := <-messages:
		if message != expected {
			t.Fatalf("received %q, expected %q", message, expected)
		}
	default:
		t.Fatalf("host did not receive %q", expected)
	}
}

func verifyLifecycleCompatWork(t *testing.T, host *lifecycleCompatHost, manager common.LifecycleMgr) {
	t.Helper()
	work := &lifecycleCompatWork{}
	manager.InitiateProgressReporting(work)
	if host.controller != work {
		t.Fatal("host did not receive the work controller")
	}
	manager.EnableCancelFromStdIn()
	if !host.cancelEnabled {
		t.Fatal("host did not receive the cancellation registration")
	}
	host.controller.Cancel(manager)
	if work.cancelManager != manager {
		t.Fatal("cancellation did not preserve the lifecycle manager")
	}
	if count := host.controller.ReportProgressOrExit(manager); count != 17 || work.progressManager != manager {
		t.Fatal("progress reporting did not preserve the controller result and lifecycle manager")
	}
	expectLifecycleCompatMessage(t, host.progressLog, "host progress")

	closed := false
	manager.RegisterCloseFunc(func() { closed = true })
	if closed || len(host.closeCallbacks) != 1 {
		t.Fatal("close callback was not deferred to the host")
	}
	host.closeCallbacks[0]()
	if !closed {
		t.Fatal("registered close callback was lost")
	}
}

func TestUIHooksCompatibilityInstalledHost(t *testing.T) {
	host := installLifecycleCompatHost(t)
	if GetLifecycleMgr() != host || common.GetLifecycleMgr() != host {
		t.Fatal("command and engine lifecycle managers must refer to the installed host")
	}
	manager := common.GetLifecycleMgr()
	manager.Info("host information")
	manager.Warn("host warning")
	manager.Error("host error")
	expectLifecycleCompatMessage(t, host.infoLog, "host information")
	expectLifecycleCompatMessage(t, host.warnLog, "host warning")
	expectLifecycleCompatMessage(t, host.errorLog, "host error")

	details := common.PromptDetails{PromptType: common.EPromptType.Cancel(), PromptTarget: "test-job"}
	if response := manager.Prompt("cancel test-job?", details); response != common.EResponseOption.Yes() {
		t.Fatal("host prompt response was not forwarded")
	}
	if host.promptMessage != "cancel test-job?" || host.promptDetails.PromptTarget != details.PromptTarget ||
		host.promptDetails.PromptType != details.PromptType {
		t.Fatal("host did not receive the prompt details")
	}
	manager.E2EAwaitAllowOpenFiles()
	if host.allowOpenCalls != 1 {
		t.Fatal("engine open-files callback was not forwarded to the host")
	}
	verifyLifecycleCompatWork(t, host, manager)
}

func TestUIHooksCompatibilityPartialHooks(t *testing.T) {
	host := installLifecycleCompatHost(t)
	var information, warning, failure string
	var promptDetails common.PromptDetails
	allowOpenCalls := 0
	common.SetUIHooks(&common.JobUIHooks{
		Info:  func(message string) { information = message },
		Warn:  func(message string) { warning = message },
		Error: func(message string) { failure = message },
		Prompt: func(_ string, details common.PromptDetails) common.ResponseOption {
			promptDetails = details
			return common.EResponseOption.Yes()
		},
		E2EAwaitAllowOpenFiles: func() { allowOpenCalls++ },
	})
	manager := common.GetLifecycleMgr()
	manager.Info("hook information")
	manager.Warn("hook warning")
	manager.Error("hook error")
	if information != "hook information" || warning != "hook warning" || failure != "hook error" {
		t.Fatal("engine callbacks did not reach the supplied UI hooks")
	}
	if response := manager.Prompt("continue?", common.PromptDetails{PromptTarget: "hook-job"}); response != common.EResponseOption.Yes() ||
		promptDetails.PromptTarget != "hook-job" {
		t.Fatal("engine prompt callback lost its details or response")
	}
	manager.E2EAwaitAllowOpenFiles()
	if allowOpenCalls != 1 || host.allowOpenCalls != 0 {
		t.Fatal("engine open-files callback did not use the installed hook")
	}
	if GetLifecycleMgr() != host {
		t.Fatal("engine-only callbacks replaced the command host")
	}
	verifyLifecycleCompatWork(t, host, manager)
}

func TestUIHooksCompatibilityOutputFormat(t *testing.T) {
	host := installLifecycleCompatHost(t)
	for _, format := range []common.OutputFormat{common.EOutputFormat.Text(), common.EOutputFormat.Json(), common.EOutputFormat.None()} {
		t.Run(format.String(), func(t *testing.T) {
			SetOutputFormat(format)
			if OutputFormat != format || host.outputFormat != format {
				t.Fatal("SetOutputFormat must update the exported value and the installed host")
			}
			var builderFormat common.OutputFormat
			common.GetLifecycleMgr().Output(func(format common.OutputFormat) string {
				builderFormat = format
				return "formatted output"
			}, common.EOutputMessageType.ListSummary())
			if builderFormat != format {
				t.Fatal("host response builder received a stale output format")
			}
		})
	}
}

func TestUIHooksCompatibilityFatalFallback(t *testing.T) {
	installLifecycleCompatHost(t)
	for _, test := range []struct {
		name  string
		error func(string)
	}{
		{name: "constructor", error: common.NewJobUIHooks().Error},
		{name: "nil-hooks", error: func(message string) {
			common.SetUIHooks(nil)
			common.GetLifecycleMgr().Error(message)
		}},
		{name: "omitted-error-hook", error: func(message string) {
			common.SetUIHooks(&common.JobUIHooks{Info: func(string) {}})
			common.GetLifecycleMgr().Error(message)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			defer func() {
				recovered := recover()
				message, ok := recovered.(string)
				if !ok || !strings.Contains(message, "fatal failure") {
					t.Fatalf("fatal fallback did not report the failure: %v", recovered)
				}
				if strings.Contains(message, "secret-signature") {
					t.Fatal("fatal fallback exposed the unredacted signature")
				}
			}()
			test.error("fatal failure https://account.blob.core.windows.net/container/blob?sig=secret-signature")
		})
	}
}
