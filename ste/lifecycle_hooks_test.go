package ste

import (
	"strings"
	"testing"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

type lifecycleHookTestHost struct {
	common.LifecycleMgr
	progress string
	closed   bool
}

func (h *lifecycleHookTestHost) Progress(builder common.OutputBuilder) {
	h.progress = builder(common.EOutputFormat.Text())
}

func (h *lifecycleHookTestHost) RegisterCloseFunc(close func()) {
	close()
	h.closed = true
}

func (h *lifecycleHookTestHost) SanitizeLogMessage(message string) string {
	return common.NewAzCopyLogSanitizer().SanitizeLogMessage(message)
}

func TestEngineUIHooksPreserveHostLifecycle(t *testing.T) {
	previous := common.GetLifecycleMgr()
	defer common.SetLifecycleMgr(previous)
	host := &lifecycleHookTestHost{}
	common.SetLifecycleMgr(host)
	var information string
	common.SetUIHooks(&common.JobUIHooks{Info: func(message string) { information = message }})

	manager := common.GetLifecycleMgr()
	manager.Info("engine information")
	manager.Progress(func(common.OutputFormat) string { return "job progress" })
	closeCalled := false
	manager.RegisterCloseFunc(func() { closeCalled = true })

	if information != "engine information" || host.progress != "job progress" || !host.closed || !closeCalled {
		t.Fatal("installing engine UI hooks lost host lifecycle callbacks")
	}
}

func TestEngineUIHooksNeverSuppressFatalErrors(t *testing.T) {
	previous := common.GetLifecycleMgr()
	defer common.SetLifecycleMgr(previous)
	common.SetUIHooks(&common.JobUIHooks{})

	defer func() {
		recovered := recover()
		if recovered == nil {
			t.Fatal("an omitted fatal callback must fail visibly")
		}
		message, ok := recovered.(string)
		if !ok || strings.Contains(message, "secret-signature") {
			t.Fatal("fatal fallback must sanitize its message")
		}
	}()
	common.GetLifecycleMgr().Error("failure https://account.blob.core.windows.net/container/blob?sig=secret-signature")
}
