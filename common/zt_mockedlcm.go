package common

// Needed so UT don't panic when they try to use the lcm in common
type MockedJobLifecycleHandler struct {
}

func (m MockedJobLifecycleHandler) OnStart(ctx JobContext) {
}

func (m MockedJobLifecycleHandler) OnScanProgress(progress ScanProgress) {
}

func (m MockedJobLifecycleHandler) OnTransferProgress(progress TransferProgress) {
}

func (m MockedJobLifecycleHandler) OnComplete(summary JobSummary) {
}

func (m MockedJobLifecycleHandler) Error(s string) {
}

func (m MockedJobLifecycleHandler) RegisterCloseFunc(f func()) {
}

func (m MockedJobLifecycleHandler) Prompt(message string, details PromptDetails) ResponseOption {
	return EResponseOption.Default()
}

func (m MockedJobLifecycleHandler) Info(s string) {
}

func (m MockedJobLifecycleHandler) Warn(s string) {
}

func (m MockedJobLifecycleHandler) E2EAwaitAllowOpenFiles() {
}
