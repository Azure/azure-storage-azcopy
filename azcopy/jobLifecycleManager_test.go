package azcopy

import (
	"context"
	"testing"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/stretchr/testify/require"
)

type incompleteEnumerationProgressTracker struct{}

func (incompleteEnumerationProgressTracker) Start() {}

func (incompleteEnumerationProgressTracker) CheckProgress() (uint32, bool) {
	return 0, false
}

func (incompleteEnumerationProgressTracker) CompletedEnumeration() bool {
	return false
}

func (incompleteEnumerationProgressTracker) GetJobID() common.JobID {
	return common.JobID{}
}

func (incompleteEnumerationProgressTracker) GetElapsedTime() time.Duration {
	return 0
}

func TestContextCancellationDoesNotPromptDuringEnumeration(t *testing.T) {
	promptCalled := make(chan struct{}, 1)
	releasePrompt := make(chan struct{})
	defer close(releasePrompt)

	warningMessages := make(chan string, 1)
	hooks := common.NewJobUIHooks()
	hooks.Warn = func(message string) {
		warningMessages <- message
	}
	hooks.Prompt = func(string, common.PromptDetails) common.ResponseOption {
		promptCalled <- struct{}{}
		<-releasePrompt
		return common.EResponseOption.Yes()
	}

	manager := NewJobLifecycleManager(hooks)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	manager.InitiateProgressReporting(ctx, incompleteEnumerationProgressTracker{})

	waitResult := make(chan error, 1)
	go func() {
		waitResult <- manager.Wait()
	}()

	select {
	case <-promptCalled:
		t.Fatal("context cancellation prompted for user input")
	case err := <-waitResult:
		require.ErrorContains(t, err, "cancel job requires the JobID")
		require.Equal(t, "The enumeration is not complete; cancelling the job at this point means it cannot be resumed.", <-warningMessages)
	case <-time.After(time.Second):
		t.Fatal("context cancellation did not reach job cancellation")
	}
}
