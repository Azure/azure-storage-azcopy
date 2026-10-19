package e2etest

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os/exec"
	"runtime"
	"strings"
	"testing"
	"time"
)

func init() {
	suiteManager.RegisterSuite(&LocalHashScanSuite{})
}

// LocalHashScanSuite runs the traverser's hashing tests on the Windows agent, since the unit-test job only runs on Linux.
type LocalHashScanSuite struct{}

func (*LocalHashScanSuite) Scenario_WindowsFileChecks(svm *ScenarioVariationManager) {
	if svm.Dryrun() {
		return
	}
	if runtime.GOOS != "windows" {
		svm.Skip("the unit-test job runs these tests on Linux")
	}
	runLocalHashScanWindowsChecks(svm.t)
}

// runLocalHashScanWindowsChecks runs the traverser's hashing tests, which depend on Windows file locking, deletion and open-handle behaviour.
func runLocalHashScanWindowsChecks(t *testing.T) {
	t.Helper()
	const pattern = "^(TestLocalHashingScanDoesNotHangWhenReadsFail|TestLocalHashingScanStopsOnProcessingError|TestLocalHashingScanSkipsFilesRemovedBeforeHashing|TestLocalHashingScanClosesHashedFiles)$"
	const minimumPassed = 4

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	command := exec.CommandContext(ctx, "go", "test", "-json", "-count=1", "-timeout=4m", "-run", pattern, "./traverser")
	command.Dir = ".."
	var stdout, stderr bytes.Buffer
	command.Stdout = &stdout
	command.Stderr = &stderr
	runErr := command.Run()

	passed := 0
	decoder := json.NewDecoder(&stdout)
	for {
		var event struct {
			Action string
			Test   string
			Output string
		}
		if err := decoder.Decode(&event); err == io.EOF {
			break
		} else if err != nil {
			t.Fatalf("decode traverser test results: %v\n%s", err, stderr.String())
		}
		if event.Output != "" {
			t.Log(strings.TrimSpace(event.Output))
		}
		if event.Action == "pass" && event.Test != "" && !strings.Contains(event.Test, "/") {
			passed++
		}
	}
	if runErr != nil {
		t.Fatalf("traverser hashing tests failed: %v\n%s", runErr, stderr.String())
	}
	if passed < minimumPassed {
		t.Fatalf("expected at least %d traverser hashing tests to pass, got %d", minimumPassed, passed)
	}
}
