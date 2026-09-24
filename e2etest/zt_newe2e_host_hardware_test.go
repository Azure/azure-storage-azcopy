package e2etest

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/azcopy"
)

func init() {
	suiteManager.RegisterSuite(&HostHardwareSuite{})
}

type HostHardwareSuite struct{}

func (*HostHardwareSuite) Scenario_PlatformChecks(svm *ScenarioVariationManager) {
	if svm.Dryrun() {
		return
	}
	runHostHardwarePlatformChecks(svm.t)
}

func runHostHardwarePlatformChecks(t *testing.T) {
	t.Helper()
	var pattern string
	var minimumPassed int
	switch runtime.GOOS {
	case "linux":
		pattern = "^TestHostHardwareLinux"
		minimumPassed = 7
	case "windows":
		pattern = "^TestHostHardwareWindows"
		minimumPassed = 4
	case "darwin":
		pattern = "^TestHostHardwareDarwin"
		minimumPassed = 4
	default:
		t.Skip("host hardware E2E requires Windows, Linux, or macOS")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	command := exec.CommandContext(ctx, "go", "test", "-json", "-count=1", "-timeout=2m", "-tags=hostinfoe2e", "-run", pattern, "./azcopy/internal/hostinfo")
	command.Dir = ".."
	var stdout, stderr bytes.Buffer
	command.Stdout = &stdout
	command.Stderr = &stderr
	if err := command.Run(); err != nil {
		t.Fatalf("host hardware platform checks failed: %v\n%s\n%s", err, stdout.String(), stderr.String())
	}

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
			t.Fatalf("decode host hardware test results: %v", err)
		}
		if event.Output != "" {
			t.Log(strings.TrimSpace(event.Output))
		}
		if event.Action == "pass" && event.Test != "" && !strings.Contains(event.Test, "/") {
			passed++
		}
	}
	if passed < minimumPassed {
		t.Fatalf("expected at least %d platform tests to pass, got %d", minimumPassed, passed)
	}
}

func (*HostHardwareSuite) Scenario_NativeProbes(svm *ScenarioVariationManager) {
	if svm.Dryrun() {
		return
	}

	hardware := azcopy.ProbeHostHardware()
	svm.Assert("OS version should be available on supported E2E agents", Empty{Invert: true}, hardware.OSVersion)
	svm.Assert("physical memory should be available on supported E2E agents", Equal{}, true, hardware.MemoryTotalGB > 0)
	if runtime.GOOS == "windows" || runtime.GOARCH == "amd64" || runtime.GOARCH == "386" {
		svm.Assert("CPU model should be available on supported E2E agents", Empty{Invert: true}, hardware.CPUModel)
	}

	root := svm.t.TempDir()
	svm.Assert("temporary directory should be on a local disk", Equal{}, "local-disk", azcopy.LocalMountType(root))

	switch runtime.GOOS {
	case "windows":
		svm.Assert("nonexistent path should inherit its Windows volume type", Equal{}, "local-disk", azcopy.LocalMountType(filepath.Join(root, "not-created")))
		svm.Assert("extended Windows path should retain its volume type", Equal{}, "local-disk", azcopy.LocalMountType(`\\?\`+root))
	case "linux":
		svm.Assert("nonexistent path should inherit its Linux mount type", Equal{}, "local-disk", azcopy.LocalMountType(filepath.Join(root, "not-created")))
		workingDirectory, err := os.Getwd()
		svm.NoError("get working directory", err, true)
		relative, err := filepath.Rel(workingDirectory, root)
		svm.NoError("make temporary directory relative", err, true)
		svm.Assert("relative path should resolve to its Linux mount", Equal{}, "local-disk", azcopy.LocalMountType(relative))
	case "darwin":
		svm.Assert("nonexistent macOS path should remain unknown", Empty{}, azcopy.LocalMountType(filepath.Join(root, "not-created")))
		svm.Assert("invalid macOS path should remain unknown", Empty{}, azcopy.LocalMountType(root+"\x00"))
	default:
		svm.Skip("host hardware E2E requires Windows, Linux, or macOS")
	}
}
