package e2etest

import (
	"os"
	"path/filepath"
	"runtime"

	"github.com/Azure/azure-storage-azcopy/v10/azcopy"
)

func init() {
	suiteManager.RegisterSuite(&HostHardwareSuite{})
}

type HostHardwareSuite struct{}

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
