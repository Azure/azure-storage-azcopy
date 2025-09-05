package azcopy

import (
	"runtime"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

// GetPreserveInfoDefault returns the default value for the PreserveInfo option based on the current OS and FromTo.
func GetPreserveInfoDefault(fromTo common.FromTo) bool {
	// defaults to true for NFS-aware transfers, and SMB-aware transfers on Windows.
	return AreBothLocationsNFSAware(fromTo) ||
		(runtime.GOOS == "windows" && AreBothLocationsSMBAware(fromTo))
}

func AreBothLocationsNFSAware(fromTo common.FromTo) bool {
	// 1. Upload (Linux -> Azure File)
	// 2. Download (Azure File -> Linux)
	// 3. S2S (Azure File -> Azure File) (Works on Windows,Linux,Mac)
	if (runtime.GOOS == "linux") &&
		(fromTo == common.EFromTo.LocalFileNFS() || fromTo == common.EFromTo.FileNFSLocal()) {
		return true
	} else if fromTo == common.EFromTo.FileNFSFileNFS() {
		return true
	} else {
		return false
	}
}

func AreBothLocationsSMBAware(fromTo common.FromTo) bool {
	// 1. Upload (Windows/Linux -> Azure File)
	// 2. Download (Azure File -> Windows/Linux)
	// 3. S2S (Azure File -> Azure File)
	if (runtime.GOOS == "windows" || runtime.GOOS == "linux") &&
		(fromTo == common.EFromTo.LocalFile() || fromTo == common.EFromTo.FileLocal()) {
		return true
	} else if fromTo == common.EFromTo.FileFile() {
		return true
	} else {
		return false
	}
}
