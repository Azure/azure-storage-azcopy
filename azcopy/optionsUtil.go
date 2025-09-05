package azcopy

import (
	"errors"
	"fmt"
	"math"
	"runtime"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

// GetPreserveInfoDefault returns the default value for the PreserveInfo option based on the current OS and FromTo.
func GetPreserveInfoDefault(fromTo common.FromTo) bool {
	// defaults to true for NFS-aware transfers, and SMB-aware transfers on Windows.
	return areBothLocationsNFSAware(fromTo) ||
		(runtime.GOOS == "windows" && areBothLocationsSMBAware(fromTo))
}

func areBothLocationsNFSAware(fromTo common.FromTo) bool {
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

func areBothLocationsSMBAware(fromTo common.FromTo) bool {
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

func areBothLocationsPOSIXAware(fromTo common.FromTo) bool {
	// POSIX properties are stored in blob metadata-- They don't need a special persistence strategy for S2S methods.
	switch fromTo {
	case common.EFromTo.BlobLocal(), common.EFromTo.LocalBlob(), common.EFromTo.BlobFSLocal(), common.EFromTo.LocalBlobFS():
		return runtime.GOOS == "linux"
	case common.EFromTo.BlobBlob(), common.EFromTo.BlobFSBlobFS(), common.EFromTo.BlobFSBlob(), common.EFromTo.BlobBlobFS():
		return true
	default:
		return false
	}
}

// BlockSizeInBytes converts a FLOATING POINT number of MiB, to a number of bytes
// A non-nil error is returned if the conversion is not possible to do accurately (e.g. it comes out of a fractional number of bytes)
// The purpose of using floating point is to allow specialist users (e.g. those who want small block sizes to tune their read IOPS)
// to use fractions of a MiB. E.g.
// 0.25 = 256 KiB
// 0.015625 = 16 KiB
func BlockSizeInBytes(rawBlockSizeInMiB float64) (int64, error) {
	if rawBlockSizeInMiB < 0 {
		return 0, errors.New("negative block size not allowed")
	}
	rawSizeInBytes := rawBlockSizeInMiB * 1024 * 1024 // internally we use bytes, but users' convenience the command line uses MiB
	if rawSizeInBytes > math.MaxInt64 {
		return 0, errors.New("block size too big for int64")
	}
	const epsilon = 0.001 // arbitrarily using a tolerance of 1000th of a byte
	_, frac := math.Modf(rawSizeInBytes)
	isWholeNumber := frac < epsilon || frac > 1.0-epsilon // frac is very close to 0 or 1, so rawSizeInBytes is (very close to) an integer
	if !isWholeNumber {
		return 0, fmt.Errorf("while fractional numbers of MiB are allowed as the block size, the fraction must result to a whole number of bytes. %.12f MiB resolves to %.3f bytes", rawBlockSizeInMiB, rawSizeInBytes)
	}
	return int64(math.Round(rawSizeInBytes)), nil
}
