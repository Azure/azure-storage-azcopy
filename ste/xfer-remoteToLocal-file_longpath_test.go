// Copyright © Microsoft <wastore@microsoft.com>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package ste

import (
	"os"
	"runtime"
	"strings"
	"testing"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/stretchr/testify/assert"
)

func TestTransferInfo_getDownloadPath_LongPath(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("Long path test is Windows-specific")
	}

	// Create a long destination path that would exceed 260 characters
	longFileName := strings.Repeat("a", 200) + ".txt"
	longPath := "C:\\very\\long\\directory\\structure\\with\\many\\subdirectories\\that\\will\\exceed\\windows\\path\\limit\\" + longFileName
	
	// Ensure we're testing with an extended path (starting with \\?\)
	extendedDestination := common.ToExtendedPath(longPath)
	assert.True(t, strings.HasPrefix(extendedDestination, common.EXTENDED_PATH_PREFIX), "Destination should use extended path format")

	info := TransferInfo{
		Destination: extendedDestination,
		JobID:       common.NewJobID(),
		SourceSize:  1024, // Non-zero to trigger temp path logic
	}

	// Ensure the environment variable is set to download to temp path (default behavior)
	os.Setenv("AZCOPY_DOWNLOAD_TO_TEMP_PATH", "true")
	defer os.Unsetenv("AZCOPY_DOWNLOAD_TO_TEMP_PATH")

	downloadPath := info.getDownloadPath()

	// Verify the download path uses extended format for long paths
	assert.True(t, strings.HasPrefix(downloadPath, common.EXTENDED_PATH_PREFIX), 
		"Download path should use extended path format on Windows for long paths")

	// Verify it contains the temp download prefix
	assert.Contains(t, downloadPath, ".azDownload-", "Download path should contain temp prefix")
	
	// Verify the temp path is different from the final destination
	assert.NotEqual(t, downloadPath, extendedDestination, "Temp download path should be different from destination")
}

func TestTransferInfo_getDownloadPath_ShortPath(t *testing.T) {
	// Test that short paths work correctly (no change to existing behavior)
	shortPath := "/tmp/short.txt"
	if runtime.GOOS == "windows" {
		shortPath = "C:\\temp\\short.txt"
	}

	info := TransferInfo{
		Destination: shortPath,
		JobID:       common.NewJobID(),
		SourceSize:  1024,
	}

	os.Setenv("AZCOPY_DOWNLOAD_TO_TEMP_PATH", "true")
	defer os.Unsetenv("AZCOPY_DOWNLOAD_TO_TEMP_PATH")

	downloadPath := info.getDownloadPath()

	// On non-Windows, should remain unchanged
	// On Windows, short paths may still get ToExtendedPath treatment but that's fine
	if runtime.GOOS != "windows" {
		assert.Contains(t, downloadPath, ".azDownload-", "Download path should contain temp prefix")
		assert.NotEqual(t, downloadPath, shortPath, "Temp download path should be different from destination")
	}
}

func TestTransferInfo_getDownloadPath_DirectPath(t *testing.T) {
	// Test when AZCOPY_DOWNLOAD_TO_TEMP_PATH=false, should return destination directly
	info := TransferInfo{
		Destination: "C:\\direct\\path\\file.txt",
		JobID:       common.NewJobID(),
		SourceSize:  1024,
	}

	os.Setenv("AZCOPY_DOWNLOAD_TO_TEMP_PATH", "false")
	defer os.Unsetenv("AZCOPY_DOWNLOAD_TO_TEMP_PATH")

	downloadPath := info.getDownloadPath()

	assert.Equal(t, info.Destination, downloadPath, "When temp download is disabled, should return destination directly")
}

func TestTransferInfo_getDownloadPath_ZeroSizeFile(t *testing.T) {
	// Test that zero-size files skip temp path logic
	info := TransferInfo{
		Destination: "C:\\test\\empty.txt",
		JobID:       common.NewJobID(),
		SourceSize:  0, // Zero size
	}

	os.Setenv("AZCOPY_DOWNLOAD_TO_TEMP_PATH", "true")
	defer os.Unsetenv("AZCOPY_DOWNLOAD_TO_TEMP_PATH")

	downloadPath := info.getDownloadPath()

	assert.Equal(t, info.Destination, downloadPath, "Zero-size files should not use temp path")
}