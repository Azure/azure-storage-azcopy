// Copyright © 2017 Microsoft <wastore@microsoft.com>
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

package cmd

import (
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"github.com/Azure/azure-storage-azcopy/common"
	tm "github.com/buger/goterm"
	"net/url"
	"os"
	"strings"
	"time"
	"github.com/Azure/azure-storage-azcopy/ste"
)

const (
	NumOfFilesPerUploadJobPart = 10000
)

type copyHandlerUtil struct{}

// checks whether a given url contains a prefix pattern
func (copyHandlerUtil) numOfStarInUrl(url string) int {
	return strings.Count(url, "*")
}

// checks if a given url points to a container, as opposed to a blob or prefix match
func (copyHandlerUtil) urlIsContainer(url *url.URL) bool {
	// if the path contains more than one "/", then it means it points to a blob, and not a container
	numOfSlashes := strings.Count(url.Path[1:], "/")

	if numOfSlashes == 0 {
		return true
	} else if numOfSlashes == 1 && url.Path[len(url.Path)-1:] == "/" { // this checks if container_name/ was given
		return true
	}
	return false
}

// append a file name to the container path to generate a blob path
func (copyHandlerUtil) generateBlobPath(destinationPath, fileName string) string {
	return fmt.Sprintf("%s/%s", destinationPath, fileName)
}

// get relative path given a root path
func (copyHandlerUtil) getRelativePath(rootPath, filePath string) string {
	// root path contains the entire absolute path to the root directory, so we need to take away everything except the root directory from filePath
	// example: rootPath = "/dir1/dir2/dir3" filePath = "/dir1/dir2/dir3/file1.txt" result = "dir3/file1.txt" scrubAway="/dir1/dir2/"

	var scrubAway string
	// test if root path finishes with a /, if yes, ignore it
	if rootPath[len(rootPath)-1:] == string(os.PathSeparator) {
		scrubAway = rootPath[:strings.LastIndex(rootPath[:len(rootPath)-1], string(os.PathSeparator))+1]
	} else {
		// +1 because we want to include the / at the end of the dir
		scrubAway = rootPath[:strings.LastIndex(rootPath, string(os.PathSeparator))+1]
	}

	result := strings.Replace(filePath, scrubAway, "", 1)

	// the back slashes need to be replaced with forward ones
	if os.PathSeparator == '\\' {
		result = strings.Replace(result, "\\", "/", -1)
	}
	return result
}

// this function can tell if a path represents a directory (must exist)
func (util copyHandlerUtil) isPathDirectory(pathString string) bool {
	// check if path exists
	destinationInfo, err := os.Stat(pathString)

	if err == nil && destinationInfo.IsDir() {
		return true
	}

	return false
}

func (util copyHandlerUtil) generateLocalPath(directoryPath, fileName string) string {
	var result string

	// check if the directory path ends with the path separator
	if strings.LastIndex(directoryPath, string(os.PathSeparator)) == len(directoryPath)-1 {
		result = fmt.Sprintf("%s%s", directoryPath, fileName)
	} else {
		result = fmt.Sprintf("%s%s%s", directoryPath, string(os.PathSeparator), fileName)
	}

	if os.PathSeparator == '\\' {
		return strings.Replace(result, "/", "\\", -1)
	}
	return result
}

func (util copyHandlerUtil) getBlobNameFromURL(path string) string {
	// return everything after the second /
	return strings.SplitAfterN(path[1:], "/", 2)[1]
}

func (util copyHandlerUtil) getContainerURLFromString(url url.URL) url.URL {
	containerName := strings.SplitAfterN(url.Path[1:], "/", 2)[0]
	url.Path = "/" + containerName
	return url
}

func (util copyHandlerUtil) generateBlobUrl(containerUrl url.URL, blobName string) string {
	containerUrl.Path = containerUrl.Path + blobName
	return containerUrl.String()
}

// for a given virtual directory, find the directory directly above the virtual file
func (util copyHandlerUtil) getLastVirtualDirectoryFromPath(path string) string {
	if path == "" {
		return ""
	}

	lastSlashIndex := strings.LastIndex(path, "/")
	if lastSlashIndex == -1 {
		return ""
	}

	return path[0:lastSlashIndex]
}

func (util copyHandlerUtil) blockIDIntToBase64(blockID int) string {
	blockIDBinaryToBase64 := func(blockID []byte) string { return base64.StdEncoding.EncodeToString(blockID) }

	binaryBlockID := (&[4]byte{})[:] // All block IDs are 4 bytes long
	binary.LittleEndian.PutUint32(binaryBlockID, uint32(blockID))
	return blockIDBinaryToBase64(binaryBlockID)
}

func (copyHandlerUtil) fetchJobStatus(jobID common.JobID, startTime time.Time) common.JobStatus {
	//lsCommand := common.ListRequest{JobID: jobID}
	var summary common.ListJobSummaryResponse
	Rpc(common.ERpcCmd.ListJobSummary(), &jobID, &summary)

	tm.Clear()
	tm.MoveCursor(1, 1)

	fmt.Println("----------------- Progress Summary for JobId ", jobID, "------------------")
	bytesInMb := float64(float64(summary.BytesOverWire) / float64(1024 * 1024))
	timeElapsed := time.Since(startTime).Seconds()
	throughPut :=  bytesInMb/ timeElapsed
	// If the time elapsed is 0, then throughput is set to 0.
	if timeElapsed == 0{
		throughPut = 0
	}
	message := fmt.Sprintf("%v Complete, throughput : %v MB/s, ( %d transfers: %d successful, %d failed, %d pending. Job ordered completely %v",
		summary.JobProgress, ste.ToFixed(throughPut, 4), summary.TotalNumberOfTransfers, summary.TotalNumberOfTransferCompleted, summary.TotalNumberOfFailedTransfer,
			summary.TotalNumberOfTransfers - (summary.TotalNumberOfTransferCompleted + summary.TotalNumberOfFailedTransfer), summary.CompleteJobOrdered)
	fmt.Println(message)
	tm.Flush()

	return summary.JobStatus
}

func startsWith(s string, t string) bool{
	return len(s) >= len(t) && strings.EqualFold(s[0:len(t)], t)
}
