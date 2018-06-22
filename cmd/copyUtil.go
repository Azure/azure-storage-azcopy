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
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/azbfs"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-azcopy/ste"
	"github.com/Azure/azure-storage-blob-go/2017-07-29/azblob"
	"github.com/Azure/azure-storage-file-go/2017-07-29/azfile"
	"path/filepath"
)

const (
	NumOfFilesPerDispatchJobPart = 10000
)

type copyHandlerUtil struct{}

// checks whether a given url contains a prefix pattern
func (copyHandlerUtil) numOfStarInUrl(url string) int {
	return strings.Count(url, "*")
}

// checks if a given url points to a container, as opposed to a blob or prefix match
func (copyHandlerUtil) urlIsContainerOrShare(url *url.URL) bool {
	// if the path contains more than one "/", then it means it points to a blob, and not a container
	numOfSlashes := strings.Count(url.Path[1:], "/")

	if numOfSlashes == 0 {
		return true
	} else if numOfSlashes == 1 && strings.HasSuffix(url.Path, "/") { // this checks if container_name/ was given
		return true
	}
	return false
}

// redactSigQueryParam checks for the signature in the given rawquery part of the url
// If the signature exists, it replaces the value of the signature with "REDACTED"
// This api is used when SAS is written to log file to avoid exposing the user given SAS
func (util copyHandlerUtil) redactSigQueryParam(rawQuery string) (bool, string) {
	rawQuery = strings.ToLower(rawQuery) // lowercase the string so we can look for ?sig= and &sig=
	sigFound := strings.Contains(rawQuery, "?sig=")
	if !sigFound {
		sigFound = strings.Contains(rawQuery, "&sig=")
		if !sigFound {
			return sigFound, rawQuery // [?|&]sig= not found; return same rawQuery passed in (no memory allocation)
		}
	}
	// [?|&]sig= found, redact its value
	values, _ := url.ParseQuery(rawQuery)
	for name := range values {
		if strings.EqualFold(name, "sig") {
			values[name] = []string{"REDACTED"}
		}
	}
	return sigFound, values.Encode()
}

// ConstructCommandStringFromArgs creates the user given commandString from the os Arguments
// If any argument passed is an http Url and contains the signature, then the signature is redacted
func (util copyHandlerUtil) ConstructCommandStringFromArgs() string{
	// Get the os Args and strip away the first argument since it will be the path of Azcopy executable
	args := os.Args[1:]
	if len(args) == 0 {
		return ""
	}
	s := strings.Builder{}
	for _, arg := range args {
		// If the argument starts with https, it is either the remote source or remote destination
		// If there exists a signature in the argument string it needs to be redacted
		if startsWith(arg, "https") {
			// parse the url
			argUrl, err := url.Parse(arg)
			// If there is an error parsing the url, then throw the error
			if err != nil {
				panic(fmt.Errorf("error parsing the url %s. Failed with error %s", argUrl.String(), err.Error()))
			}
			// Check for the signature query parameter
			 _, rawQuery  := util.redactSigQueryParam(argUrl.RawQuery)
			argUrl.RawQuery = rawQuery
			s.WriteString(argUrl.String())
		}else {
			s.WriteString(arg)
		}
		s.WriteString(" ")
	}
	return s.String()
}

func (util copyHandlerUtil) sharedKeyCreds() *azbfs.SharedKeyCredential {
	name := os.Getenv("ACCOUNT_NAME")
	key := os.Getenv("ACCOUNT_KEY")
	// If the ACCOUNT_NAME and ACCOUNT_KEY are not set in environment variables
	if name == "" || key == "" {
		panic("ACCOUNT_NAME and ACCOUNT_KEY environment vars must be set before creating the blobfs pipeline")
	}
	return azbfs.NewSharedKeyCredential(name, key)
}
func (util copyHandlerUtil) urlIsDFSFileSystemOrDirectory(ctx context.Context, url *url.URL) bool {
	if util.urlIsContainerOrShare(url) {
		return true
	}
	c := util.sharedKeyCreds()
	// Need to get the resource properties and verify if it is a file or directory
	p := azbfs.NewPipeline(c, azbfs.PipelineOptions{})
	dirUrl := azbfs.NewDirectoryURL(*url, p)
	return dirUrl.IsDirectory(context.Background())
}

func (util copyHandlerUtil) urlIsAzureFileDirectory(ctx context.Context, url *url.URL) bool {
	// Azure file share case
	if util.urlIsContainerOrShare(url) {
		return true
	}

	// Need make request to ensure if it's directory
	p := azfile.NewPipeline(azfile.NewAnonymousCredential(), azfile.PipelineOptions{})
	directoryURL := azfile.NewDirectoryURL(*url, p)
	_, err := directoryURL.GetProperties(ctx)
	if err != nil {
		return false
	}

	return true
}

// append a file name to the container path to generate a blob path
func (copyHandlerUtil) generateObjectPath(destinationPath, fileName string) string {
	if strings.LastIndex(destinationPath, "/") == len(destinationPath)-1 {
		return fmt.Sprintf("%s%s", destinationPath, fileName)
	}
	return fmt.Sprintf("%s/%s", destinationPath, fileName)
}

// resourceShouldBeExcluded decides whether the file at given filePath should be excluded from the transfer or not.
// First, checks whether filePath exists in the Map or not.
// Then iterates through each entry of the map and check whether the given filePath matches the expression of any
// entry of the map.
func (util copyHandlerUtil) resourceShouldBeExcluded(excludedFilePathMap map[string]int, filePath string) bool {
	// Check if the given filePath exists as an entry in the map
	_, ok := excludedFilePathMap[filePath]
	if ok {
		return true
	}
	// Iterate through each entry of the Map
	// Matches the given filePath against map entry pattern
	// This is to handle case when user passed a sub-dir inside
	// source to exclude. All the files inside that sub-directory
	// should be excluded.
	// For Example: src = C:\User\user-1 exclude = "dir1"
	// Entry in Map = C:\User\user-1\dir1\* will match the filePath C:\User\user-1\dir1\file1.txt
	for key, _ := range excludedFilePathMap {
		matched, err := filepath.Match(key, filePath)
		if err != nil {
			panic(err)
		}
		if matched {
			return true
		}
	}
	return false
}

// relativePathToRoot returns the path of filePath relative to root
// For Example: root = /a1/a2/ filePath = /a1/a2/f1.txt
// relativePath = `f1.txt
// For Example: root = /a1 filePath =/a1/a2/f1.txt
// relativePath = a2/f1.txt
func (util copyHandlerUtil) relativePathToRoot(rootPath, filePath string, pathSep byte) string {
	if len(rootPath) == 0 {
		return filePath
	}
	result := strings.Replace(filePath, rootPath, "", 1)
	if len(result) > 0 && result[0] == pathSep {
		result = result[1:]
	}
	return result
}

// get relative path given a root path
func (copyHandlerUtil) getRelativePath(rootPath, filePath string, pathSep string) string {
	// root path contains the entire absolute path to the root directory, so we need to take away everything except the root directory from filePath
	// example: rootPath = "/dir1/dir2/dir3" filePath = "/dir1/dir2/dir3/file1.txt" result = "dir3/file1.txt" scrubAway="/dir1/dir2/"
	if len(rootPath) == 0 {
		return filePath
	}

	var scrubAway string
	// test if root path finishes with a /, if yes, ignore it
	if rootPath[len(rootPath)-1:] == pathSep {
		scrubAway = rootPath[:strings.LastIndex(rootPath[:len(rootPath)-1], pathSep)+1]
	} else {
		// +1 because we want to include the / at the end of the dir
		scrubAway = rootPath[:strings.LastIndex(rootPath, pathSep)+1]
	}

	result := strings.Replace(filePath, scrubAway, "", 1)

	// the back slashes need to be replaced with forward ones
	if os.PathSeparator == '\\' {
		result = strings.Replace(result, "\\", "/", -1)
	}
	return result
}

// this function can tell if a path represents a directory (must exist)
func (util copyHandlerUtil) isPathALocalDirectory(pathString string) bool {
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

	// blob name has "/" as Path Separator.
	// To preserve the path in blob name on local disk, replace "/" with OS Path Separator
	// For Example blob name = "blob-1/blob-2/blob-2" will be "blob-1\\blob-2\\blob-3" for windows
	return strings.Replace(result, "/", string(os.PathSeparator), -1)
}

func (util copyHandlerUtil) getBlobNameFromURL(path string) string {
	// return everything after the second /
	return strings.SplitAfterN(path[1:], "/", 2)[1]
}

func (util copyHandlerUtil) getDirNameFromSource(path string) (sourcePathWithoutPrefix, searchPrefix string) {
	if path[len(path)-1:] == string(os.PathSeparator) {
		sourcePathWithoutPrefix = path[:strings.LastIndex(path[:len(path)-1], string(os.PathSeparator))+1]
		searchPrefix = path[strings.LastIndex(path[:len(path)-1], string(os.PathSeparator))+1:]
	} else {
		// +1 because we want to include the / at the end of the dir
		sourcePathWithoutPrefix = path[:strings.LastIndex(path, string(os.PathSeparator))+1]
		searchPrefix = path[strings.LastIndex(path, string(os.PathSeparator))+1:]
	}
	return
}

func (util copyHandlerUtil) firstIndexOfWildCard(name string) int {
	return strings.Index(name, "*")
}
func (util copyHandlerUtil) getContainerURLFromString(url url.URL) url.URL {
	blobParts := azblob.NewBlobURLParts(url)
	blobParts.BlobName = ""
	return blobParts.URL()
	//containerName := strings.SplitAfterN(url.Path[1:], "/", 2)[0]
	//url.Path = "/" + containerName
	//return url
}

func (util copyHandlerUtil) getContainerUrl(blobParts azblob.BlobURLParts) url.URL {
	blobParts.BlobName = ""
	return blobParts.URL()
}

func (util copyHandlerUtil) blobNameFromUrl(blobParts azblob.BlobURLParts) string {
	return blobParts.BlobName
}

func (util copyHandlerUtil) createBlobUrlFromContainer(blobUrlParts azblob.BlobURLParts, blobName string) string {
	blobUrlParts.BlobName = blobName
	blobUrl := blobUrlParts.URL()
	return blobUrl.String()
}

func (util copyHandlerUtil) appendBlobNameToUrl(blobUrlParts azblob.BlobURLParts, blobName string) (url.URL, string) {
	if os.PathSeparator == '\\' {
		blobName = strings.Replace(blobName, string(os.PathSeparator), "/", -1)
	}
	if blobUrlParts.BlobName == "" {
		blobUrlParts.BlobName = blobName
	} else {
		if blobUrlParts.BlobName[len(blobUrlParts.BlobName)-1] == '/' {
			blobUrlParts.BlobName += blobName
		} else {
			blobUrlParts.BlobName += "/" + blobName
		}
	}
	return blobUrlParts.URL(), blobUrlParts.BlobName
}

// sourceRootPathWithoutWildCards returns the directory from path that does not have wildCards
// returns the patterns that defines pattern for relativePath of files to the above mentioned directory
// For Example: src = C:\User\a*\a1*\*.txt rootDir = C:\User\ pattern = a*\a1*\*.txt
func (util copyHandlerUtil) sourceRootPathWithoutWildCards(path string, pathSep byte) (string, string) {
	if len(path) == 0 {
		return path, "*"
	}
	// if no wild card exists, then root directory is the given directory
	// pattern is '*' i.e to include all the files inside the given path
	wIndex := util.firstIndexOfWildCard(path)
	if wIndex == -1 {
		return path, "*"
	}
	pathWithoutWildcard := path[:wIndex]
	// find the last separator in path without the wildCards
	// result will be content of path till the above separator
	// for Example: src = C:\User\a*\a1*\*.txt pathWithoutWildcard = C:\User\a
	// sepIndex = 7
	// rootDirectory = C:\User and pattern = a*\a1*\*.txt
	sepIndex := strings.LastIndex(pathWithoutWildcard, string(pathSep))
	if sepIndex == -1 {
		return "", path
	}
	return pathWithoutWildcard[:sepIndex], path[sepIndex+1:]
}

func (util copyHandlerUtil) blobNameMatchesThePattern(patternString string, blobName string) bool {
	//// Since filePath.Match matches "*" with any sequence of non-separator characters
	//// it will return false when "*" matched with "a/b" on linux or "a\\b" on windows
	//// Hence hard-coded check added for "*"
	//if pattern == "*" {
	//	return true
	//}
	//// BlobName has "/" as path separators
	//// filePath.Match matches "*" with any sequence of non-separator characters
	//// since path separator on linux and blobName is same
	//// Replace "/" with its url encoded value "%2F"
	//// This is to handle cases like matching "dir* and dir/a.txt"
	//// or matching "dir/* and dir/a/b.txt"
	//if os.PathSeparator == '/' {
	//	pattern = strings.Replace(pattern, "/", "%2F", -1)
	//	blobName = strings.Replace(blobName, "/", "%2F", -1)
	//}
	//
	//matched, err := filepath.Match(pattern, blobName)
	//if err != nil {
	//	panic(err)
	//}
	//return matched
	str := []rune(blobName)
	pattern := []rune(patternString)
	s := 0 // counter for str index
	p := 0 // counter for pattern index
	startIndex := -1
	match := 0
	for s < len(str) {
		// advancing both pointers
		if p < len(pattern) && str[s] == pattern[p] {
			s++
			p++
		} else if p < len(pattern) && pattern[p] == '*' {
			// * found, only advancing pattern pointer
			startIndex = p
			match = s
			p++
		} else if startIndex != -1 {
			p = startIndex + 1
			match++
			s = match
		} else {
			//current pattern pointer is not star, last patter pointer was not *
			//characters do not match
			return false
		}
	}
	//check for remaining characters in pattern
	for p < len(pattern) && pattern[p] == '*' {
		p++
	}

	return p == len(pattern)
}

func (util copyHandlerUtil) searchPrefixFromUrl(parts azblob.BlobURLParts) (prefix, pattern string) {
	// If the blobName is empty, it means  the url provided is of a container,
	// then all blobs inside containers needs to be included, so pattern is set to *
	if parts.BlobName == "" {
		pattern = "*"
		return
	}
	// Check for wildcards and get the index of first wildcard
	// If the wild card does not exists, then index returned is -1
	wildCardIndex := util.firstIndexOfWildCard(parts.BlobName)
	if wildCardIndex < 0 {
		// If no wild card exits and url represents a virtual directory
		// prefix is the path of virtual directory after the container.
		// Example: https://<container-name>/vd-1?<signature>, prefix = /vd-1
		// Example: https://<container-name>/vd-1/vd-2?<signature>, prefix = /vd-1/vd-2
		prefix = parts.BlobName
		// check for separator at the end of virtual directory
		if prefix[len(prefix)-1] != '/' {
			prefix += "/"
		}
		// since the url is a virtual directory, then all blobs inside the virtual directory
		// needs to be downloaded, so the pattern is "*"
		// pattern being "*", all blobNames when matched with "*" will be true
		// so all blobs inside the virtual dir will be included
		pattern = "*"
		return
	}
	// wild card exists prefix will be the content of blob name till the wildcard index
	// Example: https://<container-name>/vd-1/vd-2/abc*
	// prefix = /vd-1/vd-2/abc and pattern = /vd-1/vd-2/abc*
	// All the blob inside the container in virtual dir vd-2 that have the prefix "abc"
	prefix = parts.BlobName[:wildCardIndex]
	pattern = parts.BlobName
	return
}

func (util copyHandlerUtil) getConatinerUrlAndSuffix(url url.URL) (containerUrl, suffix string) {
	s := strings.SplitAfterN(url.Path[1:], "/", 2)
	containerUrl = "/" + s[0]
	suffix = s[1]
	if strings.LastIndex(suffix, "/") == len(suffix)-1 {
		// if there is a path separator at the end, then remove the path separator.
		suffix = suffix[:len(suffix)-1]
	}
	return
}

func (util copyHandlerUtil) generateBlobUrl(containerUrl url.URL, blobName string) string {
	if containerUrl.Path[len(containerUrl.Path)-1] != '/' {
		containerUrl.Path = containerUrl.Path + "/" + blobName
	} else {
		containerUrl.Path = containerUrl.Path + blobName
	}
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

	return path[0 : lastSlashIndex+1]
}

func (util copyHandlerUtil) blockIDIntToBase64(blockID int) string {
	blockIDBinaryToBase64 := func(blockID []byte) string { return base64.StdEncoding.EncodeToString(blockID) }

	binaryBlockID := (&[4]byte{})[:] // All block IDs are 4 bytes long
	binary.LittleEndian.PutUint32(binaryBlockID, uint32(blockID))
	return blockIDBinaryToBase64(binaryBlockID)
}

// containsSpecialChars checks for the special characters in the given name.
// " \\ < > * | ? : are not allowed while creating file / dir by the OS.
// space is also included as a special character since space at the end of name of file / dir
// is not considered.
// For example "abcd " is same as "abcd"
func (util copyHandlerUtil) containsSpecialChars(name string) bool {
	for _, r := range name {
		if r == '"' || r == '\\' || r == '<' ||
			r == '>' || r == '|' || r == '*' ||
			r == '?' || r == ':' {
			return true
		}
	}
	// if the last character in the file / dir name is ' '
	// then it not accepted by OS.
	// 'test1 ' is created as 'test1'
	if len(name) > 0 && name[len(name)-1] == ' ' {
		return true
	}
	return false
}

// blobPathWOSpecialCharacters checks the special character in the given blob path.
// If the special characters exists, then it encodes the path so that blob can created
// locally.
// Some special characters are not allowed while creating file / dir by OS
// returns the path without special characters.
func (util copyHandlerUtil) blobPathWOSpecialCharacters(blobPath string) string {
	// split the path by separator "/"
	parts := strings.Split(blobPath, "/")
	bnwc := ""
	// iterates through each part of the path.
	// for example if given path is /a/b/c/d/e.txt,
	// then check for special character in each part a,b,c,d and e.txt
	for i := range parts {
		if len(parts[i]) == 0 {
			// If the part length is 0, then encode the "/" char and add to the new path.
			// This is for scenarios when there exists "/" at the end of blob or start of the blobName.
			bnwc += url.QueryEscape("/") + "/"
		} else if util.containsSpecialChars(parts[i]) {
			// if the special character exists, then perform the encoding.
			bnwc += url.QueryEscape(parts[i]) + "/"
		} else {
			// If there is no special character, then add the part as it is.
			bnwc += parts[i] + "/"
		}
	}
	// remove "/" at the end of blob path.
	bnwc = bnwc[:len(bnwc)-1]
	return bnwc
}

// doesBlobRepresentAFolder verifies whether blob is valid or not.
// Used to handle special scenarios or conditions.
func (util copyHandlerUtil) doesBlobRepresentAFolder(bInfo azblob.Blob) bool {
	// this condition is to handle the WASB V1 directory structure.
	// HDFS driver creates a blob for the empty directories (let’s call it ‘myfolder’)
	// and names all the blobs under ‘myfolder’ as such: ‘myfolder/myblob’
	// The empty directory has meta-data 'hdi_isfolder = true'
	return bInfo.Metadata["hdi_isfolder"] == "true"
}

// PrintFinalJobProgressSummary prints the final progress summary of the Job after job is either completed or cancelled.
func (util copyHandlerUtil) PrintFinalJobProgressSummary(summary common.ListJobSummaryResponse, duration time.Duration){
	// added an empty line to provide gap between the last Job progress status and final job summary
	fmt.Println("")
	fmt.Println(fmt.Sprintf("Job %s summary ", summary.JobID.String()))
	fmt.Println("Elapsed Time (Minutes)", ste.ToFixed(duration.Minutes(), 4))
	fmt.Println("Total Number Of Transfers ", summary.TotalTransfers)
	fmt.Println("Number of Transfers Completed ", summary.TransfersCompleted)
	fmt.Println("Number of Transfers Failed ", summary.TransfersFailed)
	fmt.Println("Final Job Status ", summary.JobStatus)
}

func (copyHandlerUtil) fetchJobStatus(jobID common.JobID, startTime *time.Time, bytesTransferredInLastInterval *uint64, outputJson bool) common.ListJobSummaryResponse {
	var scanningString = ""
	//lsCommand := common.ListRequest{JobID: jobID}
	var summary common.ListJobSummaryResponse
	Rpc(common.ERpcCmd.ListJobSummary(), &jobID, &summary)
	if !summary.CompleteJobOrdered {
		scanningString = "(Scanning ...)"
	}else{
		scanningString = ""
	}
	if outputJson {
		jsonOutput, err := json.MarshalIndent(summary, "", "  ")
		if err != nil {
			panic(err)
		}
		fmt.Println(string(jsonOutput))
	} else {
		bytesInMb := float64(float64(summary.BytesOverWire-*bytesTransferredInLastInterval) / float64(1024*1024))
		timeElapsed := time.Since(*startTime).Seconds()
		*startTime = time.Now()
		*bytesTransferredInLastInterval = summary.BytesOverWire
		throughPut := common.Ifffloat64(timeElapsed != 0, bytesInMb/timeElapsed, 0)
		// TODO: add active connections in the Job Summary for debugging purpose. remove later
		fmt.Printf("\r %v Active Connections %v Complete, %v Failed, %v Pending, %v Total, %s 2-sec throughput: %v MB/s", summary.ActiveConnections,
			summary.TransfersCompleted, summary.TransfersFailed, summary.TotalTransfers-(summary.TransfersCompleted+summary.TransfersFailed),
			summary.TotalTransfers, scanningString, ste.ToFixed(throughPut, 4))
		//fmt.Printf("\r %v Complete, JobStatus %s , throughput : %v MB/s, ( %d transfers: %d successful, %d failed, %d pending. Job ordered completely %v)",
		//	summary.JobProgressPercentage, summary.JobStatus, ste.ToFixed(throughPut, 4), summary.TotalTransfers, summary.TransfersCompleted, summary.TransfersFailed,
		//	summary.TotalTransfers-(summary.TransfersCompleted+summary.TransfersFailed), summary.CompleteJobOrdered)
	}
	return summary
}

func startsWith(s string, t string) bool {
	return len(s) >= len(t) && strings.EqualFold(s[0:len(t)], t)
}

func endWithSlashOrBackSlash(path string) bool {
	return strings.HasSuffix(path, "/") || strings.HasSuffix(path, "\\")
}

// getPossibleFileNameFromURL return the possible file name get from URL.
func (util copyHandlerUtil) getPossibleFileNameFromURL(path string) string {
	if path == "" {
		panic("can not get file name from an empty path")
	}

	if endWithSlashOrBackSlash(path) {
		return ""
	}

	return path[strings.LastIndex(path, "/")+1:]
}

// getDeepestDirOrFileURLFromString returns the deepest valid DirectoryURL or FileURL can be picked out from the provided URL.
// When provided URL is endwith *, get parent directory of file whose name is with *.
// When provided URL without *, the url could be a file or a directory, in this case make request to get valid DirectoryURL or FileURL.
func (util copyHandlerUtil) getDeepestDirOrFileURLFromString(ctx context.Context, givenURL url.URL, p pipeline.Pipeline) (*azfile.DirectoryURL, *azfile.FileURL, *azfile.FileGetPropertiesResponse, bool) {
	url := givenURL
	path := url.Path

	buffer := bytes.Buffer{}
	defer buffer.Reset() // Ensure to free the buffer.

	if strings.HasSuffix(path, "*") {
		lastSlashIndex := strings.LastIndex(path, "/")
		url.Path = url.Path[:lastSlashIndex]
	} else {
		if !strings.HasSuffix(path, "/") {
			// Could be a file or a directory, try to see if file exists
			fileURL := azfile.NewFileURL(url, p)

			if gResp, err := fileURL.GetProperties(ctx); err == nil {
				return nil, &fileURL, gResp, true
			} else {
				fmt.Fprintf(&buffer, "Fail to parse %v as a file for error %v, given URL: %s\n", url, err, givenURL.String())
			}
		}
	}
	dirURL := azfile.NewDirectoryURL(url, p)
	if _, err := dirURL.GetProperties(ctx); err == nil {
		return &dirURL, nil, nil, true
	} else {
		fmt.Fprintf(&buffer, "Fail to parse %v as a directory for error %v, given URL: %s\n", url, err, givenURL.String())
	}

	// Log the error if the given URL is neither an existing Azure file directory nor an existing Azure file.
	fmt.Print(buffer.String())

	return nil, nil, nil, false
}

// isDirectoryStartExpression verifies if an url is like directory/* or share/* which equals to a directory or share.
// If it could be transferred to a directory, return the URL which directly express directory.
func (util copyHandlerUtil) hasEquivalentDirectoryURL(url url.URL) (isDirectoryStartExpression bool, equivalentURL url.URL) {
	if strings.HasSuffix(url.Path, "/*") {
		url.Path = url.Path[:len(url.Path)-1]
		isDirectoryStartExpression = true
	}
	equivalentURL = url
	return
}

// reactURLQuery reacts the query part of URL.
func (util copyHandlerUtil) reactURLQuery(url url.URL) url.URL {
	// Note: this is copy by value
	url.RawQuery = "<Reacted Query>"
	return url
}

// replaceBackSlashWithSlash replaces all backslash '\' with slash '/' in a given URL string.
func (util copyHandlerUtil) replaceBackSlashWithSlash(urlStr string) string {
	str := strings.Replace(urlStr, "\\", "/", -1)

	return str
}
