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
	"context"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"net"
	"net/url"
	"os"
	"strings"

	"path/filepath"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/azbfs"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-blob-go/2018-03-28/azblob"
	"github.com/Azure/azure-storage-file-go/2017-07-29/azfile"
)

const (
	NumOfFilesPerDispatchJobPart = 10000
)

type copyHandlerUtil struct{}

// TODO: Need be replaced with anonymous embedded field technique.
var gCopyUtil = copyHandlerUtil{}

const wildCard = "*"

// checks whether a given url contains a prefix pattern
func (copyHandlerUtil) numOfWildcardInURL(url url.URL) int {
	return strings.Count(url.String(), wildCard)
}

// isIPEndpointStyle checkes if URL's host is IP, in this case the storage account endpoint will be composed as:
// http(s)://IP(:port)/storageaccount/share(||container||etc)/...
// TODO: Remove this, it can be replaced by SDK's native support for IP endpoint style.
func (util copyHandlerUtil) isIPEndpointStyle(url url.URL) bool {
	return net.ParseIP(url.Host) != nil
}

// checks if a given url points to a container, as opposed to a blob or prefix match
func (util copyHandlerUtil) urlIsContainerOrShare(url *url.URL) bool {
	// When it's IP endpoint style, if the path contains more than two "/", then it means it points to a blob, and not a container.
	// When it's not IP endpoint style, if the path contains more than one "/", then it means it points to a blob, and not a container.
	numOfSlashes := strings.Count(url.Path[1:], "/")
	isIPEndpointStyle := util.isIPEndpointStyle(*url)

	if (!isIPEndpointStyle && numOfSlashes == 0) || (isIPEndpointStyle && numOfSlashes == 1) {
		return true
	} else if ((!isIPEndpointStyle && numOfSlashes == 1) || (isIPEndpointStyle && numOfSlashes == 2)) && strings.HasSuffix(url.Path, "/") { // this checks if container_name/ was given
		return true
	}
	return false
}

func (util copyHandlerUtil) appendQueryParamToUrl(url *url.URL, queryParam string) *url.URL {
	if len(url.RawQuery) > 0 {
		url.RawQuery += "&" + queryParam
	} else {
		url.RawQuery = queryParam
	}
	return url
}

// redactSigQueryParam checks for the signature in the given rawquery part of the url
// If the signature exists, it replaces the value of the signature with "REDACTED"
// This api is used when SAS is written to log file to avoid exposing the user given SAS
// TODO: remove this, redactSigQueryParam could be added in SDK
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
func (util copyHandlerUtil) ConstructCommandStringFromArgs() string {
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
			_, rawQuery := util.redactSigQueryParam(argUrl.RawQuery)
			argUrl.RawQuery = rawQuery
			s.WriteString(argUrl.String())
		} else {
			s.WriteString(arg)
		}
		s.WriteString(" ")
	}
	return s.String()
}

func (util copyHandlerUtil) urlIsBFSFileSystemOrDirectory(ctx context.Context, url *url.URL, p pipeline.Pipeline) bool {
	if util.urlIsContainerOrShare(url) {
		return true
	}
	// Need to get the resource properties and verify if it is a file or directory
	dirURL := azbfs.NewDirectoryURL(*url, p)
	return dirURL.IsDirectory(context.Background())
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

// resourceShouldBeIncluded decides whether the file at given path should be included or not
// If no files are explicitly mentioned with the include flag, then given file will be included.
// If there are files mentioned with the include flag, then given file will be matched first
// to decide to keep the file or not
func (util copyHandlerUtil) resourceShouldBeIncluded(parentSourcePath string, includeFileMap map[string]int, filePath string) bool {

	// If no files have been mentioned explicitly with the include flag
	// then file at given filePath will be included
	if len(includeFileMap) == 0 {
		return true
	}

	// strip the parent source path from the file path to match against the
	//relative path mentioned in the exclude flag
	fileRelativePath := strings.Replace(filePath, parentSourcePath, "", 1)
	if fileRelativePath[0] == common.AZCOPY_PATH_SEPARATOR_CHAR {
		fileRelativePath = fileRelativePath[1:]
	}

	// Check if the given filePath exists as an entry in the map
	_, ok := includeFileMap[fileRelativePath]
	if ok {
		return true
	}
	// Iterate through each entry of the Map
	// Matches the given filePath against map entry pattern
	// This is to handle case when user passed a sub-dir inside
	// source to exclude. All the files inside that sub-directory
	// should be excluded.
	// For Example: source = C:\User\user-1 exclude = "dir1"
	// Entry in Map = C:\User\user-1\dir1\* will match the filePath C:\User\user-1\dir1\file1.txt
	for key, _ := range includeFileMap {
		if util.blobNameMatchesThePattern(key, fileRelativePath) {
			return true
		}
	}
	return false
}

// resourceShouldBeExcluded decides whether the file at given filePath should be excluded from the transfer or not.
// First, checks whether filePath exists in the Map or not.
// Then iterates through each entry of the map and check whether the given filePath matches the expression of any
// entry of the map.
func (util copyHandlerUtil) resourceShouldBeExcluded(parentSourcePath string, excludedFilePathMap map[string]int, filePath string) bool {

	// strip the parent source path from the file path to match against the
	//relative path mentioned in the exclude flag
	fileRelativePath := strings.Replace(filePath, parentSourcePath, "", 1)
	if fileRelativePath[0] == common.AZCOPY_PATH_SEPARATOR_CHAR {
		fileRelativePath = fileRelativePath[1:]
	}
	// Check if the given filePath exists as an entry in the map
	_, ok := excludedFilePathMap[fileRelativePath]
	if ok {
		return true
	}
	// Iterate through each entry of the Map
	// Matches the given filePath against map entry pattern
	// This is to handle case when user passed a sub-dir inside
	// source to exclude. All the files inside that sub-directory
	// should be excluded.
	// For Example: source = C:\User\user-1 exclude = "dir1"
	// Entry in Map = C:\User\user-1\dir1\* will match the filePath C:\User\user-1\dir1\file1.txt
	for key, _ := range excludedFilePathMap {
		if util.blobNameMatchesThePattern(key, fileRelativePath) {
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

// getSymlinkTransferList api scans all the elements inside the symlinkPath and enumerates the transfers.
// If there exists a symlink in the given symlinkPath, it recursively scans it and enumerate the transfer.
// The path of the files in the symlinkPath will be relative to the original path.
// Example 1: C:\MountedD is a symlink to D: and D: contains file1, file2.
// The destination for file1, file2 remotely will be MountedD/file1, MountedD/file2.
// Example 2. If there exists a symlink inside the D: "D:\MountecF" pointing to F: and there exists
// ffile1, ffile2, then destination for ffile1, ffile2 remotely will be MountedD/MountedF/ffile1 and
// MountedD/MountedF/ffile2
func (util copyHandlerUtil) getSymlinkTransferList(symlinkPath, source, parentSource, cleanContainerPath string,
	destinationUrl *url.URL, include, exclude map[string]int) ([]common.CopyTransfer, []error) {
	// replace the "\\" path separator with "/" separator
	symlinkPath = strings.Replace(symlinkPath, common.OS_PATH_SEPARATOR, common.AZCOPY_PATH_SEPARATOR_STRING, -1)
	// maintains the list of transfers that are added while traversing the symlink path
	var transferList = []common.CopyTransfer{}
	// maintains the list of errors occurred while traversing the symlink path
	var errorList = []error{}
	listOfFilesDirs, err := filepath.Glob(symlinkPath)
	if err != nil {
		return []common.CopyTransfer{}, []error{fmt.Errorf(fmt.Sprintf("found cycle in symlink path %s", symlinkPath))}
	}
	for _, files := range listOfFilesDirs {
		// replace the windows path separator in the path with "/" path separator
		files = strings.Replace(files, common.OS_PATH_SEPARATOR, common.AZCOPY_PATH_SEPARATOR_STRING, -1)
		fInfo, err := os.Stat(files)
		if err != nil {
			errorList = append(errorList, err)
		} else if fInfo.IsDir() {
			filepath.Walk(files, func(path string, fileInfo os.FileInfo, err error) error {
				if err != nil {
					errorList = append(errorList, err)
					return nil
				} else if fileInfo.IsDir() {
					return nil
				} else if fileInfo.Mode().IsRegular() { // If the file is a regular file i.e not a directory and symlink.
					// replace the windows path separator in the path with "/" path separator
					path = strings.Replace(path, common.OS_PATH_SEPARATOR, common.AZCOPY_PATH_SEPARATOR_STRING, -1)
					// strip the original symlink path from the filePath
					// For Example: C:\MountedD points to D:\ and path is D:\file1
					// relativePath = file1
					relativePath := strings.Replace(path, symlinkPath, "", 1)
					var sourcePath = ""
					// concatenate the relative symlink path to the original source path
					// For Example: C:\MountedD points to D:\ and path is D:\file1
					// sourcePath = c:\MounteD\file1
					if len(source) > 0 && source[len(source)-1] == common.AZCOPY_PATH_SEPARATOR_CHAR {
						sourcePath = fmt.Sprintf("%s%s", source, relativePath)
					} else {
						sourcePath = fmt.Sprintf("%s%s%s", source, common.AZCOPY_PATH_SEPARATOR_STRING, relativePath)
					}

					// check if the sourcePath needs to be include or not
					if !util.resourceShouldBeIncluded(parentSource, include, sourcePath) {
						return nil
					}
					// check if the source has to be excluded or not
					if util.resourceShouldBeExcluded(parentSource, exclude, sourcePath) {
						return nil
					}

					// create the transfer and add to the list
					destinationUrl.Path = util.generateObjectPath(cleanContainerPath,
						util.getRelativePath(parentSource, sourcePath))
					transfer := common.CopyTransfer{
						Source:           path,
						Destination:      destinationUrl.String(),
						LastModifiedTime: fileInfo.ModTime(),
						SourceSize:       fileInfo.Size(),
					}
					transferList = append(transferList, transfer)
					return nil
				} else if fileInfo.Mode()&os.ModeSymlink != 0 { // If the file is a symlink
					// replace the windows path separator in the path with "/" path separator
					path = strings.Replace(path, common.OS_PATH_SEPARATOR, common.AZCOPY_PATH_SEPARATOR_STRING, -1)
					// Evaulate the symlink path
					sLinkPath, err := filepath.EvalSymlinks(path)
					if err != nil {
						errorList = append(errorList, err)
						return nil
					}
					// strip the original symlink path and concatenate the relativePath to the original sourcePath
					// for Example: source = C:\MountedD sLinkPath = D:\MountedE
					// relativePath = MountedE , sourcePath = C;\MountedD\MountedE
					relativePath := strings.Replace(path, symlinkPath, "", 1)
					var sourcePath = ""
					// concatenate the relative symlink path to the original source
					if len(source) > 0 && source[len(source)-1] == common.AZCOPY_PATH_SEPARATOR_CHAR {
						sourcePath = fmt.Sprintf("%s%s", source, relativePath)
					} else {
						sourcePath = fmt.Sprintf("%s%s%s", source, common.AZCOPY_PATH_SEPARATOR_STRING, relativePath)
					}
					tlist, erList := util.getSymlinkTransferList(sLinkPath, sourcePath,
						parentSource, cleanContainerPath, destinationUrl,
						include, exclude)
					transferList = append(transferList, tlist...)
					errorList = append(errorList, erList...)
					return nil
				}
				return nil
			})
		} else if fInfo.Mode().IsRegular() {
			// strip the original symlink path
			relativePath := strings.Replace(files, symlinkPath, "", 1)

			// concatenate the path to the parent source
			var sourcePath = ""
			if len(source) > 0 && source[len(source)-1] == common.AZCOPY_PATH_SEPARATOR_CHAR {
				sourcePath = fmt.Sprintf("%s%s", source, relativePath)
			} else {
				sourcePath = fmt.Sprintf("%s%s%s", source, common.AZCOPY_PATH_SEPARATOR_STRING, relativePath)
			}

			// check if the sourcePath needs to be include or not
			if !util.resourceShouldBeIncluded(parentSource, include, sourcePath) {
				continue
			}
			// check if the source has to be excluded or not
			if util.resourceShouldBeExcluded(parentSource, exclude, sourcePath) {
				continue
			}

			// create the transfer and add to the list
			destinationUrl.Path = util.generateObjectPath(cleanContainerPath,
				util.getRelativePath(source, sourcePath))
			transfer := common.CopyTransfer{
				Source:           files,
				Destination:      destinationUrl.String(),
				LastModifiedTime: fInfo.ModTime(),
				SourceSize:       fInfo.Size(),
			}
			transferList = append(transferList, transfer)
		} else {
			continue
		}
	}
	return transferList, errorList
}

// get relative path given a root path
func (copyHandlerUtil) getRelativePath(rootPath, filePath string) string {
	// root path contains the entire absolute path to the root directory, so we need to take away everything except the root directory from filePath
	// example: rootPath = "/dir1/dir2/dir3" filePath = "/dir1/dir2/dir3/file1.txt" result = "dir3/file1.txt" scrubAway="/dir1/dir2/"
	if len(rootPath) == 0 {
		return filePath
	}
	result := filePath

	// replace the path separator in filepath with AZCOPY_PATH_SEPARATOR
	// this replacement is required to handle the windows filepath
	filePath = strings.Replace(filePath, common.OS_PATH_SEPARATOR, common.AZCOPY_PATH_SEPARATOR_STRING, -1)
	var scrubAway string
	// test if root path finishes with a /, if yes, ignore it
	if rootPath[len(rootPath)-1:] == common.AZCOPY_PATH_SEPARATOR_STRING {
		scrubAway = rootPath[:strings.LastIndex(rootPath[:len(rootPath)-1], common.AZCOPY_PATH_SEPARATOR_STRING)+1]
	} else {
		// +1 because we want to include the / at the end of the dir
		scrubAway = rootPath[:strings.LastIndex(rootPath, common.AZCOPY_PATH_SEPARATOR_STRING)+1]
	}

	result = strings.Replace(filePath, scrubAway, "", 1)

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
	if strings.LastIndex(directoryPath, common.AZCOPY_PATH_SEPARATOR_STRING) == len(directoryPath)-1 {
		result = fmt.Sprintf("%s%s", directoryPath, fileName)
	} else {
		result = fmt.Sprintf("%s%s%s", directoryPath, common.AZCOPY_PATH_SEPARATOR_STRING, fileName)
	}

	// blob name has "/" as Path Separator.
	// To preserve the path in blob name on local disk, replace "/" with OS Path Separator
	// For Example blob name = "blob-1/blob-2/blob-2" will be "blob-1\\blob-2\\blob-3" for windows
	//return strings.Replace(result, "/", string(os.PathSeparator), -1)
	return result
}

func (util copyHandlerUtil) getBlobNameFromURL(path string) string {
	// return everything after the second /
	return strings.SplitAfterN(path[1:], common.AZCOPY_PATH_SEPARATOR_STRING, 2)[1]
}

func (util copyHandlerUtil) getDirNameFromSource(path string) (sourcePathWithoutPrefix, searchPrefix string) {
	if path[len(path)-1:] == common.AZCOPY_PATH_SEPARATOR_STRING {
		sourcePathWithoutPrefix = path[:strings.LastIndex(path[:len(path)-1], common.AZCOPY_PATH_SEPARATOR_STRING)+1]
		searchPrefix = path[strings.LastIndex(path[:len(path)-1], common.AZCOPY_PATH_SEPARATOR_STRING)+1:]
	} else {
		// +1 because we want to include the / at the end of the dir
		sourcePathWithoutPrefix = path[:strings.LastIndex(path, common.AZCOPY_PATH_SEPARATOR_STRING)+1]
		searchPrefix = path[strings.LastIndex(path, common.AZCOPY_PATH_SEPARATOR_STRING)+1:]
	}
	return
}

func (util copyHandlerUtil) firstIndexOfWildCard(name string) int {
	return strings.Index(name, wildCard)
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

// stripSASFromFileShareUrl takes azure file and remove the SAS query param from the URL.
func (util copyHandlerUtil) stripSASFromFileShareUrl(fileUrl url.URL) *url.URL {
	fuParts := azfile.NewFileURLParts(fileUrl)
	fuParts.SAS = azfile.SASQueryParameters{}
	fUrl := fuParts.URL()
	return &fUrl
}

// stripSASFromBlobUrl takes azure blob url and remove the SAS query param from the URL
func (util copyHandlerUtil) stripSASFromBlobUrl(blobUrl url.URL) *url.URL {
	buParts := azblob.NewBlobURLParts(blobUrl)
	buParts.SAS = azblob.SASQueryParameters{}
	bUrl := buParts.URL()
	return &bUrl
}

// createBlobUrlFromContainer returns a url for given blob parts and blobName.
func (util copyHandlerUtil) createBlobUrlFromContainer(blobUrlParts azblob.BlobURLParts, blobName string) url.URL {
	blobUrlParts.BlobName = blobName
	return blobUrlParts.URL()
}

func (util copyHandlerUtil) appendBlobNameToUrl(blobUrlParts azblob.BlobURLParts, blobName string) (url.URL, string) {
	//if os.PathSeparator == '\\' {
	//	blobName = strings.Replace(blobName, string(os.PathSeparator), "/", -1)
	//}
	if blobUrlParts.BlobName == "" {
		blobUrlParts.BlobName = blobName
	} else {
		if blobUrlParts.BlobName[len(blobUrlParts.BlobName)-1] == '/' {
			blobUrlParts.BlobName += blobName
		} else {
			blobUrlParts.BlobName += common.AZCOPY_PATH_SEPARATOR_STRING + blobName
		}
	}
	return blobUrlParts.URL(), blobUrlParts.BlobName
}

// sourceRootPathWithoutWildCards returns the directory from path that does not have wildCards
// returns the patterns that defines pattern for relativePath of files to the above mentioned directory
// For Example: source = C:\User\a*\a1*\*.txt rootDir = C:\User\ pattern = a*\a1*\*.txt
func (util copyHandlerUtil) sourceRootPathWithoutWildCards(path string) (string, string) {
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
	// for Example: source = C:\User\a*\a1*\*.txt pathWithoutWildcard = C:\User\a
	// sepIndex = 7
	// rootDirectory = C:\User and pattern = a*\a1*\*.txt
	sepIndex := strings.LastIndex(pathWithoutWildcard, common.AZCOPY_PATH_SEPARATOR_STRING)
	if sepIndex == -1 {
		return "", path
	}
	return pathWithoutWildcard[:sepIndex], path[sepIndex+1:]
}

// blobNameMatchesThePatternComponentWise matches the blobName against the pattern component wise
// Example: /home/user/dir*/*file matches /home/user/dir1/abcfile but does not matches
// /home/user/dir1/dir2/abcfile. This api does not assume path separator '/' for a wildcard '*'
func (util copyHandlerUtil) blobNameMatchesThePatternComponentWise(pattern string, blobName string) bool {
	// find the number of path separator in pattern and blobName
	// If the number of path separator doesn't match, then blob name doesn't match the pattern
	pSepInPattern := strings.Count(pattern, common.AZCOPY_PATH_SEPARATOR_STRING)
	pSepInBlobName := strings.Count(blobName, common.AZCOPY_PATH_SEPARATOR_STRING)
	if pSepInPattern != pSepInBlobName {
		return false
	}
	// If the number of path separator matches in both blobName and pattern
	// each component of the blobName should match each component in pattern
	// Length of patternComponents and blobNameComponents is same since we already
	// match the number of path separators above.
	patternComponents := strings.Split(blobName, common.AZCOPY_PATH_SEPARATOR_STRING)
	blobNameComponents := strings.Split(blobName, common.AZCOPY_PATH_SEPARATOR_STRING)
	for index := 0; index < len(patternComponents); index++ {
		// match the pattern component and blobName component
		if !util.blobNameMatchesThePattern(patternComponents[index], blobNameComponents[index]) {
			return false
		}
	}
	return true
}

func (util copyHandlerUtil) blobNameMatchesThePattern(patternString string, blobName string) bool {
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

// matchBlobNameAgainstPattern matches the given blobName against the pattern. If the recursive is set to true
// '*' in the pattern will match the path sep since we need to recursively look into the sub-dir of given source.
// If recursive is set to false, then matches happens component wise where component is each dir in the given path
// defined by the blobname. For Example: blobname = /dir-1/dir-2/blob1.txt components are dir-1, dir-2, blob1.txt
func (util copyHandlerUtil) matchBlobNameAgainstPattern(pattern string, blobName string, recursive bool) bool {
	if recursive {
		return util.blobNameMatchesThePattern(pattern, blobName)
	}
	return util.blobNameMatchesThePatternComponentWise(pattern, blobName)
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

func (util copyHandlerUtil) generateBlobUrl(containerUrl url.URL, blobName string) url.URL {
	if containerUrl.Path[len(containerUrl.Path)-1] != '/' {
		containerUrl.Path = containerUrl.Path + "/" + blobName
	} else {
		containerUrl.Path = containerUrl.Path + blobName
	}
	return containerUrl
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
func (util copyHandlerUtil) doesBlobRepresentAFolder(bInfo azblob.BlobItem) bool {
	// this condition is to handle the WASB V1 directory structure.
	// HDFS driver creates a blob for the empty directories (let’s call it ‘myfolder’)
	// and names all the blobs under ‘myfolder’ as such: ‘myfolder/myblob’
	// The empty directory has meta-data 'hdi_isfolder = true'
	return bInfo.Metadata["hdi_isfolder"] == "true"
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
// TODO: deprecated, remove this method
func (util copyHandlerUtil) getDeepestDirOrFileURLFromString(ctx context.Context, givenURL url.URL, p pipeline.Pipeline) (*azfile.DirectoryURL, *azfile.FileURL, *azfile.FileGetPropertiesResponse, bool) {
	url := givenURL
	path := url.Path

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
				glcm.Info("Fail to parse " +
					common.URLExtension{URL: url}.RedactSigQueryParamForLogging() +
					" as a file for error " + err.Error() + ", given URL: " + givenURL.String())
			}
		}
	}
	dirURL := azfile.NewDirectoryURL(url, p)
	if _, err := dirURL.GetProperties(ctx); err == nil {
		return &dirURL, nil, nil, true
	} else {
		glcm.Info("Fail to parse " +
			common.URLExtension{URL: url}.RedactSigQueryParamForLogging() +
			" as a directory for error " + err.Error() + ", given URL: " + givenURL.String())
	}

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

// replaceBackSlashWithSlash replaces all backslash '\' with slash '/' in a given URL string.
func (util copyHandlerUtil) replaceBackSlashWithSlash(urlStr string) string {
	str := strings.Replace(urlStr, "\\", "/", -1)

	return str
}

/////////////////////////////////////////////////////////////////////////////////////////////////
type urlExtension struct {
	url.URL
}

func (u urlExtension) redactSigQueryParamForLogging() string {
	if ok, rawQuery := gCopyUtil.redactSigQueryParam(u.RawQuery); ok {
		u.RawQuery = rawQuery
	}

	return u.String()
}

func (u urlExtension) generateObjectPath(objectName string) url.URL {
	u.Path = gCopyUtil.generateObjectPath(u.Path, objectName)
	return u.URL
}

/////////////////////////////////////////////////////////////////////////////////////////////////
type blobURLPartsExtension struct {
	azblob.BlobURLParts
}

// searchPrefixFromBlobURL gets search prefix and patterns from Blob URL.
func (parts blobURLPartsExtension) searchPrefixFromBlobURL() (prefix, pattern string, isWildcardSearch bool) {
	// If the blobName is empty, it means the url provided is of a container,
	// then all blobs inside containers needs to be included, so pattern is set to *
	if parts.BlobName == "" {
		pattern = "*"
		return
	}
	// Check for wildcards and get the index of first wildcard
	// If the wild card does not exists, then index returned is -1
	wildCardIndex := gCopyUtil.firstIndexOfWildCard(parts.BlobName)
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

	isWildcardSearch = true
	// wild card exists prefix will be the content of blob name till the wildcard index
	// Example: https://<container-name>/vd-1/vd-2/abc*
	// prefix = /vd-1/vd-2/abc and pattern = /vd-1/vd-2/abc*
	// All the blob inside the container in virtual dir vd-2 that have the prefix "abc"
	prefix = parts.BlobName[:wildCardIndex]
	pattern = parts.BlobName

	return
}

// isBlobAccountLevelSearch check if it's an account level search for blob service.
// And returns search prefix(part before wildcard) for container and pattern is the blob pattern to match.
func (parts blobURLPartsExtension) isBlobAccountLevelSearch() (isBlobAccountLevelSearch bool, containerPrefix string) {
	// If it's account level URL which need search container, there could be two cases:
	// a. https://<account-name>(/)
	// b. https://<account-name>/containerprefix*(/*)
	if parts.ContainerName == "" ||
		strings.Contains(parts.ContainerName, wildCard) {
		isBlobAccountLevelSearch = true
		// For case container name is empty, search for all containers.
		if parts.ContainerName == "" {
			return
		}

		wildCardIndex := gCopyUtil.firstIndexOfWildCard(parts.ContainerName)

		// wild card exists prefix will be the content of container name till the wildcard index
		// Example 1: for URL https://<account-name>/c-2*, containerPrefix = c-2
		// Example 2: for URL https://<account-name>/c-2*/vd/b*, containerPrefix = c-2
		containerPrefix = parts.ContainerName[:wildCardIndex]
		return
	}
	// Otherwise, it's not account level search.
	return
}

func (parts blobURLPartsExtension) getContainerURL() url.URL {
	parts.BlobName = ""
	return parts.URL()
}

func (parts blobURLPartsExtension) getServiceURL() url.URL {
	parts.ContainerName = ""
	parts.BlobName = ""
	return parts.URL()
}

func (parts blobURLPartsExtension) isContainerURL() bool {
	return parts.ContainerName != "" && parts.BlobName == ""
}

// Get the source path without the wildcards
// This is defined since the files mentioned with exclude flag
// & include flag are relative to the Source
// If the source has wildcards, then files are relative to the
// parent source path which is the path of last directory in the source
// without wildcards
// For Example: src = "/home/user/dir1" parentSourcePath = "/home/user/dir1"
// For Example: src = "/home/user/dir*" parentSourcePath = "/home/user"
// For Example: src = "/home/*" parentSourcePath = "/home"
func (parts blobURLPartsExtension) getParentSourcePath() string {
	parentSourcePath := parts.BlobName
	wcIndex := gCopyUtil.firstIndexOfWildCard(parentSourcePath)
	if wcIndex != -1 {
		parentSourcePath = parentSourcePath[:wcIndex]
		pathSepIndex := strings.LastIndex(parentSourcePath, "/")
		if pathSepIndex == -1 {
			parentSourcePath = ""
		} else {
			parentSourcePath = parentSourcePath[:pathSepIndex]
		}
	}

	return parentSourcePath
}

/////////////////////////////////////////////////////////////////////////////////////////////////
type fileURLPartsExtension struct {
	azfile.FileURLParts
}

// isFileAccountLevelSearch check if it's an account level search for file service.
// And returns search prefix(part before wildcard) and pattern when it's account level search.
func (parts fileURLPartsExtension) isFileAccountLevelSearch() (isFileAccountLevelSearch bool, prefix string) {
	// If it's account level URL which need search share, there could be two cases:
	// a. https://<account-name>(/)
	// b. https://<account-name>/shareprefix*
	if parts.ShareName == "" ||
		strings.Contains(parts.ShareName, wildCard) {
		isFileAccountLevelSearch = true
		// For case 1-a, search for all shares.
		if parts.ShareName == "" {
			return
		}

		wildCardIndex := gCopyUtil.firstIndexOfWildCard(parts.ShareName)
		// wild card exists prefix will be the content of share name till the wildcard index
		// Example 1: for URL https://<account-name>/s-2*, sharePrefix = s-2
		// Example 2: for URL https://<account-name>/s-2*/d/f*, sharePrefix = s-2
		prefix = parts.ShareName[:wildCardIndex]
		return
	}
	// Otherwise, it's not account level search.
	return
}

// searchPrefixFromFileURL aligns to blobURL's method searchPrefixFromBlobURL
// Note: This method doesn't validate if the provided URL points to a FileURL, and will treat the input without
// wildcard as directory URL.
func (parts fileURLPartsExtension) searchPrefixFromFileURL() (prefix, pattern string, isWildcardSearch bool) {
	// If the DirectoryOrFilePath is empty, it means the url provided is of a share,
	// then all files inside share needs to be included, so pattern is set to *
	if parts.DirectoryOrFilePath == "" {
		pattern = "*"
		return
	}
	// Check for wildcards and get the index of first wildcard
	// If the wild card does not exists, then index returned is -1
	wildCardIndex := gCopyUtil.firstIndexOfWildCard(parts.DirectoryOrFilePath)
	if wildCardIndex < 0 {
		// If no wild card exits and url represents a directory
		// prefix is the path of directory after the share.
		// Example: https://<share-name>/d-1?<signature>, prefix = /d-1
		// Example: https://<share-name>/d-1/d-2?<signature>, prefix = /d-1/d-2
		prefix = parts.DirectoryOrFilePath
		// check for separator at the end of directory
		if prefix[len(prefix)-1] != '/' {
			prefix += "/"
		}
		// since the url is a directory, then all files inside the directory
		// needs to be downloaded, so the pattern is "*"
		pattern = "*"
		return
	}

	isWildcardSearch = true
	// wild card exists prefix will be the content of file name till the wildcard index
	// Example: https://<share-name>/vd-1/vd-2/abc*
	// prefix = /vd-1/vd-2/abc and pattern = /vd-1/vd-2/abc*
	// All the file inside the share in dir vd-2 that have the prefix "abc"
	prefix = parts.DirectoryOrFilePath[:wildCardIndex]
	pattern = parts.DirectoryOrFilePath

	return
}

// Aligns to blobURL's getParentSourcePath
func (parts fileURLPartsExtension) getParentSourcePath() string {
	parentSourcePath := parts.DirectoryOrFilePath
	wcIndex := gCopyUtil.firstIndexOfWildCard(parentSourcePath)
	if wcIndex != -1 {
		parentSourcePath = parentSourcePath[:wcIndex]
		pathSepIndex := strings.LastIndex(parentSourcePath, "/")
		if pathSepIndex == -1 {
			parentSourcePath = ""
		} else {
			parentSourcePath = parentSourcePath[:pathSepIndex]
		}
	}

	return parentSourcePath
}

// getDirURLAndSearchPrefixFromFileURL gets the sub dir and file search prefix based on provided File service resource URL.
// Note: This method doesn't validate if the provided URL points to a FileURL, and will treat the input without
// wildcard as directory URL.
func (parts fileURLPartsExtension) getDirURLAndSearchPrefixFromFileURL(p pipeline.Pipeline) (dirURL azfile.DirectoryURL, prefix string) {
	// If the DirectoryOrFilePath is empty, it means the url provided is of a share,
	// then all files and directories inside share needs to be included, so pattern is set to *
	if parts.DirectoryOrFilePath == "" {
		dirURL = azfile.NewDirectoryURL(parts.URL(), p)
		return
	}
	// Check for wildcards and get the index of first wildcard
	// If the wild card does not exists, then index returned is -1
	wildCardIndex := gCopyUtil.firstIndexOfWildCard(parts.DirectoryOrFilePath)
	if wildCardIndex < 0 {
		// If no wild card exits and url represents a directory
		// file prefix is "".
		// Example: https://<share-name>/d-1?<signature>, directoryURL = https://<share-name>/d-1?<signature>, prefix = ""
		dirURL = azfile.NewDirectoryURL(parts.URL(), p)
		return
	}
	// wild card exists prefix will be the content of file name till the wildcard index
	// Example: https://<share-name>/d-1/d-2/abc*
	// diretoryURL = "https://<share-name>/d-1/d-2/", prefix = abc
	dirOrFilePath := parts.DirectoryOrFilePath
	lastSlashIndex := strings.LastIndex(dirOrFilePath, "/")

	prefix = dirOrFilePath[lastSlashIndex+1 : wildCardIndex] // If no slash exist, start from 0, end at wildcard index.

	// compose the parent directory of search prefix
	parts.DirectoryOrFilePath = dirOrFilePath[:lastSlashIndex]
	dirURL = azfile.NewDirectoryURL(parts.URL(), p)
	return
}

func (parts fileURLPartsExtension) getShareURL() url.URL {
	parts.DirectoryOrFilePath = ""
	return parts.URL()
}

func (parts fileURLPartsExtension) getServiceURL() url.URL {
	parts.ShareName = ""
	parts.DirectoryOrFilePath = ""
	return parts.URL()
}
