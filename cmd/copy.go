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
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/Azure/azure-pipeline-go/pipeline"

	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/Azure/azure-storage-file-go/azfile"
	"github.com/spf13/cobra"

	"github.com/Azure/azure-storage-azcopy/azbfs"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-azcopy/ste"
)

const pipingUploadParallelism = 5
const pipingDefaultBlockSize = 8 * 1024 * 1024

// For networking throughput in Mbps, (and only for networking), we divide by 1000*1000 (not 1024 * 1024) because
// networking is traditionally done in base 10 units (not base 2).
// E.g. "gigabit ethernet" means 10^9 bits/sec, not 2^30. So by using base 10 units
// we give the best correspondence to the sizing of the user's network pipes.
// See https://networkengineering.stackexchange.com/questions/3628/iec-or-si-units-binary-prefixes-used-for-network-measurement
// NOTE that for everything else in the app (e.g. sizes of files) we use the base 2 units (i.e. 1024 * 1024) because
// for RAM and disk file sizes, it is conventional to use the power-of-two-based units.
const base10Mega = 1000 * 1000

const pipeLocation = "~pipe~"

// represents the raw copy command input from the user
type rawCopyCmdArgs struct {
	// from arguments
	src    string
	dst    string
	fromTo string
	//blobUrlForRedirection string

	// TODO remove after refactoring
	legacyInclude string
	legacyExclude string
	// new include/exclude only apply to file names
	// implemented for remove (and sync) only
	include               string
	exclude               string
	includePath           string
	excludePath           string
	includeFileAttributes string
	excludeFileAttributes string

	// filters from flags
	listOfFilesToCopy string
	recursive         bool
	followSymlinks    bool
	withSnapshots     bool
	// forceWrite flag is used to define the User behavior
	// to overwrite the existing blobs or not.
	forceWrite bool

	// options from flags
	blockSizeMB              float64
	metadata                 string
	contentType              string
	contentEncoding          string
	contentDisposition       string
	contentLanguage          string
	cacheControl             string
	noGuessMimeType          bool
	preserveLastModifiedTime bool
	putMd5                   bool
	md5ValidationOption      string
	s2sCheckLength           bool
	// defines the type of the blob at the destination in case of upload / account to account copy
	blobType        string
	blockBlobTier   string
	pageBlobTier    string
	background      bool
	output          string // TODO: Is this unused now? replaced with param at root level?
	acl             string
	logVerbosity    string
	cancelFromStdin bool
	// list of blobTypes to exclude while enumerating the transfer
	excludeBlobType string
	// whether user wants to preserve full properties during service to service copy, the default value is true.
	// For S3 and Azure File non-single file source, as list operation doesn't return full properties of objects/files,
	// to preserve full properties AzCopy needs to send one additional request per object/file.
	s2sPreserveProperties bool
	// useful when preserveS3Properties set to true, enables get S3 objects' or Azure files' properties during s2s copy in backend, the default value is true
	s2sGetPropertiesInBackend bool
	// whether user wants to preserve access tier during service to service copy, the default value is true.
	// In some case, e.g. target is a GPv1 storage account, access tier cannot be set properly.
	// In such cases, use s2sPreserveAccessTier=false to bypass the access tier copy.
	// For more details, please refer to https://docs.microsoft.com/en-us/azure/storage/blobs/storage-blob-storage-tiers
	s2sPreserveAccessTier bool
	// whether user wants to check if source has changed after enumerating, the default value is true.
	// For S2S copy, as source is a remote resource, validating whether source has changed need additional request costs.
	s2sSourceChangeValidation bool
	// specify how user wants to handle invalid metadata.
	s2sInvalidMetadataHandleOption string
}

func (raw *rawCopyCmdArgs) parsePatterns(pattern string) (cookedPatterns []string) {
	cookedPatterns = make([]string, 0)
	rawPatterns := strings.Split(pattern, ";")
	for _, pattern := range rawPatterns {

		// skip the empty patterns
		if len(pattern) != 0 {
			cookedPatterns = append(cookedPatterns, pattern)
		}
	}

	return
}

// blocSizeInBytes converts a FLOATING POINT number of MiB, to a number of bytes
// A non-nil error is returned if the conversion is not possible to do accurately (e.g. it comes out of a fractional number of bytes)
// The purpose of using floating point is to allow specialist users (e.g. those who want small block sizes to tune their read IOPS)
// to use fractions of a MiB. E.g.
// 0.25 = 256 KiB
// 0.015625 = 16 KiB
func blockSizeInBytes(rawBlockSizeInMiB float64) (uint32, error) {
	if rawBlockSizeInMiB < 0 {
		return 0, errors.New("negative block size not allowed")
	}
	rawSizeInBytes := rawBlockSizeInMiB * 1024 * 1024 // internally we use bytes, but users' convenience the command line uses MiB
	if rawSizeInBytes > math.MaxUint32 {
		return 0, errors.New("block size too big for uint32")
	}
	const epsilon = 0.001 // arbitrarily using a tolerance of 1000th of a byte
	_, frac := math.Modf(rawSizeInBytes)
	isWholeNumber := frac < epsilon || frac > 1.0-epsilon // frac is very close to 0 or 1, so rawSizeInBytes is (very close to) an integer
	if !isWholeNumber {
		return 0, fmt.Errorf("while fractional numbers of MiB are allowed as the block size, the fraction must result to a whole number of bytes. %.12f MiB resolves to %.3f bytes", rawBlockSizeInMiB, rawSizeInBytes)
	}
	return uint32(math.Round(rawSizeInBytes)), nil
}

// validates and transform raw input into cooked input
func (raw rawCopyCmdArgs) cook() (cookedCopyCmdArgs, error) {
	cooked := cookedCopyCmdArgs{}

	fromTo, err := validateFromTo(raw.src, raw.dst, raw.fromTo) // TODO: src/dst
	if err != nil {
		return cooked, err
	}
	cooked.source = raw.src
	cooked.destination = raw.dst

	cooked.fromTo = fromTo

	// copy&transform flags to type-safety
	cooked.recursive = raw.recursive
	cooked.followSymlinks = raw.followSymlinks
	cooked.withSnapshots = raw.withSnapshots
	cooked.forceWrite = raw.forceWrite

	cooked.blockSize, err = blockSizeInBytes(raw.blockSizeMB)
	if err != nil {
		return cooked, err
	}

	// parse the given blob type.
	err = cooked.blobType.Parse(raw.blobType)
	if err != nil {
		return cooked, err
	}

	// If the given blobType is AppendBlob, block-size-mb should not be greater than
	// 4MB.
	if cookedSize, _ := blockSizeInBytes(raw.blockSizeMB); cooked.blobType == common.EBlobType.AppendBlob() && cookedSize > common.MaxAppendBlobBlockSize {
		return cooked, fmt.Errorf("block size cannot be greater than 4MB for AppendBlob blob type")
	}

	err = cooked.blockBlobTier.Parse(raw.blockBlobTier)
	if err != nil {
		return cooked, err
	}
	err = cooked.pageBlobTier.Parse(raw.pageBlobTier)
	if err != nil {
		return cooked, err
	}
	err = cooked.logVerbosity.Parse(raw.logVerbosity)
	if err != nil {
		return cooked, err
	}

	// Note: new implementation of list-of-files only works for remove command and upload for now.
	// * -> Garbage and * -> Local work under this implementation
	if fromTo.To() == common.ELocation.Unknown() || cooked.fromTo.From() == common.ELocation.Local() {
		// This handles both list-of-files and include-path as a list enumerator.
		// This saves us time because we know *exactly* what we're looking for right off the bat.
		// Note that exclude-path is handled as a filter unlike include-path.

		if (len(raw.include) > 0 || len(raw.exclude) > 0) && cooked.fromTo == common.EFromTo.BlobFSTrash() {
			return cooked, fmt.Errorf("include/exclude flags are not supported for this destination")
		}

		// unbuffered so this reads as we need it to rather than all at once in bulk
		listChan := make(chan string)
		var f *os.File

		if raw.listOfFilesToCopy != "" {
			f, err = os.Open(raw.listOfFilesToCopy)

			if err != nil {
				return cooked, fmt.Errorf("cannot open %s file passed with the list-of-file flag", raw.listOfFilesToCopy)
			}
		}

		go func() {
			defer close(listChan)

			if f != nil {
				scanner := bufio.NewScanner(f)
				for scanner.Scan() {
					v := scanner.Text()
					listChan <- v
				}
			}

			// This occurs much earlier than the other include or exclude filters. I'd _like_ to move it closer in cook() once we get rid of the other bits of list-of-files.
			includePathList := raw.parsePatterns(raw.includePath)

			for _, v := range includePathList {
				listChan <- v
			}
		}()

		// A combined implementation reduces the amount of code duplication present.
		// However, it _does_ increase the amount of code-intertwining present.
		if raw.listOfFilesToCopy != "" && raw.includePath != "" {
			return cooked, errors.New("cannot combine list of files and include path")
		}

		if raw.listOfFilesToCopy != "" || raw.includePath != "" {
			cooked.listOfFilesChannel = listChan
		}
	} else if len(raw.listOfFilesToCopy) > 0 {
		// TODO remove this legacy implementation after copy enumerator refactoring

		// User can provide either listOfFilesToCopy or include since listOFFiles mentions
		// file names to include explicitly and include file may mention the pattern.
		// This could conflict enumerating the files to queue up for transfer.
		if len(raw.listOfFilesToCopy) > 0 && len(raw.legacyInclude) > 0 {
			return cooked, fmt.Errorf("both list-of-files and include flag were provided." +
				"Only one of them is allowed at a time")
		}

		// If the user provided the list of files explicitly to be copied, then parse the argument
		// The user passes the location of json file which will have the list of files to be copied.
		// The "json file" is chosen as input because there is limit on the number of characters that
		// can be supplied with the argument, but Storage Explorer folks requirements was not to impose
		// any limit on the number of files that can be copied.

		jsonFile, err := os.Open(raw.listOfFilesToCopy)
		if err != nil {
			return cooked, fmt.Errorf("cannot open %s file passed with the list-of-file flag", raw.listOfFilesToCopy)
		}
		// read opened json file as a byte array.
		jsonBytes, err := ioutil.ReadAll(jsonFile)
		if err != nil {
			return cooked, fmt.Errorf("error %s read %s file passed with the list-of-file flag", err.Error(), raw.listOfFilesToCopy)
		}
		var files common.ListOfFiles
		err = json.Unmarshal(jsonBytes, &files)
		if err != nil {
			return cooked, fmt.Errorf("error %s unmarshalling the contents of %s file passed with the list-of-file flag", err.Error(), raw.listOfFilesToCopy)
		}
		for _, file := range files.Files {
			// If split of the include string leads to an empty string
			// not include that string
			if len(file) == 0 {
				continue
			}
			// replace the OS path separator in includePath string with AZCOPY_PATH_SEPARATOR
			// this replacement is done to handle the windows file paths where path separator "\\"
			filePath := strings.Replace(file, common.OS_PATH_SEPARATOR, common.AZCOPY_PATH_SEPARATOR_STRING, -1)
			cooked.listOfFilesToCopy = append(cooked.listOfFilesToCopy, filePath)
		}
	}

	// TODO: When further in the refactor, just check this against a map
	if cooked.fromTo.From() != common.ELocation.Local() {
		// initialize the include map which contains the list of files to be included
		// parse the string passed in include flag
		// more than one file are expected to be separated by ';'
		cooked.legacyInclude = make(map[string]int)
		if len(raw.legacyInclude) > 0 {
			files := strings.Split(raw.legacyInclude, ";")
			for index := range files {
				// If split of the include string leads to an empty string
				// not include that string
				if len(files[index]) == 0 {
					continue
				}
				// replace the OS path separator in includePath string with AZCOPY_PATH_SEPARATOR
				// this replacement is done to handle the windows file paths where path separator "\\"
				includePath := strings.Replace(files[index], common.OS_PATH_SEPARATOR, common.AZCOPY_PATH_SEPARATOR_STRING, -1)
				cooked.legacyInclude[includePath] = index
			}
		}

		// initialize the exclude map which contains the list of files to be excluded
		// parse the string passed in exclude flag
		// more than one file are expected to be separated by ';'
		cooked.legacyExclude = make(map[string]int)
		if len(raw.legacyExclude) > 0 {
			files := strings.Split(raw.legacyExclude, ";")
			for index := range files {
				// If split of the include string leads to an empty string
				// not include that string
				if len(files[index]) == 0 {
					continue
				}
				// replace the OS path separator in excludePath string with AZCOPY_PATH_SEPARATOR
				// this replacement is done to handle the windows file paths where path separator "\\"
				excludePath := strings.Replace(files[index], common.OS_PATH_SEPARATOR, common.AZCOPY_PATH_SEPARATOR_STRING, -1)
				cooked.legacyExclude[excludePath] = index
			}
		}
	} else {
		// I do not like this implementation whatsoever, but just for now, we need to circumvent the legacy include and excludes.
		raw.include = raw.legacyInclude
		raw.exclude = raw.legacyExclude
	}

	cooked.metadata = raw.metadata
	cooked.contentType = raw.contentType
	cooked.contentEncoding = raw.contentEncoding
	cooked.contentLanguage = raw.contentLanguage
	cooked.contentDisposition = raw.contentDisposition
	cooked.cacheControl = raw.cacheControl
	cooked.noGuessMimeType = raw.noGuessMimeType
	cooked.preserveLastModifiedTime = raw.preserveLastModifiedTime

	cooked.putMd5 = raw.putMd5
	err = cooked.md5ValidationOption.Parse(raw.md5ValidationOption)
	if err != nil {
		return cooked, err
	}

	// Do not check for an error on this. Leaving this as true doesn't hurt, as the event never triggers unless it's an s2s copy anyway.
	// This is thanks to a type assertion attempt.
	// Atop this, leaving this on board for up/downloads in the future would be useful.
	cooked.s2sCheckLength = raw.s2sCheckLength

	cooked.background = raw.background
	cooked.acl = raw.acl
	cooked.cancelFromStdin = raw.cancelFromStdin

	// if redirection is triggered, avoid printing any output
	if cooked.isRedirection() {
		glcm.SetOutputFormat(common.EOutputFormat.None())
	}

	// generate a unique job ID
	cooked.jobID = common.NewJobID()

	// check for the flag value relative to fromTo location type
	// Example1: for Local to Blob, preserve-last-modified-time flag should not be set to true
	// Example2: for Blob to Local, follow-symlinks, blob-tier flags should not be provided with values.
	switch cooked.fromTo {
	case common.EFromTo.LocalBlobFS():
		if cooked.blobType != common.EBlobType.Detect() && cooked.blobType != common.EBlobType.None() {
			return cooked, fmt.Errorf("blob-type is not supported on ADLS Gen 2")
		}
	case common.EFromTo.LocalBlob():
		if cooked.preserveLastModifiedTime {
			return cooked, fmt.Errorf("preserve-last-modified-time is not supported while uploading")
		}
		if cooked.s2sPreserveProperties {
			return cooked, fmt.Errorf("s2s-preserve-properties is not supported while uploading")
		}
		if cooked.s2sPreserveAccessTier {
			return cooked, fmt.Errorf("s2s-preserve-access-tier is not supported while uploading")
		}
		if cooked.s2sInvalidMetadataHandleOption != common.DefaultInvalidMetadataHandleOption {
			return cooked, fmt.Errorf("s2s-handle-invalid-metadata is not supported while uploading")
		}
		if cooked.s2sSourceChangeValidation {
			return cooked, fmt.Errorf("s2s-detect-source-changed is not supported while uploading")
		}
	case common.EFromTo.LocalFile():
		if cooked.preserveLastModifiedTime {
			return cooked, fmt.Errorf("preserve-last-modified-time is not supported while uploading")
		}
		if cooked.blockBlobTier != common.EBlockBlobTier.None() ||
			cooked.pageBlobTier != common.EPageBlobTier.None() {
			return cooked, fmt.Errorf("blob-tier is not supported while uploading to Azure File")
		}
		if cooked.s2sPreserveProperties {
			return cooked, fmt.Errorf("s2s-preserve-properties is not supported while uploading")
		}
		if cooked.s2sPreserveAccessTier {
			return cooked, fmt.Errorf("s2s-preserve-access-tier is not supported while uploading")
		}
		if cooked.s2sInvalidMetadataHandleOption != common.DefaultInvalidMetadataHandleOption {
			return cooked, fmt.Errorf("s2s-handle-invalid-metadata is not supported while uploading")
		}
		if cooked.s2sSourceChangeValidation {
			return cooked, fmt.Errorf("s2s-detect-source-changed is not supported while uploading")
		}
		if cooked.blobType != common.EBlobType.Detect() && cooked.blobType != common.EBlobType.None() {
			return cooked, fmt.Errorf("blob-type is not supported on Azure File")
		}
	case common.EFromTo.BlobLocal(),
		common.EFromTo.FileLocal():
		if cooked.followSymlinks {
			return cooked, fmt.Errorf("follow-symlinks flag is not supported while downloading")
		}
		if cooked.blockBlobTier != common.EBlockBlobTier.None() ||
			cooked.pageBlobTier != common.EPageBlobTier.None() {
			return cooked, fmt.Errorf("blob-tier is not supported while downloading")
		}
		if cooked.noGuessMimeType {
			return cooked, fmt.Errorf("no-guess-mime-type is not supported while downloading")
		}
		if len(cooked.contentType) > 0 || len(cooked.contentEncoding) > 0 || len(cooked.contentLanguage) > 0 || len(cooked.contentDisposition) > 0 || len(cooked.cacheControl) > 0 || len(cooked.metadata) > 0 {
			return cooked, fmt.Errorf("content-type, content-encoding, content-language, content-disposition, cache-control, or metadata is not supported while downloading")
		}
		if cooked.s2sPreserveProperties {
			return cooked, fmt.Errorf("s2s-preserve-properties is not supported while downloading")
		}
		if cooked.s2sPreserveAccessTier {
			return cooked, fmt.Errorf("s2s-preserve-access-tier is not supported while downloading")
		}
		if cooked.s2sInvalidMetadataHandleOption != common.DefaultInvalidMetadataHandleOption {
			return cooked, fmt.Errorf("s2s-handle-invalid-metadata is not supported while downloading")
		}
		if cooked.s2sSourceChangeValidation {
			return cooked, fmt.Errorf("s2s-detect-source-changed is not supported while downloading")
		}
	case common.EFromTo.BlobBlob(),
		common.EFromTo.FileBlob(),
		common.EFromTo.S3Blob():
		if cooked.preserveLastModifiedTime {
			return cooked, fmt.Errorf("preserve-last-modified-time is not supported while copying from service to service")
		}
		if cooked.followSymlinks {
			return cooked, fmt.Errorf("follow-symlinks flag is not supported while copying from service to service")
		}
		// Disabling blob tier override, when copying block -> block blob or page -> page blob, blob tier will be kept,
		// For s3 and file, only hot block blob tier is supported.
		if cooked.fromTo == common.EFromTo.FileBlob() && cooked.blobType != common.EBlobType.Detect() && cooked.blobType != common.EBlobType.None() {
			return cooked, fmt.Errorf("blob-type is not supported while copying from file to blob")
		}
		if cooked.fromTo == common.EFromTo.S3Blob() && cooked.blobType != common.EBlobType.Detect() && cooked.blobType != common.EBlobType.None() {
			return cooked, fmt.Errorf("blob-type is not supported while copying from s3 to blob")
		}
		if cooked.blockBlobTier != common.EBlockBlobTier.None() ||
			cooked.pageBlobTier != common.EPageBlobTier.None() {
			return cooked, fmt.Errorf("blob-tier is not supported while copying from sevice to service")
		}
		if cooked.noGuessMimeType {
			return cooked, fmt.Errorf("no-guess-mime-type is not supported while copying from service to service")
		}
		if len(cooked.contentType) > 0 || len(cooked.contentEncoding) > 0 || len(cooked.contentLanguage) > 0 || len(cooked.contentDisposition) > 0 || len(cooked.cacheControl) > 0 || len(cooked.metadata) > 0 {
			return cooked, fmt.Errorf("content-type, content-encoding, content-language, content-disposition, cache-control, or metadata is not supported while copying from service to service")
		}
	}
	if err = validatePutMd5(cooked.putMd5, cooked.fromTo); err != nil {
		return cooked, err
	}
	if err = validateMd5Option(cooked.md5ValidationOption, cooked.fromTo); err != nil {
		return cooked, err
	}

	// If the user has provided some input with excludeBlobType flag, parse the input.
	if len(raw.excludeBlobType) > 0 {
		// Split the string using delimeter ';' and parse the individual blobType
		blobTypes := strings.Split(raw.excludeBlobType, ";")
		for _, blobType := range blobTypes {
			var eBlobType common.BlobType
			err := eBlobType.Parse(blobType)
			if err != nil {
				return cooked, fmt.Errorf("error parsing the exclude-blob-type %s provided with exclude-blob-type flag ", blobType)
			}
			cooked.excludeBlobType = append(cooked.excludeBlobType, eBlobType.ToAzBlobType())
		}
	}

	cooked.s2sPreserveProperties = raw.s2sPreserveProperties
	cooked.s2sGetPropertiesInBackend = raw.s2sGetPropertiesInBackend
	cooked.s2sPreserveAccessTier = raw.s2sPreserveAccessTier
	cooked.s2sSourceChangeValidation = raw.s2sSourceChangeValidation

	err = cooked.s2sInvalidMetadataHandleOption.Parse(raw.s2sInvalidMetadataHandleOption)
	if err != nil {
		return cooked, err
	}

	// parse the filter patterns
	cooked.includePatterns = raw.parsePatterns(raw.include)
	cooked.excludePatterns = raw.parsePatterns(raw.exclude)
	cooked.excludePathPatterns = raw.parsePatterns(raw.excludePath)

	if (raw.includeFileAttributes != "" || raw.excludeFileAttributes != "") && fromTo.From() != common.ELocation.Local() {
		return cooked, errors.New("cannot check file attributes on remote objects")
	}
	cooked.includeFileAttributes = raw.parsePatterns(raw.includeFileAttributes)
	cooked.excludeFileAttributes = raw.parsePatterns(raw.excludeFileAttributes)

	return cooked, nil
}

// When other commands use the copy command arguments to cook cook, set the blobType to None and validation option
// else parsing the arguments will fail.
func (raw *rawCopyCmdArgs) setMandatoryDefaults() {
	raw.blobType = common.EBlobType.None().String()
	raw.blockBlobTier = common.EBlockBlobTier.None().String()
	raw.pageBlobTier = common.EPageBlobTier.None().String()
	raw.md5ValidationOption = common.DefaultHashValidationOption.String()
	raw.s2sInvalidMetadataHandleOption = common.DefaultInvalidMetadataHandleOption.String()
}

func validatePutMd5(putMd5 bool, fromTo common.FromTo) error {
	if putMd5 && !fromTo.IsUpload() {
		return fmt.Errorf("put-md5 is set but the job is not an upload")
	}
	return nil
}

func validateMd5Option(option common.HashValidationOption, fromTo common.FromTo) error {
	hasMd5Validation := option != common.DefaultHashValidationOption
	if hasMd5Validation && !fromTo.IsDownload() {
		return fmt.Errorf("check-md5 is set but the job is not a download")
	}
	return nil
}

// represents the processed copy command input from the user
type cookedCopyCmdArgs struct {
	// from arguments
	source         string
	sourceSAS      string
	destination    string
	destinationSAS string
	fromTo         common.FromTo

	// TODO remove after refactoring
	legacyInclude map[string]int
	legacyExclude map[string]int
	// new include/exclude only apply to file names
	// implemented for remove (and sync) only
	includePatterns       []string
	includePathPatterns   []string
	excludePatterns       []string
	excludePathPatterns   []string
	includeFileAttributes []string
	excludeFileAttributes []string

	// filters from flags
	listOfFilesToCopy  []string
	listOfFilesChannel chan string // We make it a pointer so we can check if it exists w/o reading from it & tack things onto it if necessary.
	recursive          bool
	followSymlinks     bool
	withSnapshots      bool
	forceWrite         bool

	// options from flags
	blockSize uint32
	// list of blobTypes to exclude while enumerating the transfer
	excludeBlobType          []azblob.BlobType
	blobType                 common.BlobType
	blockBlobTier            common.BlockBlobTier
	pageBlobTier             common.PageBlobTier
	metadata                 string
	contentType              string
	contentEncoding          string
	contentLanguage          string
	contentDisposition       string
	cacheControl             string
	noGuessMimeType          bool
	preserveLastModifiedTime bool
	putMd5                   bool
	md5ValidationOption      common.HashValidationOption
	s2sCheckLength           bool
	background               bool
	acl                      string
	logVerbosity             common.LogLevel
	cancelFromStdin          bool
	// commandString hold the user given command which is logged to the Job log file
	commandString string

	// generated
	jobID common.JobID

	// extracted from the input
	credentialInfo common.CredentialInfo

	// variables used to calculate progress
	// intervalStartTime holds the last time value when the progress summary was fetched
	// the value of this variable is used to calculate the throughput
	// it gets updated every time the progress summary is fetched
	intervalStartTime        time.Time
	intervalBytesTransferred uint64

	// used to calculate job summary
	jobStartTime time.Time

	// this flag is set by the enumerator
	// it is useful to indicate whether we are simply waiting for the purpose of cancelling
	isEnumerationComplete bool

	// whether user wants to preserve full properties during service to service copy, the default value is true.
	// For S3 and Azure File non-single file source, as list operation doesn't return full properties of objects/files,
	// to preserve full properties AzCopy needs to send one additional request per object/file.
	s2sPreserveProperties bool
	// useful when preserveS3Properties set to true, enables get S3 objects' or Azure files' properties during s2s copy in backend, the default value is true
	s2sGetPropertiesInBackend bool
	// whether user wants to preserve access tier during service to service copy, the default value is true.
	// In some case, e.g. target is a GPv1 storage account, access tier cannot be set properly.
	// In such cases, use s2sPreserveAccessTier=false to bypass the access tier copy.
	// For more details, please refer to https://docs.microsoft.com/en-us/azure/storage/blobs/storage-blob-storage-tiers
	s2sPreserveAccessTier bool
	// whether user wants to check if source has changed after enumerating, the default value is true.
	// For S2S copy, as source is a remote resource, validating whether source has changed need additional request costs.
	s2sSourceChangeValidation bool
	// specify how user wants to handle invalid metadata.
	s2sInvalidMetadataHandleOption common.InvalidMetadataHandleOption
}

func (cca *cookedCopyCmdArgs) isRedirection() bool {
	switch cca.fromTo {
	case common.EFromTo.BlobPipe():
		fallthrough
	case common.EFromTo.PipeBlob():
		return true
	default:
		return false
	}
}

func (cca *cookedCopyCmdArgs) process() error {
	if cca.isRedirection() {
		err := cca.processRedirectionCopy()

		if err != nil {
			return err
		}

		// if no error, the operation is now complete
		glcm.Exit(nil, common.EExitCode.Success())
	}
	return cca.processCopyJobPartOrders()
}

// TODO discuss with Jeff what features should be supported by redirection, such as metadata, content-type, etc.
func (cca *cookedCopyCmdArgs) processRedirectionCopy() error {
	if cca.fromTo == common.EFromTo.PipeBlob() {
		return cca.processRedirectionUpload(cca.destination, cca.blockSize)
	} else if cca.fromTo == common.EFromTo.BlobPipe() {
		return cca.processRedirectionDownload(cca.source)
	}

	return fmt.Errorf("unsupported redirection type: %s", cca.fromTo)
}

func (cca *cookedCopyCmdArgs) processRedirectionDownload(blobUrl string) error {
	ctx := context.WithValue(context.TODO(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)

	// step 0: check the Stdout before uploading
	_, err := os.Stdout.Stat()
	if err != nil {
		return fmt.Errorf("fatal: cannot write to Stdout due to error: %s", err.Error())
	}

	// step 1: initialize pipeline
	p, err := createBlobPipeline(ctx, common.CredentialInfo{CredentialType: common.ECredentialType.Anonymous()})
	if err != nil {
		return err
	}

	// step 2: parse source url
	u, err := url.Parse(blobUrl)
	if err != nil {
		return fmt.Errorf("fatal: cannot parse source blob URL due to error: %s", err.Error())
	}

	// step 3: start download
	blobURL := azblob.NewBlobURL(*u, p)
	blobStream, err := blobURL.Download(ctx, 0, azblob.CountToEnd, azblob.BlobAccessConditions{}, false)
	if err != nil {
		return fmt.Errorf("fatal: cannot download blob due to error: %s", err.Error())
	}

	blobBody := blobStream.Body(azblob.RetryReaderOptions{MaxRetryRequests: ste.MaxRetryPerDownloadBody})
	defer blobBody.Close()

	// step 4: pipe everything into Stdout
	_, err = io.Copy(os.Stdout, blobBody)
	if err != nil {
		return fmt.Errorf("fatal: cannot download blob to Stdout due to error: %s", err.Error())
	}

	return nil
}

func (cca *cookedCopyCmdArgs) processRedirectionUpload(blobUrl string, blockSize uint32) error {
	ctx := context.WithValue(context.TODO(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)

	// if no block size is set, then use default value
	if blockSize == 0 {
		blockSize = pipingDefaultBlockSize
	}

	// step 0: initialize pipeline
	p, err := createBlobPipeline(ctx, common.CredentialInfo{CredentialType: common.ECredentialType.Anonymous()})
	if err != nil {
		return err
	}

	// step 1: parse destination url
	u, err := url.Parse(blobUrl)
	if err != nil {
		return fmt.Errorf("fatal: cannot parse destination blob URL due to error: %s", err.Error())
	}

	// step 2: leverage high-level call in Blob SDK to upload stdin in parallel
	blockBlobUrl := azblob.NewBlockBlobURL(*u, p)
	_, err = azblob.UploadStreamToBlockBlob(ctx, os.Stdin, blockBlobUrl, azblob.UploadStreamToBlockBlobOptions{
		BufferSize: int(blockSize),
		MaxBuffers: pipingUploadParallelism,
	})

	return err
}

// handles the copy command
// dispatches the job order (in parts) to the storage engine
func (cca *cookedCopyCmdArgs) processCopyJobPartOrders() (err error) {
	ctx := context.WithValue(context.TODO(), ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)

	// verifies credential type and initializes credential info.
	// Note: Currently, only one credential type is necessary for source and destination.
	// For upload&download, only one side need credential.
	// For S2S copy, as azcopy-v10 use Put*FromUrl, only one credential is needed for destination.
	if cca.credentialInfo.CredentialType, err = getCredentialType(ctx, rawFromToInfo{
		fromTo:         cca.fromTo,
		source:         cca.source,
		destination:    cca.destination,
		sourceSAS:      cca.sourceSAS,
		destinationSAS: cca.destinationSAS,
	}); err != nil {
		return err
	}

	// For OAuthToken credential, assign OAuthTokenInfo to CopyJobPartOrderRequest properly,
	// the info will be transferred to STE.
	if cca.credentialInfo.CredentialType == common.ECredentialType.OAuthToken() {
		// Message user that they are using Oauth token for authentication,
		// in case of silently using cached token without consciousness。
		glcm.Info("Using OAuth token for authentication.")

		uotm := GetUserOAuthTokenManagerInstance()
		// Get token from env var or cache.
		if tokenInfo, err := uotm.GetTokenInfo(ctx); err != nil {
			return err
		} else {
			cca.credentialInfo.OAuthTokenInfo = *tokenInfo
		}
	}

	// initialize the fields that are constant across all job part orders
	jobPartOrder := common.CopyJobPartOrderRequest{
		JobID:           cca.jobID,
		FromTo:          cca.fromTo,
		ForceWrite:      cca.forceWrite,
		Priority:        common.EJobPriority.Normal(),
		LogLevel:        cca.logVerbosity,
		Include:         cca.legacyInclude,
		Exclude:         cca.legacyExclude,
		ExcludeBlobType: cca.excludeBlobType,
		BlobAttributes: common.BlobTransferAttributes{
			BlobType:                 cca.blobType,
			BlockSizeInBytes:         cca.blockSize,
			ContentType:              cca.contentType,
			ContentEncoding:          cca.contentEncoding,
			ContentLanguage:          cca.contentLanguage,
			ContentDisposition:       cca.contentDisposition,
			CacheControl:             cca.cacheControl,
			BlockBlobTier:            cca.blockBlobTier,
			PageBlobTier:             cca.pageBlobTier,
			Metadata:                 cca.metadata,
			NoGuessMimeType:          cca.noGuessMimeType,
			PreserveLastModifiedTime: cca.preserveLastModifiedTime,
			PutMd5:                   cca.putMd5,
			MD5ValidationOption:      cca.md5ValidationOption,
		},
		// source sas is stripped from the source given by the user and it will not be stored in the part plan file.
		SourceSAS: cca.sourceSAS,

		// destination sas is stripped from the destination given by the user and it will not be stored in the part plan file.
		DestinationSAS: cca.destinationSAS,
		CommandString:  cca.commandString,
		CredentialInfo: cca.credentialInfo,
	}

	// TODO remove this copy pasted code during refactoring
	from := cca.fromTo.From()
	to := cca.fromTo.To()
	// Strip the SAS from the source and destination whenever there is SAS exists in URL.
	// Note: SAS could exists in source of S2S copy, even if the credential type is OAuth for destination.
	switch from {
	case common.ELocation.Local():
		tmpSrc, err := filepath.Abs(cca.source)
		if err != nil {
			return fmt.Errorf("couldn't get absolute path of the source location %s. Failed with errror %s", cca.source, err.Error())
		}

		jobPartOrder.SourceRoot = cleanLocalPath(getPathBeforeFirstWildcard(tmpSrc))
		cca.source = cleanLocalPath(tmpSrc)

	case common.ELocation.Benchmark():

		// TODO: is this the right place for this virtual dir code?
		//  Should it instead be destination focussed (manipulate the dest path) and/or be done in benchmark.go (problem with that
		//  is that we don'nt have the job id there...

		// Add in a unique virtual dir name, so that we know for sure we will never overwrite anything
		// Use the jobID for clarity and debuggability
		virtualDir := "benchmark-" + cca.jobID.String()

		// TODO: this seems to be necessary because the processor in init enumerator pre-pends
		//   a /.  After all refactoring is finished, is that what we still expect?  And should it prepending
		//  / or AZCOPY_PATH_SEPARATOR_STRING?
		//  If we don't prepend it here, the stripping out of SourceRoot doesn't work in addTransfer
		//  Reviewers: this seems messy and error-prone. Any thoughts?
		jobPartOrder.SourceRoot = common.AZCOPY_PATH_SEPARATOR_STRING + virtualDir

		cca.source, err = benchmarkSourceHelper{}.SetVirtualDir(cca.source, virtualDir)
		if err != nil {
			return err
		}
	case common.ELocation.Blob():
		fromUrl, err := url.Parse(cca.source)
		if err != nil {
			return fmt.Errorf("error parsing the source url %s. Failed with error %s", fromUrl.String(), err.Error())
		}
		blobParts := azblob.NewBlobURLParts(*fromUrl)
		cca.sourceSAS = blobParts.SAS.Encode()
		jobPartOrder.SourceSAS = cca.sourceSAS
		blobParts.SAS = azblob.SASQueryParameters{}
		bUrl := blobParts.URL()
		cca.source = bUrl.String()

		// set the clean source root
		bUrl.Path, _ = gCopyUtil.getRootPathWithoutWildCards(bUrl.Path)
		jobPartOrder.SourceRoot = bUrl.String()

	case common.ELocation.File():
		fromUrl, err := url.Parse(cca.source)
		if err != nil {
			return fmt.Errorf("error parsing the source url %s. Failed with error %s", fromUrl.String(), err.Error())
		}
		fileParts := azfile.NewFileURLParts(*fromUrl)
		cca.sourceSAS = fileParts.SAS.Encode()
		if cca.sourceSAS == "" {
			return fmt.Errorf("azure files only supports SAS token authentication")
		}
		jobPartOrder.SourceSAS = cca.sourceSAS
		fileParts.SAS = azfile.SASQueryParameters{}
		fUrl := fileParts.URL()
		cca.source = fUrl.String()

		// set the clean source root
		fUrl.Path, _ = gCopyUtil.getRootPathWithoutWildCards(fUrl.Path)
		jobPartOrder.SourceRoot = fUrl.String()

	case common.ELocation.BlobFS():
		fromUrl, err := url.Parse(cca.source)
		if err != nil {
			return fmt.Errorf("error parsing the source url %s. Failed with error %s", fromUrl.String(), err.Error())
		}
		bfsParts := azbfs.NewBfsURLParts(*fromUrl)
		cca.sourceSAS = bfsParts.SAS.Encode()
		jobPartOrder.SourceSAS = cca.sourceSAS
		bfsParts.SAS = azbfs.SASQueryParameters{}
		bfsUrl := bfsParts.URL()
		cca.source = bfsUrl.String() // this escapes spaces in the source

		// set the clean source root
		bfsUrl.Path, _ = gCopyUtil.getRootPathWithoutWildCards(bfsUrl.Path)
		jobPartOrder.SourceRoot = bfsUrl.String()

	case common.ELocation.S3():
		fromURL, err := url.Parse(cca.source)
		if err != nil {
			return fmt.Errorf("error parsing the source url %s. Failed with error %s", fromURL.String(), err.Error())
		}

		// S3 management console encode ' '(space) as '+', which is not supported by Azure resources.
		// To support URL from S3 managment console, azcopy decode '+' as ' '(space).
		*fromURL = common.URLExtension{URL: *fromURL}.URLWithPlusDecodedInPath()
		cca.source = fromURL.String()

		// set the clean source root
		fromURL.Path, _ = gCopyUtil.getRootPathWithoutWildCards(fromURL.Path)
		jobPartOrder.SourceRoot = fromURL.String()

	default:
		jobPartOrder.SourceRoot, _ = gCopyUtil.getRootPathWithoutWildCards(cca.source)
	}

	switch to {
	case common.ELocation.Blob():
		toUrl, err := url.Parse(cca.destination)
		if err != nil {
			return fmt.Errorf("error parsing the destination url %s. Failed with error %s", toUrl.String(), err.Error())
		}
		blobParts := azblob.NewBlobURLParts(*toUrl)
		cca.destinationSAS = blobParts.SAS.Encode()
		jobPartOrder.DestinationSAS = cca.destinationSAS
		blobParts.SAS = azblob.SASQueryParameters{}
		bUrl := blobParts.URL()
		cca.destination = bUrl.String()
	case common.ELocation.File():
		toUrl, err := url.Parse(cca.destination)
		if err != nil {
			return fmt.Errorf("error parsing the destination url %s. Failed with error %s", toUrl.String(), err.Error())
		}
		fileParts := azfile.NewFileURLParts(*toUrl)
		cca.destinationSAS = fileParts.SAS.Encode()
		if cca.destinationSAS == "" {
			return fmt.Errorf("azure files only supports SAS token authentication")
		}
		jobPartOrder.DestinationSAS = cca.destinationSAS
		fileParts.SAS = azfile.SASQueryParameters{}
		fUrl := fileParts.URL()
		cca.destination = fUrl.String()
	case common.ELocation.BlobFS():
		toUrl, err := url.Parse(cca.destination)
		if err != nil {
			return fmt.Errorf("error parsing the destination url %s. Failed with error %s", toUrl.String(), err.Error())
		}
		bfsParts := azbfs.NewBfsURLParts(*toUrl)
		cca.destinationSAS = bfsParts.SAS.Encode()
		jobPartOrder.DestinationSAS = cca.destinationSAS
		bfsParts.SAS = azbfs.SASQueryParameters{}
		bfsUrl := bfsParts.URL()
		cca.destination = bfsUrl.String() // this escapes spaces in the destination
	case common.ELocation.Local():
		cca.destination = cleanLocalPath(cca.destination)
	}

	// set the root destination after it's been cleaned
	jobPartOrder.DestinationRoot = cca.destination

	// depending on the source and destination type, we process the cp command differently
	// Create enumerator and do enumerating
	switch cca.fromTo {
	case common.EFromTo.LocalBlob(),
		common.EFromTo.LocalBlobFS(),
		common.EFromTo.LocalFile(),

		common.EFromTo.BenchmarkBlob(),
		common.EFromTo.BenchmarkBlobFS(),
		common.EFromTo.BenchmarkFile():

		var e *copyEnumerator
		e, err = cca.initEnumerator(jobPartOrder, ctx)
		if err != nil {
			return err
		}

		err = e.enumerate()
	case common.EFromTo.BlobLocal():
		e := copyDownloadBlobEnumerator(jobPartOrder)
		err = e.enumerate(cca)
	case common.EFromTo.FileLocal():
		e := copyDownloadFileEnumerator(jobPartOrder)
		err = e.enumerate(cca)
	case common.EFromTo.BlobFSLocal():
		e := copyDownloadBlobFSEnumerator(jobPartOrder)
		err = e.enumerate(cca)
	case common.EFromTo.BlobTrash(), common.EFromTo.FileTrash():
		e, createErr := newRemoveEnumerator(cca)
		if createErr != nil {
			return createErr
		}

		err = e.enumerate()

	case common.EFromTo.BlobFSTrash():
		// TODO merge with BlobTrash case
		msg, err := removeBfsResources(cca)
		if err == nil {
			glcm.Exit(func(format common.OutputFormat) string {
				return msg
			}, common.EExitCode.Success())
		}

		return err

	case common.EFromTo.BlobBlob():
		e := copyS2SMigrationBlobEnumerator{
			copyS2SMigrationEnumeratorBase: copyS2SMigrationEnumeratorBase{
				CopyJobPartOrderRequest: jobPartOrder,
			},
		}
		err = e.enumerate(cca)
	case common.EFromTo.FileBlob():
		e := copyS2SMigrationFileEnumerator{
			copyS2SMigrationEnumeratorBase: copyS2SMigrationEnumeratorBase{
				CopyJobPartOrderRequest: jobPartOrder,
			},
		}
		err = e.enumerate(cca)
	case common.EFromTo.S3Blob():
		e := copyS2SMigrationS3Enumerator{ // S3 enumerator for S2S copy.
			copyS2SMigrationEnumeratorBase: copyS2SMigrationEnumeratorBase{
				CopyJobPartOrderRequest: jobPartOrder,
			},
		}
		err = e.enumerate(cca)

	// TODO: Hide the File to Blob direction temporarily, as service support on-going.
	// case common.EFromTo.FileBlob():
	// 	e := copyFileToNEnumerator(jobPartOrder)
	// 	err = e.enumerate(cca)
	default:
		return fmt.Errorf("copy direction %v is not supported\n", cca.fromTo)
	}

	if err != nil {
		return fmt.Errorf("cannot start job due to error: %s.\n", err)
	}

	return nil
}

// wraps call to lifecycle manager to wait for the job to complete
// if blocking is specified to true, then this method will never return
// if blocking is specified to false, then another goroutine spawns and wait out the job
func (cca *cookedCopyCmdArgs) waitUntilJobCompletion(blocking bool) {
	// print initial message to indicate that the job is starting
	glcm.Init(common.GetStandardInitOutputBuilder(cca.jobID.String(), fmt.Sprintf("%s/%s.log", azcopyLogPathFolder, cca.jobID)))

	// initialize the times necessary to track progress
	cca.jobStartTime = time.Now()
	cca.intervalStartTime = time.Now()
	cca.intervalBytesTransferred = 0

	// hand over control to the lifecycle manager if blocking
	if blocking {
		glcm.InitiateProgressReporting(cca, !cca.cancelFromStdin)
		glcm.SurrenderControl()
	} else {
		// non-blocking, return after spawning a go routine to watch the job
		glcm.InitiateProgressReporting(cca, !cca.cancelFromStdin)
	}
}

func (cca *cookedCopyCmdArgs) Cancel(lcm common.LifecycleMgr) {
	// prompt for confirmation, except when:
	// 1. output is not in text format
	// 2. azcopy was spawned by another process (cancelFromStdin indicates this)
	// 3. enumeration is complete
	if !(azcopyOutputFormat != common.EOutputFormat.Text() || cca.cancelFromStdin || cca.isEnumerationComplete) {
		answer := lcm.Prompt("The source enumeration is not complete, cancelling the job at this point means it cannot be resumed. Please confirm with y/n: ")

		// read a line from stdin, if the answer is not yes, then abort cancel by returning
		if !strings.EqualFold(answer, "y") {
			return
		}
	}

	err := cookedCancelCmdArgs{jobID: cca.jobID}.process()
	if err != nil {
		lcm.Error("error occurred while cancelling the job " + cca.jobID.String() + ": " + err.Error())
	}
}

func (cca *cookedCopyCmdArgs) ReportProgressOrExit(lcm common.LifecycleMgr) {
	// fetch a job status
	var summary common.ListJobSummaryResponse
	Rpc(common.ERpcCmd.ListJobSummary(), &cca.jobID, &summary)
	jobDone := summary.JobStatus.IsJobDone()

	// if json is not desired, and job is done, then we generate a special end message to conclude the job
	duration := time.Now().Sub(cca.jobStartTime) // report the total run time of the job

	if jobDone {
		exitCode := common.EExitCode.Success()
		if summary.TransfersFailed > 0 {
			exitCode = common.EExitCode.Error()
		}

		lcm.Exit(func(format common.OutputFormat) string {
			if format == common.EOutputFormat.Json() {
				jsonOutput, err := json.Marshal(summary)
				common.PanicIfErr(err)
				return string(jsonOutput)
			} else {
				output := fmt.Sprintf(
					`

Job %s summary
Elapsed Time (Minutes): %v
Total Number Of Transfers: %v
Number of Transfers Completed: %v
Number of Transfers Failed: %v
Number of Transfers Skipped: %v
TotalBytesTransferred: %v
IOPS; ms per req: %v; %v 
Ntwk Err; Srv Busy: %.2f%%; %.2f%%
Final Job Status: %v%s
`,
					summary.JobID.String(),
					ste.ToFixed(duration.Minutes(), 4),
					summary.TotalTransfers,
					summary.TransfersCompleted,
					summary.TransfersFailed,
					summary.TransfersSkipped,
					summary.TotalBytesTransferred,
					summary.AverageIOPS, summary.AverageE2EMilliseconds, summary.NetworkErrorPercentage, summary.ServerBusyPercentage,
					summary.JobStatus,
					formatPerfAdvice(summary.PerformanceAdvice))

				jobMan, exists := ste.JobsAdmin.JobMgr(summary.JobID)
				if exists {
					jobMan.Log(pipeline.LogInfo, output)
				}
				return output
			}
		}, exitCode)
	}

	var computeThroughput = func() float64 {
		// compute the average throughput for the last time interval
		bytesInMb := float64(float64(summary.BytesOverWire-cca.intervalBytesTransferred) / float64(base10Mega))
		timeElapsed := time.Since(cca.intervalStartTime).Seconds()

		// reset the interval timer and byte count
		cca.intervalStartTime = time.Now()
		cca.intervalBytesTransferred = summary.BytesOverWire

		return common.Iffloat64(timeElapsed != 0, bytesInMb/timeElapsed, 0) * 8
	}

	glcm.Progress(func(format common.OutputFormat) string {
		if format == common.EOutputFormat.Json() {
			jsonOutput, err := json.Marshal(summary)
			common.PanicIfErr(err)
			return string(jsonOutput)
		} else {
			// if json is not needed, then we generate a message that goes nicely on the same line
			// display a scanning keyword if the job is not completely ordered
			var scanningString = " (scanning...)"
			if summary.CompleteJobOrdered {
				scanningString = ""
			}

			throughput := computeThroughput()
			throughputString := fmt.Sprintf("2-sec Throughput (Mb/s): %v", ste.ToFixed(throughput, 4))
			if throughput == 0 {
				// As there would be case when no bits sent from local, e.g. service side copy, when throughput = 0, hide it.
				throughputString = ""
			}

			// indicate whether constrained by disk or not
			perfString, diskString := getPerfDisplayText(summary.PerfStrings, summary.PerfConstraint, duration)

			return fmt.Sprintf("%.1f %%, %v Done, %v Failed, %v Pending, %v Skipped, %v Total%s, %s%s%s",
				summary.PercentComplete,
				summary.TransfersCompleted,
				summary.TransfersFailed,
				summary.TotalTransfers-(summary.TransfersCompleted+summary.TransfersFailed+summary.TransfersSkipped),
				summary.TransfersSkipped, summary.TotalTransfers, scanningString, perfString, throughputString, diskString)
		}
	})
}

func formatPerfAdvice(advice []common.PerformanceAdvice) string {
	if len(advice) == 0 {
		return ""
	}
	b := strings.Builder{}
	b.WriteString("\n\n") // two newlines to separate the perf results from everything else
	b.WriteString("Performance benchmark results: \n")
	for _, a := range advice {
		b.WriteString("\n")
		pri := "Main"
		if !a.PriorityAdvice {
			pri = "Additional"
		}
		b.WriteString(pri + " Result:\n")
		b.WriteString("  Code:   " + a.Code + "\n")
		b.WriteString("  Desc:   " + a.Title + "\n")
		b.WriteString("  Reason: " + a.Reason + "\n")
	}
	return b.String()
}

// Is disk speed looking like a constraint on throughput?  Ignore the first little-while,
// to give an (arbitrary) amount of time for things to reach steady-state.
func getPerfDisplayText(perfDiagnosticStrings []string, constraint common.PerfConstraint, durationOfJob time.Duration) (perfString string, diskString string) {
	perfString = ""
	if shouldDisplayPerfStates() {
		perfString = "[States: " + strings.Join(perfDiagnosticStrings, ", ") + "], "
	}

	haveBeenRunningLongEnoughToStabilize := durationOfJob.Seconds() > 30 // this duration is an arbitrary guestimate
	if constraint != common.EPerfConstraint.Unknown() && haveBeenRunningLongEnoughToStabilize {
		diskString = fmt.Sprintf(" (%s may be limiting speed)", constraint)
	} else {
		diskString = ""
	}
	return
}

func shouldDisplayPerfStates() bool {
	return glcm.GetEnvironmentVariable(common.EEnvironmentVariable.ShowPerfStates()) != ""
}

func isStdinPipeIn() (bool, error) {
	// check the Stdin to see if we are uploading or downloading
	info, err := os.Stdin.Stat()
	if err != nil {
		return false, fmt.Errorf("fatal: failed to read from Stdin due to error: %s", err)
	}

	// if the stdin is a named pipe, then we assume there will be data on the stdin
	// the reason for this assumption is that we do not know when will the data come in
	// it could come in right away, or come in 10 minutes later
	return info.Mode()&os.ModeNamedPipe != 0, nil
}

// TODO check file size, max is 4.75TB
func init() {
	raw := rawCopyCmdArgs{}

	// cpCmd represents the cp command
	cpCmd := &cobra.Command{
		Use:        "copy [source] [destination]",
		Aliases:    []string{"cp", "c"},
		SuggestFor: []string{"cpy", "cy", "mv"}, //TODO why does message appear twice on the console
		Short:      copyCmdShortDescription,
		Long:       copyCmdLongDescription,
		Example:    copyCmdExample,
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) == 1 { // redirection
				if stdinPipeIn, err := isStdinPipeIn(); stdinPipeIn == true {
					raw.src = pipeLocation
					raw.dst = args[0]
				} else {
					if err != nil {
						return fmt.Errorf("fatal: failed to read from Stdin due to error: %s", err)
					} else {
						raw.src = args[0]
						raw.dst = pipeLocation
					}
				}
			} else if len(args) == 2 { // normal copy
				raw.src = args[0]
				raw.dst = args[1]
			} else {
				return errors.New("wrong number of arguments, please refer to the help page on usage of this command")
			}
			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			cooked, err := raw.cook()
			if err != nil {
				glcm.Error("failed to parse user input due to error: " + err.Error())
			}

			glcm.Info("Scanning...")

			cooked.commandString = copyHandlerUtil{}.ConstructCommandStringFromArgs()
			err = cooked.process()
			if err != nil {
				glcm.Error("failed to perform copy command due to error: " + err.Error())
			}

			glcm.SurrenderControl()
		},
	}
	rootCmd.AddCommand(cpCmd)

	// filters change which files get transferred
	cpCmd.PersistentFlags().BoolVar(&raw.followSymlinks, "follow-symlinks", false, "follow symbolic links when uploading from local file system.")
	cpCmd.PersistentFlags().BoolVar(&raw.withSnapshots, "with-snapshots", false, "include the snapshots. Only valid when the source is blobs.")
	cpCmd.PersistentFlags().StringVar(&raw.legacyInclude, "include-pattern", "", "only include these files when copying. "+
		"Support use of *. Files should be separated with ';'.")
	cpCmd.PersistentFlags().StringVar(&raw.includePath, "include-path", "", "only include these paths when copying. "+
		"Does not support using wildcards. Checks relative path prefix. ex. myFolder;myFolder/subDirName/file.pdf")
	cpCmd.PersistentFlags().StringVar(&raw.excludePath, "exclude-path", "", "exclude these paths when copying. "+
		"Does not support using wildcards. Checks relative path prefix. ex. myFolder;myFolder/subDirName/file.pdf")
	// This flag is implemented only for Storage Explorer.
	cpCmd.PersistentFlags().StringVar(&raw.listOfFilesToCopy, "list-of-files", "", "defines the location of json which has the list of only files to be copied")
	cpCmd.PersistentFlags().StringVar(&raw.legacyExclude, "exclude-pattern", "", "exclude these files when copying. Support use of *.")
	cpCmd.PersistentFlags().BoolVar(&raw.forceWrite, "overwrite", true, "overwrite the conflicting files/blobs at the destination if this flag is set to true.")
	cpCmd.PersistentFlags().BoolVar(&raw.recursive, "recursive", false, "look into sub-directories recursively when uploading from local file system.")
	cpCmd.PersistentFlags().StringVar(&raw.fromTo, "from-to", "", "optionally specifies the source destination combination. For Example: LocalBlob, BlobLocal, LocalBlobFS.")
	cpCmd.PersistentFlags().StringVar(&raw.excludeBlobType, "exclude-blob-type", "", "optionally specifies the type of blob (BlockBlob/ PageBlob/ AppendBlob) to exclude when copying blobs from Container / Account. Use of "+
		"this flag is not applicable for copying data from non azure-service to service. More than one blob should be separated by ';' ")
	// options change how the transfers are performed
	cpCmd.PersistentFlags().Float64Var(&raw.blockSizeMB, "block-size-mb", 0, "use this block size (specified in MiB) when uploading to/downloading from Azure Storage. Default is automatically calculated based on file size. Decimal fractions are allowed - e.g. 0.25")
	cpCmd.PersistentFlags().StringVar(&raw.logVerbosity, "log-level", "INFO", "define the log verbosity for the log file, available levels: INFO(all requests/responses), WARNING(slow responses), ERROR(only failed requests), and NONE(no output logs).")
	cpCmd.PersistentFlags().StringVar(&raw.blobType, "blob-type", "Detect", "defines the type of blob at the destination. This is used in case of upload / account to account copy. Use --blob-type detect for auto-detection.")
	cpCmd.PersistentFlags().StringVar(&raw.blockBlobTier, "block-blob-tier", "None", "upload block blob to Azure Storage using this blob tier.")
	cpCmd.PersistentFlags().StringVar(&raw.pageBlobTier, "page-blob-tier", "None", "upload page blob to Azure Storage using this blob tier.")
	cpCmd.PersistentFlags().StringVar(&raw.metadata, "metadata", "", "upload to Azure Storage with these key-value pairs as metadata.")
	cpCmd.PersistentFlags().StringVar(&raw.contentType, "content-type", "", "specifies content type of the file. Implies no-guess-mime-type. Returned on download.")
	cpCmd.PersistentFlags().StringVar(&raw.contentEncoding, "content-encoding", "", "set the content-encoding header. Returned on download.")
	cpCmd.PersistentFlags().StringVar(&raw.contentDisposition, "content-disposition", "", "set the content-disposition header. Returned on download.")
	cpCmd.PersistentFlags().StringVar(&raw.contentLanguage, "content-language", "", "set the content-language header. Returned on download.")
	cpCmd.PersistentFlags().StringVar(&raw.cacheControl, "cache-control", "", "set the cache-control header. Returned on download.")
	cpCmd.PersistentFlags().BoolVar(&raw.noGuessMimeType, "no-guess-mime-type", false, "prevents AzCopy from detecting the content-type based on the extension/content of the file.")
	cpCmd.PersistentFlags().BoolVar(&raw.preserveLastModifiedTime, "preserve-last-modified-time", false, "only available when destination is file system.")
	cpCmd.PersistentFlags().BoolVar(&raw.putMd5, "put-md5", false, "create an MD5 hash of each file, and save the hash as the Content-MD5 property of the destination blob/file. (By default the hash is NOT created.) Only available when uploading.")
	cpCmd.PersistentFlags().StringVar(&raw.md5ValidationOption, "check-md5", common.DefaultHashValidationOption.String(), "specifies how strictly MD5 hashes should be validated when downloading. Only available when downloading. Available options: NoCheck, LogOnly, FailIfDifferent, FailIfDifferentOrMissing.")

	cpCmd.PersistentFlags().BoolVar(&raw.cancelFromStdin, "cancel-from-stdin", false, "true if user wants to cancel the process by passing 'cancel' "+
		"to the standard input. This is mostly used when the application is spawned by another process.")
	cpCmd.PersistentFlags().BoolVar(&raw.background, "background-op", false, "true if user has to perform the operations as a background operation.")
	cpCmd.PersistentFlags().StringVar(&raw.acl, "acl", "", "Access conditions to be used when uploading/downloading from Azure Storage.")

	cpCmd.PersistentFlags().BoolVar(&raw.s2sCheckLength, "s2s-check-length", true, "Check the length of a file transferred S2S after the transfer. If there is a mismatch, fail the transfer.")
	cpCmd.PersistentFlags().BoolVar(&raw.s2sPreserveProperties, "s2s-preserve-properties", true, "preserve full properties during service to service copy. "+
		"For S3 and Azure File non-single file source, as list operation doesn't return full properties of objects/files, to preserve full properties AzCopy needs to send one additional request per object/file.")
	cpCmd.PersistentFlags().BoolVar(&raw.s2sPreserveAccessTier, "s2s-preserve-access-tier", true, "preserve access tier during service to service copy. "+
		"please refer to https://docs.microsoft.com/en-us/azure/storage/blobs/storage-blob-storage-tiers to ensure destination storage account supports setting access tier. "+
		"In the cases that setting access tier is not supported, please use s2sPreserveAccessTier=false to bypass copying access tier. ")
	cpCmd.PersistentFlags().BoolVar(&raw.s2sSourceChangeValidation, "s2s-detect-source-changed", false, "check if source has changed after enumerating. "+
		"For S2S copy, as source is a remote resource, validating whether source has changed need additional request costs. ")
	cpCmd.PersistentFlags().StringVar(&raw.s2sInvalidMetadataHandleOption, "s2s-handle-invalid-metadata", common.DefaultInvalidMetadataHandleOption.String(), "specifies how invalid metadata keys are handled. AvailabeOptions: ExcludeIfInvalid, FailIfInvalid, RenameIfInvalid.")

	// s2sGetPropertiesInBackend is an optional flag for controlling whether S3 object's or Azure file's full properties are get during enumerating in frontend or
	// right before transferring in ste(backend).
	// The traditional behavior of all existing enumerator is to get full properties during enumerating(more specifically listing),
	// while this could cause big performance issue for S3 and Azure file, where listing doesn't return full properties,
	// and enumerating logic do fetching properties sequentially!
	// To achieve better performance and at same time have good control for overall go routine numbers, getting property in ste is introduced,
	// so properties can be get in parallel, at same time no additional go routines are created for this specific job.
	// The usage of this hidden flag is to provide fallback to traditional behavior, when service supports returning full properties during list.
	cpCmd.PersistentFlags().BoolVar(&raw.s2sGetPropertiesInBackend, "s2s-get-properties-in-backend", true, "get S3 objects' or Azure files' properties in backend. ")

	// not implemented
	cpCmd.PersistentFlags().MarkHidden("acl")

	// permanently hidden
	// Hide the list-of-files flag since it is implemented only for Storage Explorer.
	cpCmd.PersistentFlags().MarkHidden("list-of-files")
	cpCmd.PersistentFlags().MarkHidden("with-snapshots")
	cpCmd.PersistentFlags().MarkHidden("background-op")
	cpCmd.PersistentFlags().MarkHidden("cancel-from-stdin")
	cpCmd.PersistentFlags().MarkHidden("s2s-get-properties-in-backend")
	cpCmd.PersistentFlags().MarkHidden("with-snapshots") // TODO this flag is not supported right now

	// Hide the flush-threshold flag since it is implemented only for CI.
	cpCmd.PersistentFlags().Uint32Var(&ste.ADLSFlushThreshold, "flush-threshold", 7500, "Adjust the number of blocks to flush at once on ADLS gen 2")
	cpCmd.PersistentFlags().MarkHidden("flush-threshold")
}
