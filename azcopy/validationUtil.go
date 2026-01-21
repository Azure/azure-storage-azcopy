// Copyright © 2025 Microsoft <wastore@microsoft.com>
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

package azcopy

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"reflect"
	"regexp"
	"runtime"
	"strings"
	"sync"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/file"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/traverser"
	"github.com/JeffreyRichter/enum/enum"
)

const (
	FromToHelpFormat                          = "Specified to nudge AzCopy when resource detection may not work (e.g. piping/emulator/azure stack); Valid FromTo are pairs of Source-Destination words (e.g. BlobLocal, BlobBlob) that specify the source and destination resource types. All valid FromTos are: %s"
	PipeLocation                              = "~pipe~"
	PreservePermissionsFlag                   = "preserve-permissions"
	PreserveInfoFlag                          = "preserve-info"
	PreservePOSIXPropertiesIncompatibilityMsg = "to use the --preserve-posix-properties flag, both the source and destination must be POSIX-aware. Valid combinations are: Linux -> Blob, Blob -> Linux, or Blob -> Blob"
	POSIXStyleMisuse                          = "to use --posix-properties-style flag, it has to be used with preserve-posix-properties. Please include this preserve flag in your AzCopy command"
	DstShareDoesNotExists                     = "the destination file share does not exist; please create it manually with the required quota and settings before running the copy —refer to https://learn.microsoft.com/en-us/azure/storage/files/storage-how-to-create-file-share?tabs=azure-portal for SMB or https://learn.microsoft.com/en-us/azure/storage/files/storage-files-quick-create-use-linux for NFS."
)

var FromToHelp = func() string {
	validFromTos := ""

	isSafeToOutput := func(loc common.Location) bool {
		switch loc {
		case common.ELocation.Benchmark(),
			common.ELocation.None(),
			common.ELocation.Unknown():
			return false
		default:
			return true
		}
	}

	enum.GetSymbols(reflect.TypeOf(common.EFromTo), func(enumSymbolName string, enumSymbolValue interface{}) (stop bool) {
		fromTo := enumSymbolValue.(common.FromTo)

		if isSafeToOutput(fromTo.From()) && isSafeToOutput(fromTo.To()) {
			fromtoStr := fromTo.String()
			if fromTo.String() == common.EFromTo.LocalFile().String() {
				fromtoStr = "LocalFileSMB"
			} else if fromTo.String() == common.EFromTo.FileLocal().String() {
				fromtoStr = "FileSMBLocal"
			} else if fromTo.String() == common.EFromTo.FileFile().String() {
				fromtoStr = "FileSMBFileSMB"
			}
			validFromTos += fromtoStr + ", "
		}
		return false
	})

	return fmt.Sprintf(FromToHelpFormat, strings.TrimSuffix(validFromTos, ", "))
}()

var FromToHelpText = FromToHelp

func InferAndValidateFromTo(src, dst string, userSpecifiedFromTo string) (common.FromTo, error) {
	if userSpecifiedFromTo == "" {
		inferredFromTo := inferFromTo(src, dst)

		// If user didn't explicitly specify FromTo, use what was inferred (if possible)
		if inferredFromTo == common.EFromTo.Unknown() {
			return common.EFromTo.Unknown(), fmt.Errorf("the inferred source/destination combination could not be identified, or is currently not supported")
		}
		return inferredFromTo, nil
	}

	// User explicitly specified FromTo, therefore, we should respect what they specified.
	var userFromTo common.FromTo
	err := userFromTo.Parse(userSpecifiedFromTo)
	if err != nil {
		return common.EFromTo.Unknown(), fmt.Errorf("invalid --from-to value specified: %q. "+FromToHelpText, userSpecifiedFromTo)

	}

	// Normalize FileSMB cases to corresponding File cases.
	// This remapping ensures that we can handle FileSMB scenarios without requiring
	// widespread code changes in AzCopy for the time being.
	if userFromTo == common.EFromTo.LocalFileSMB() {
		userFromTo = common.EFromTo.LocalFile()
	} else if userFromTo == common.EFromTo.FileSMBLocal() {
		userFromTo = common.EFromTo.FileLocal()
	} else if userFromTo == common.EFromTo.FileSMBFileSMB() {
		userFromTo = common.EFromTo.FileFile()
	} else if userFromTo == common.EFromTo.FileSMBBlob() {
		userFromTo = common.EFromTo.FileBlob()
	} else if userFromTo == common.EFromTo.BlobFileSMB() {
		userFromTo = common.EFromTo.BlobFile()
	} else if userFromTo == common.EFromTo.FileSMBPipe() {
		userFromTo = common.EFromTo.FilePipe()
	} else if userFromTo == common.EFromTo.PipeFileSMB() {
		userFromTo = common.EFromTo.PipeFile()
	} else if userFromTo == common.EFromTo.FileSMBTrash() {
		userFromTo = common.EFromTo.FileTrash()
	} else if userFromTo == common.EFromTo.FileSMBBlobFS() {
		userFromTo = common.EFromTo.FileBlobFS()
	} else if userFromTo == common.EFromTo.BlobFSFileSMB() {
		userFromTo = common.EFromTo.BlobFSFile()
	} else if userFromTo == common.EFromTo.FileSMBTrash() {
		userFromTo = common.EFromTo.FileTrash()
	}

	return userFromTo, nil
}

func inferFromTo(src, dst string) common.FromTo {
	// Try to infer the 1st argument
	srcLocation := InferArgumentLocation(src)
	if srcLocation == srcLocation.Unknown() {
		common.GetLifecycleMgr().Info("Cannot infer source location of " +
			common.URLStringExtension(src).RedactSecretQueryParamForLogging() +
			". Please specify the --from-to switch. " + FromToHelpText)
		return common.EFromTo.Unknown()
	}

	dstLocation := InferArgumentLocation(dst)
	if dstLocation == dstLocation.Unknown() {
		common.GetLifecycleMgr().Info("Cannot infer destination location of " +
			common.URLStringExtension(dst).RedactSecretQueryParamForLogging() +
			". Please specify the --from-to switch. " + FromToHelpText)
		return common.EFromTo.Unknown()
	}

	out := common.EFromTo.Unknown() // Check that the intended FromTo is in the list of valid FromTos; if it's not, return Unknown as usual and warn the user.
	intent := (common.FromTo(srcLocation) << 8) | common.FromTo(dstLocation)
	enum.GetSymbols(reflect.TypeOf(common.EFromTo), func(enumSymbolName string, enumSymbolValue interface{}) (stop bool) { // find if our fromto is a valid option
		fromTo := enumSymbolValue.(common.FromTo)
		// none/unknown will never appear as valid outputs of the above functions
		// If it's our intended fromto, we're good.
		if fromTo == intent {
			out = intent
			return true
		}

		return false
	})

	if out != common.EFromTo.Unknown() {
		return out
	}

	common.GetLifecycleMgr().Info("The parameters you supplied were " +
		"Source: '" + common.URLStringExtension(src).RedactSecretQueryParamForLogging() + "' of type " + srcLocation.String() +
		", and Destination: '" + common.URLStringExtension(dst).RedactSecretQueryParamForLogging() + "' of type " + dstLocation.String())
	common.GetLifecycleMgr().Info("Based on the parameters supplied, a valid source-destination combination could not " +
		"automatically be found. Please check the parameters you supplied.  If they are correct, please " +
		"specify an exact source and destination type using the --from-to switch. " + FromToHelpText)

	return out
}

func InferArgumentLocation(arg string) common.Location {
	if arg == PipeLocation {
		return common.ELocation.Pipe()
	}
	if StartsWith(arg, "http") {
		// Let's try to parse the argument as a URL
		u, err := url.Parse(arg)
		// NOTE: sometimes, a local path can also be parsed as a url. To avoid thinking it's a URL, check Scheme, Host, and Path
		if err == nil && u.Scheme != "" && u.Host != "" {
			// Is the argument a URL to blob storage?
			switch host := strings.ToLower(u.Host); true {
			// Azure Stack does not have the core.windows.net
			case strings.Contains(host, ".blob"):
				return common.ELocation.Blob()
			case strings.Contains(host, ".file"):
				return common.ELocation.File()
			case strings.Contains(host, ".dfs"):
				return common.ELocation.BlobFS()
			case strings.Contains(host, traverser.BenchmarkSourceHost):
				return common.ELocation.Benchmark()
				// enable targeting an emulator/stack
			case IPv4Regex.MatchString(host):
				return common.ELocation.Unknown()
			}

			if common.IsS3URL(*u) {
				return common.ELocation.S3()
			}

			if common.IsGCPURL(*u) {
				return common.ELocation.GCP()
			}

			// If none of the above conditions match, return Unknown
			return common.ELocation.Unknown()
		}
	}

	return common.ELocation.Local()
}

var IPv4Regex = regexp.MustCompile(`\d+\.\d+\.\d+\.\d+`) // simple regex

func ValidateForceIfReadOnly(toForce bool, fromTo common.FromTo) error {
	targetIsFiles := (fromTo.To().IsFile()) ||
		fromTo == common.EFromTo.FileTrash()
	targetIsWindowsFS := fromTo.To() == common.ELocation.Local() &&
		runtime.GOOS == "windows"
	targetIsOK := targetIsFiles || targetIsWindowsFS
	if toForce && !targetIsOK {
		return errors.New("force-if-read-only is only supported when the target is Azure Files or a Windows file system")
	}
	return nil
}

func ValidatePutMd5(putMd5 bool, fromTo common.FromTo) error {
	// In case of S2S transfers, log info message to inform the users that MD5 check doesn't work for S2S Transfers.
	// This is because we cannot calculate MD5 hash of the data stored at a remote locations.
	if putMd5 && fromTo.IsS2S() {
		common.GetLifecycleMgr().Info(" --put-md5 flag to check data consistency between source and destination is not applicable for S2S Transfers (i.e. When both the source and the destination are remote). AzCopy cannot compute MD5 hash of data stored at remote location.")
	}
	return nil
}

func ValidateMd5Option(option common.HashValidationOption, fromTo common.FromTo) error {
	hasMd5Validation := option != common.DefaultHashValidationOption
	if hasMd5Validation && !fromTo.IsDownload() {
		return fmt.Errorf("check-md5 is set but the job is not a download")
	}
	return nil
}

var nfsPermPreserveXfers = map[common.FromTo]bool{
	common.EFromTo.LocalFileNFS():   true,
	common.EFromTo.FileNFSLocal():   true,
	common.EFromTo.FileNFSFileNFS(): true,
	common.EFromTo.FileNFSFileSMB(): true,
	common.EFromTo.FileSMBFileNFS(): true,
}

func validatePreserveNFSPropertyOption(toPreserve bool, fromTo common.FromTo, flagName string) error {
	// preserverInfo will be true by default for NFS-aware locations unless specified false.
	// 1. Upload (Linux -> Azure File)
	// 2. Download (Azure File -> Linux)
	// 3. S2S (Azure File -> Azure File)

	if toPreserve {
		// The user cannot preserve permissions between SMB->NFS or NFS->SMB transfers.
		if flagName == PreservePermissionsFlag && (fromTo == common.EFromTo.FileNFSFileSMB() || fromTo == common.EFromTo.FileSMBFileNFS()) {
			return fmt.Errorf("--preserve-permissions flag is not supported for cross-protocol transfers (i.e. SMB->NFS, NFS->SMB). Please remove this flag and try again.")
		} else if !nfsPermPreserveXfers[fromTo] {
			return fmt.Errorf("%s is set but the job is not between %s-aware resources", flagName, common.Iff(flagName == PreserveInfoFlag, "permission", "NFS"))
		} else if (fromTo.IsUpload() || fromTo.IsDownload()) && runtime.GOOS != "linux" {
			return fmt.Errorf("%s is set but persistence for up/downloads is supported only in Linux", flagName)
		}
	}
	return nil
}

var smbPermPreserveXfers = map[common.FromTo]bool{
	common.EFromTo.LocalFile():      true,
	common.EFromTo.FileLocal():      true,
	common.EFromTo.FileFile():       true,
	common.EFromTo.LocalFileSMB():   true,
	common.EFromTo.FileSMBLocal():   true,
	common.EFromTo.FileSMBFileSMB(): true,
}

func validatePreserveSMBPropertyOption(toPreserve bool, fromTo common.FromTo, flagName string) error {
	// preserverInfo will be true by default for SMB-aware locations unless specified false.
	// 1. Upload (Windows/Linux -> Azure File)
	// 2. Download (Azure File -> Windows/Linux)
	// 3. S2S (Azure File -> Azure File)
	if toPreserve {
		if flagName == PreservePermissionsFlag &&
			(fromTo == common.EFromTo.BlobBlob() || fromTo == common.EFromTo.BlobFSBlob() ||
				fromTo == common.EFromTo.BlobBlobFS() || fromTo == common.EFromTo.BlobFSBlobFS()) {
			// the user probably knows what they're doing if they're trying to persist permissions between blob-type endpoints.
			return nil
		} else if !smbPermPreserveXfers[fromTo] {
			return fmt.Errorf("%s is set but the job is not between %s-aware resources", flagName, common.Iff(flagName == PreservePermissionsFlag, "permission", "SMB"))
		} else if (fromTo.IsUpload() || fromTo.IsDownload()) &&
			runtime.GOOS != "windows" && runtime.GOOS != "linux" {
			return fmt.Errorf("%s is set but persistence for up/downloads is supported only in Windows and Linux", flagName)
		}
	}
	return nil
}

func AreBothLocationsNFSAware(fromTo common.FromTo) bool {
	// 1. Upload (Linux -> Azure File)
	// 2. Download (Azure File -> Linux)
	// 3. S2S (Azure File -> Azure File) (Works on Windows,Linux,Mac)

	var s2sNFSXfers = map[common.FromTo]bool{
		common.EFromTo.FileNFSFileNFS(): true,
		common.EFromTo.FileNFSFileSMB(): true,
		common.EFromTo.FileSMBFileNFS(): true,
	}

	if (runtime.GOOS == "linux") &&
		(fromTo == common.EFromTo.LocalFileNFS() || fromTo == common.EFromTo.FileNFSLocal()) {
		return true
	} else if s2sNFSXfers[fromTo] {
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

// GetPreserveInfoDefault returns the default value for the 'preserve-info' flag
// based on the operating system and the copy type (NFS or SMB).
// The default value is:
// - true if it's an NFS copy on Linux or share to share copy on windows or mac and an SMB copy on Windows.
// - false otherwise.
//
// This default behavior ensures that file preservation logic is aligned with the OS and copy type.
func GetPreserveInfoDefault(fromTo common.FromTo) bool {
	// For Linux systems, if it's an NFS copy, we set the default value of preserveInfo to true.
	// For Windows systems, if it's an SMB copy, we set the default value of preserveInfo to true.
	// These default values are important to set here for the logic of file preservation based on the system and copy type.
	return (AreBothLocationsNFSAware(fromTo)) ||
		(runtime.GOOS == "windows" && AreBothLocationsSMBAware(fromTo))
}

// performNFSSpecificValidation performs validation specific to NFS (Network File System) configurations
// for a synchronization command. It checks NFS-related flags and settings and ensures that the necessary
// properties are set correctly for NFS copy operations.
//
// The function checks the following:
//   - Validates the "preserve-info" flag to ensure it is set correctly for NFS-aware locations.
//   - Validates the "preserve-permissions" flag, ensuring that user input is correct and provides feedback
//     if the flag is set to false and NFS info is being preserved.
//   - Ensures that both source and destination locations are NFS-aware for relevant operations.
//
// Returns:
// - An error if any validation fails, otherwise nil indicating successful validation.
func PerformNFSSpecificValidation(fromTo common.FromTo,
	preservePermissions common.PreservePermissionsOption,
	preserveInfo bool,
	hardlinkHandling *common.HardlinkHandlingType,
	symlinkHandling common.SymlinkHandlingType) (err error) {

	// check for unsupported NFS behavior
	if isUnsupported, err := isUnsupportedPlatformForNFS(fromTo); isUnsupported {
		return err
	}

	if err = validatePreserveNFSPropertyOption(preserveInfo,
		fromTo,
		PreserveInfoFlag); err != nil {
		return err
	}
	if err = validatePreserveNFSPropertyOption(preservePermissions.IsTruthy(),
		fromTo,
		PreservePermissionsFlag); err != nil {
		return err
	}
	// TODO: Add this check in Phase-3 which targets to support hardlinks for NFS copy.
	// if err = validateAndAdjustHardlinksFlag(hardlinkHandling, fromTo); err != nil {
	// 	return err
	// }

	if err = validateSymlinkFlag(symlinkHandling == common.ESymlinkHandlingType.Follow(),
		fromTo); err != nil {
		return err
	}
	return nil
}

func PerformSMBSpecificValidation(fromTo common.FromTo,
	preservePermissions common.PreservePermissionsOption,
	preserveInfo bool,
	preservePOSIXProperties bool,
	posixStyle common.PosixPropertiesStyle) (err error) {

	if err = validatePreserveSMBPropertyOption(preserveInfo,
		fromTo,
		PreserveInfoFlag); err != nil {
		return err
	}
	if preservePOSIXProperties && !areBothLocationsPOSIXAware(fromTo) {
		return errors.New(PreservePOSIXPropertiesIncompatibilityMsg)
	}

	if posixStyle != common.StandardPosixPropertiesStyle && !preservePOSIXProperties {
		return errors.New(POSIXStyleMisuse)
	}
	if err = validatePreserveSMBPropertyOption(preservePermissions.IsTruthy(),
		fromTo,
		PreservePermissionsFlag); err != nil {
		return err
	}

	// TODO: Add this check in Phase-3 which targets to support hardlinks for NFS copy.
	// if err = validateAndAdjustHardlinksFlag(hardlinkHandling, fromTo); err != nil {
	// 	return err
	// }
	return nil
}

// validateSymlinkFlag checks if the --follow-symlink flag is valid for uploading from local filesystem.
func validateSymlinkFlag(followSymlinks bool, fromTo common.FromTo) error {

	if followSymlinks {
		if fromTo.From() != common.ELocation.Local() {
			return fmt.Errorf("The '--follow-symlink' flag is only applicable when uploading from local filesystem.")
		}
	}
	return nil
}

func isUnsupportedPlatformForNFS(fromTo common.FromTo) (bool, error) {
	// upload and download is not supported for NFS on non-linux systems
	if (fromTo.IsUpload() || fromTo.IsDownload()) && runtime.GOOS != "linux" {
		op := "operation"
		if fromTo.IsUpload() {
			op = "upload"
		} else if fromTo.IsDownload() {
			op = "download"
		}
		return true, fmt.Errorf(
			"NFS %s is not supported on %s. This functionality is only available on Linux.",
			op,
			runtime.GOOS,
		)
	}
	return false, nil
}

func validateShareProtocolCompatibility(
	ctx context.Context,
	resource common.ResourceString,
	serviceClient *common.ServiceClient,
	isSource bool,
	protocol common.Location,
	fromTo common.FromTo,
) error {

	// We can ignore the error if we fail to get the share properties.
	shareProtocol, _ := getShareProtocolType(ctx, serviceClient, resource, protocol)

	if shareProtocol == common.ELocation.File() {
		if isSource && fromTo.From() != common.ELocation.File() {
			return errors.New("the source share has SMB protocol enabled. " +
				"To copy from a SMB share, use the appropriate --from-to flag value")
		}
		if !isSource && fromTo.To() != common.ELocation.File() {
			return errors.New("the destination share has NFS protocol enabled. " +
				"To copy to a NFS share, use the appropriate --from-to flag value")
		}
	}

	if shareProtocol == common.ELocation.FileNFS() {
		if isSource && fromTo.From() != common.ELocation.FileNFS() {
			return errors.New("the source share has NFS protocol enabled. " +
				"To copy from a NFS share, use the appropriate --from-to flag value")
		}
		if !isSource && fromTo.To() != common.ELocation.FileNFS() {
			return errors.New("the destination share has NFS protocol enabled. " +
				"To copy to a NFS share, use the appropriate --from-to flag value")
		}
	}
	return nil
}

// getShareProtocolType returns "SMB", "NFS", or "UNKNOWN" based on the share's enabled protocols.
// If retrieval fails, it logs a warning and returns the fallback givenValue ("SMB" or "NFS").
func getShareProtocolType(
	ctx context.Context,
	serviceClient *common.ServiceClient,
	resource common.ResourceString,
	givenValue common.Location,
) (common.Location, error) {

	fileURLParts, err := file.ParseURL(resource.Value)
	if err != nil {
		return common.ELocation.Unknown(), fmt.Errorf("failed to parse resource URL: %w", err)
	}
	shareName := fileURLParts.ShareName

	fileServiceClient, err := serviceClient.FileServiceClient()
	if err != nil {
		return common.ELocation.Unknown(), fmt.Errorf("failed to create file service client: %w", err)
	}

	shareClient := fileServiceClient.NewShareClient(shareName)
	properties, err := shareClient.GetProperties(ctx, nil)
	if err != nil {
		common.GetLifecycleMgr().Info(fmt.Sprintf("Warning: Failed to fetch share properties for '%s'. Assuming the share uses '%s' protocol based on --from-to flag.", shareName, givenValue))
		return givenValue, err
	}

	if properties.EnabledProtocols == nil || *properties.EnabledProtocols == "SMB" {
		return common.ELocation.File(), nil // Default assumption
	}

	return common.ELocation.FileNFS(), nil
}

// Protocol compatibility validation for SMB and NFS transfers
func ValidateProtocolCompatibility(ctx context.Context, fromTo common.FromTo, src, dst common.ResourceString, srcClient, dstClient *common.ServiceClient) error {

	getUploadDownloadProtocol := func(fromTo common.FromTo) common.Location {
		switch fromTo {
		case common.EFromTo.LocalFile(), common.EFromTo.FileLocal():
			return common.ELocation.File()
		case common.EFromTo.LocalFileNFS(), common.EFromTo.FileNFSLocal():
			return common.ELocation.FileNFS()
		default:
			return common.ELocation.Unknown()
		}
	}

	var srcProtocol, dstProtocol common.Location

	// S2S Transfers
	if fromTo.IsS2S() {
		switch fromTo {
		case common.EFromTo.FileFile():
			srcProtocol, dstProtocol = common.ELocation.File(), common.ELocation.File()
		case common.EFromTo.FileNFSFileNFS():
			srcProtocol, dstProtocol = common.ELocation.FileNFS(), common.ELocation.FileNFS()
		case common.EFromTo.FileNFSFileSMB():
			srcProtocol, dstProtocol = common.ELocation.FileNFS(), common.ELocation.File()
		case common.EFromTo.FileSMBFileNFS():
			srcProtocol, dstProtocol = common.ELocation.File(), common.ELocation.FileNFS()
		}

		// Validate both source and destination
		if err := validateShareProtocolCompatibility(ctx, src, srcClient, true, srcProtocol, fromTo); err != nil {
			return err
		}
		return validateShareProtocolCompatibility(ctx, dst, dstClient, false, dstProtocol, fromTo)
	}

	// Uploads to File Shares
	if fromTo.IsUpload() {
		dstProtocol = getUploadDownloadProtocol(fromTo)
		return validateShareProtocolCompatibility(ctx, dst, dstClient, false, dstProtocol, fromTo)
	}

	// Downloads from File Shares
	if fromTo.IsDownload() {
		srcProtocol = getUploadDownloadProtocol(fromTo)
		return validateShareProtocolCompatibility(ctx, src, srcClient, true, srcProtocol, fromTo)
	}

	return nil
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

func ValidateSymlinkHandlingMode(symlinkHandling common.SymlinkHandlingType, fromTo common.FromTo) error {
	if symlinkHandling.Preserve() {
		switch fromTo {
		case common.EFromTo.LocalBlob(), common.EFromTo.BlobLocal(), common.EFromTo.BlobFSLocal(), common.EFromTo.LocalBlobFS():
			return nil // Fine on all OSes that support symlink via the OS package. (Win, MacOS, and Linux do, and that's what we officially support.)
		case common.EFromTo.BlobBlob(), common.EFromTo.BlobFSBlobFS(), common.EFromTo.BlobBlobFS(), common.EFromTo.BlobFSBlob():
			return nil // Blob->Blob doesn't involve any local requirements
		case common.EFromTo.LocalFileNFS(), common.EFromTo.FileNFSLocal(), common.EFromTo.FileNFSFileNFS():
			return nil // for NFS related transfers symlink preservation is supported.
		default:
			return fmt.Errorf("flag --%s can only be used on Blob<->Blob, Local<->Blob, Local<->FileNFS, FileNFS<->FileNFS", common.PreserveSymlinkFlagName)
		}
	}

	return nil // other older symlink handling modes can work on all OSes
}

func WarnIfAnyHasWildcard(paramName string, value []string) {
	anyOncer := &sync.Once{}
	for _, v := range value {
		WarnIfHasWildcard(anyOncer, paramName, v)
	}
}

func WarnIfHasWildcard(oncer *sync.Once, paramName string, value string) {
	if strings.Contains(value, "*") || strings.Contains(value, "?") {
		oncer.Do(func() {
			common.GetLifecycleMgr().Warn(fmt.Sprintf("*** Warning *** The %s parameter does not support wildcards. The wildcard "+
				"character provided will be interpreted literally and will not have any wildcard effect. To use wildcards "+
				"(in filenames only, not paths) use include-pattern or exclude-pattern", paramName))
		})
	}
}

func ValidatePreserveOwner(preserve bool, fromTo common.FromTo) error {
	if fromTo.IsDownload() {
		return nil // it can be used in downloads
	}
	if preserve != common.PreserveOwnerDefault {
		return fmt.Errorf("flag --%s can only be used on downloads", common.PreserveOwnerFlagName)
	}
	return nil
}

func ValidateBackupMode(backupMode bool, fromTo common.FromTo) error {
	if !backupMode {
		return nil
	}
	if runtime.GOOS != "windows" {
		return errors.New(common.BackupModeFlagName + " mode is only supported on Windows")
	}
	if fromTo.IsUpload() || fromTo.IsDownload() {
		return nil
	} else {
		return errors.New(common.BackupModeFlagName + " mode is only supported for uploads and downloads")
	}
}

// Valid tag key and value characters include:
// 1. Lowercase and uppercase letters (a-z, A-Z)
// 2. Digits (0-9)
// 3. A space ( )
// 4. Plus (+), minus (-), period (.), solidus (/), colon (:), equals (=), and underscore (_)
func isValidBlobTagsKeyValue(keyVal string) bool {
	for _, c := range keyVal {
		if !((c >= '0' && c <= '9') || (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || c == ' ' || c == '+' ||
			c == '-' || c == '.' || c == '/' || c == ':' || c == '=' || c == '_') {
			return false
		}
	}
	return true
}

// ValidateBlobTagsKeyValue
// The tag set may contain at most 10 tags. Tag keys and values are case sensitive.
// Tag keys must be between 1 and 128 characters, and tag values must be between 0 and 256 characters.
func ValidateBlobTagsKeyValue(bt common.BlobTags) error {
	if len(bt) > 10 {
		return errors.New("at-most 10 tags can be associated with a blob")
	}
	for k, v := range bt {
		key, err := url.QueryUnescape(k)
		if err != nil {
			return err
		}
		value, err := url.QueryUnescape(v)
		if err != nil {
			return err
		}

		if key == "" || len(key) > 128 || len(value) > 256 {
			return errors.New("tag keys must be between 1 and 128 characters, and tag values must be between 0 and 256 characters")
		}

		if !isValidBlobTagsKeyValue(key) {
			return errors.New("incorrect character set used in key: " + k)
		}

		if !isValidBlobTagsKeyValue(value) {
			return errors.New("incorrect character set used in value: " + v)
		}
	}
	return nil
}

func ValidateMetadataString(metadata string) error {
	if strings.EqualFold(metadata, common.MetadataAndBlobTagsClearFlag) {
		return nil
	}
	metadataMap, err := common.StringToMetadata(metadata)
	if err != nil {
		return err
	}
	for k := range metadataMap {
		if strings.ContainsAny(k, " !#$%^&*,<>{}|\\:.()+'\"?/") {
			return fmt.Errorf("invalid metadata key value '%s': can't have spaces or special characters", k)
		}
	}

	return nil
}

// validateListOfFilesFormat checks if the file uses the old JSON format
func validateListOfFilesFormat(f *os.File) error {
	scanner := bufio.NewScanner(f)
	headerLineNum := 0
	firstLineIsCurlyBrace := false
	utf8BOM := string([]byte{0xEF, 0xBB, 0xBF})
	checkBOM := false

	for scanner.Scan() && headerLineNum <= 1 {
		v := scanner.Text()

		// Check if the UTF-8 BOM is on the first line and remove it if necessary.
		if !checkBOM {
			v = strings.TrimPrefix(v, utf8BOM)
			checkBOM = true
		}

		cleanedLine := strings.Replace(strings.Replace(v, " ", "", -1), "\t", "", -1)
		cleanedLine = strings.TrimSuffix(cleanedLine, "[") // don't care which line this is on, could be third line

		if cleanedLine == "{" && headerLineNum == 0 {
			firstLineIsCurlyBrace = true
		} else {
			const jsonStart = "{\"Files\":"
			jsonStartNoBrace := strings.TrimPrefix(jsonStart, "{")
			isJson := cleanedLine == jsonStart || firstLineIsCurlyBrace && cleanedLine == jsonStartNoBrace
			if isJson {
				return errors.New("The format for list-of-files has changed. The old JSON format is no longer supported")
			}
		}
		headerLineNum++
	}

	return nil
}