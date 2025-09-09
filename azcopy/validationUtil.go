package azcopy

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"reflect"
	"regexp"
	"runtime"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/file"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/traverser"
	"github.com/JeffreyRichter/enum/enum"
)

// TODO : (gapra) Once migration is done, look at all functions in azcopy and see which ones can be made private

var IPv4Regex = regexp.MustCompile(`\d+\.\d+\.\d+\.\d+`)

const (
	PipeLocation          = "~pipe~"
	fromToHelpFormat      = "Specified to nudge AzCopy when resource detection may not work (e.g. piping/emulator/azure stack); Valid FromTo are pairs of Source-Destination words (e.g. BlobLocal, BlobBlob) that specify the source and destination resource types. All valid FromTos are: %s"
	DstShareDoesNotExists = "the destination file share does not exist; please create it manually with the required quota and settings before running the copy —refer to https://learn.microsoft.com/en-us/azure/storage/files/storage-how-to-create-file-share?tabs=azure-portal for SMB or https://learn.microsoft.com/en-us/azure/storage/files/storage-files-quick-create-use-linux for NFS."
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

	return fmt.Sprintf(fromToHelpFormat, strings.TrimSuffix(validFromTos, ", "))
}()

var fromToHelpText = FromToHelp

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
		return common.EFromTo.Unknown(), fmt.Errorf("invalid --from-to value specified: %q. "+fromToHelpText, userSpecifiedFromTo)

	}

	if userFromTo == common.EFromTo.LocalFileSMB() {
		userFromTo = common.EFromTo.LocalFile()
	} else if userFromTo == common.EFromTo.FileSMBLocal() {
		userFromTo = common.EFromTo.FileLocal()
	} else if userFromTo == common.EFromTo.FileSMBFileSMB() {
		userFromTo = common.EFromTo.FileFile()
	}

	if userFromTo == common.EFromTo.FileSMBFileNFS() || userFromTo == common.EFromTo.FileNFSFileSMB() {
		return common.EFromTo.Unknown(), errors.New("The --from-to value of " + userFromTo.String() +
			" is not supported currently. " +
			"Copy operations between SMB and NFS file shares are not supported yet.")
	}

	return userFromTo, nil
}

func inferFromTo(src, dst string) common.FromTo {
	// Try to infer the 1st argument
	srcLocation := InferArgumentLocation(src)
	if srcLocation == srcLocation.Unknown() {
		common.GetLifecycleMgr().Info("Cannot infer source location of " +
			common.URLStringExtension(src).RedactSecretQueryParamForLogging() +
			". Please specify the --from-to switch. " + fromToHelpText)
		return common.EFromTo.Unknown()
	}

	dstLocation := InferArgumentLocation(dst)
	if dstLocation == dstLocation.Unknown() {
		common.GetLifecycleMgr().Info("Cannot infer destination location of " +
			common.URLStringExtension(dst).RedactSecretQueryParamForLogging() +
			". Please specify the --from-to switch. " + fromToHelpText)
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
		"specify an exact source and destination type using the --from-to switch. " + fromToHelpText)

	return out
}

func InferArgumentLocation(arg string) common.Location {
	if arg == PipeLocation {
		return common.ELocation.Pipe()
	}
	if traverser.StartsWith(arg, "http") {
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

// it is assume that the given url has the SAS stripped, and safe to print
func validateURLIsNotServiceLevel(url string, location common.Location) error {
	srcLevel, err := traverser.DetermineLocationLevel(url, location, true)
	if err != nil {
		return err
	}

	if srcLevel == traverser.ELocationLevel.Service() {
		return fmt.Errorf("service level URLs (%s) are not supported in sync: ", url)
	}

	return nil
}

func ValidateForceIfReadOnly(toForce bool, fromTo common.FromTo) error {
	targetIsFiles := (fromTo.To() == common.ELocation.File() || fromTo.To() == common.ELocation.FileNFS()) ||
		fromTo == common.EFromTo.FileTrash()
	targetIsWindowsFS := fromTo.To() == common.ELocation.Local() &&
		runtime.GOOS == "windows"
	targetIsOK := targetIsFiles || targetIsWindowsFS
	if toForce && !targetIsOK {
		return errors.New("force-if-read-only is only supported when the target is Azure Files or a Windows file system")
	}
	return nil
}

// ValidateNFSOptions performs validation specific to NFS (Network File System) configurations
func ValidateNFSOptions(fromTo common.FromTo,
	preservePermissions common.PreservePermissionsOption,
	preserveInfo bool,
	symlinkHandling common.SymlinkHandlingType,
	hardlinkHandling common.HardlinkHandlingType) (err error) {
	// platform
	if isUnsupported, err := isUnsupportedPlatformForNFS(fromTo); isUnsupported {
		return err
	}
	// preserve permissions
	// If we are not preserving original file permissions (raw.preservePermissions == false),
	// and the operation is a file copy from azure file NFS to local linux (FromTo == FileLocal),
	// and the current OS is Linux, then we require root privileges to proceed.
	//
	// This is because modifying file ownership or permissions on Linux
	// typically requires elevated privileges. To safely handle permission
	// changes during the local file operation, we enforce that the process
	// must be running as root.
	if !preservePermissions.IsTruthy() && fromTo == common.EFromTo.FileNFSLocal() {
		if err := common.EnsureRunningAsRoot(); err != nil {
			return fmt.Errorf("failed to copy source to destination without preserving permissions: operation not permitted. Please retry with root privileges or use the default option (--preserve-permissions=true)")
		}
	}
	if err = validatePreserveForNFSAware(fromTo, preservePermissions.IsTruthy(), "preserve-permissions"); err != nil {
		return err
	}
	// preserve info
	if err = validatePreserveForNFSAware(fromTo, preserveInfo, "preserve-info"); err != nil {
		return err
	}
	// symlink handling
	if err = validateSymlinkForNFSAware(symlinkHandling); err != nil {
		return err
	}
	// hardlink handling
	if err = validateHardlinkForNFSAware(fromTo, hardlinkHandling); err != nil {
		return err
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

func validatePreserveForNFSAware(fromTo common.FromTo, toPreserve bool, flagName string) error {
	// 1. Upload (Windows/Linux -> Azure File)
	// 2. Download (Azure File -> Windows/Linux)
	// 3. S2S (Azure File -> Azure File)
	// TODO: More combination checks to be added later
	if toPreserve && !(fromTo == common.EFromTo.LocalFileNFS() ||
		fromTo == common.EFromTo.FileNFSLocal() ||
		fromTo == common.EFromTo.FileNFSFileNFS()) {
		return fmt.Errorf("%s is set but the job is not between %s-aware resources", flagName, common.Iff(flagName == "preserve-info", "permission", "NFS"))
	}

	if toPreserve && (fromTo.IsUpload() || fromTo.IsDownload()) &&
		runtime.GOOS != "windows" && runtime.GOOS != "linux" {
		return fmt.Errorf("%s is set but persistence for up/downloads is supported only in Windows and Linux", flagName)
	}

	return nil
}

// validateSymlinkForNFSAware checks whether the '--follow-symlink' or '--preserve-symlink' flags
// are set for an NFS copy operation. Since symlink support is not available for NFS,
// the function returns an error if either flag is enabled.
// By default, symlink files will be skipped during NFS copy.
func validateSymlinkForNFSAware(option common.SymlinkHandlingType) error {
	if option == common.ESymlinkHandlingType.Follow() {
		return fmt.Errorf("The '--follow-symlink' flag is not supported for NFS copy. Symlink files will be skipped by default.")

	}
	if option == common.ESymlinkHandlingType.Preserve() {
		return fmt.Errorf("the --preserve-symlink flag is not support for NFS copy. Symlink files will be skipped by default.")
	}
	return nil
}

// validateHardlinkForNFSAware
func validateHardlinkForNFSAware(fromTo common.FromTo, option common.HardlinkHandlingType) error {
	// Validate for Download: Only allowed when downloading from an NFS share to a Linux filesystem
	if runtime.GOOS == "linux" && fromTo.IsDownload() && (fromTo.From() != common.ELocation.FileNFS()) {
		return fmt.Errorf("The --hardlinks option, when downloading, is only supported from a NFS file share to a Linux filesystem.")
	}
	// Validate for Upload or S2S: Only allowed when uploading *to* a local file system
	if runtime.GOOS == "linux" && (fromTo.IsUpload() || fromTo.IsS2S()) && (fromTo.To() != common.ELocation.FileNFS()) {
		return fmt.Errorf("The --hardlinks option, when uploading, is only supported from a NFS file share to a Linux filesystem or between NFS file shares.")
	}
	if option == common.DefaultHardlinkHandlingType {
		common.GetLifecycleMgr().Info("The --hardlinks option is set to 'follow'. Hardlinked files will be copied as a regular file at the destination.")
	}
	return nil
}

func ValidateSMBOptions(fromTo common.FromTo,
	preservePermissions common.PreservePermissionsOption,
	preserveInfo bool,
	preservePOSIXProperties bool) (err error) {
	// preserve permissions
	if err = validatePreserveForSMBAware(fromTo, preservePermissions.IsTruthy(), "preserve-permissions"); err != nil {
		return err
	}
	// preserve info
	if err = validatePreserveForSMBAware(fromTo, preserveInfo, "preserve-info"); err != nil {
		return err
	}
	// preserve POSIX properties
	if preservePOSIXProperties && !areBothLocationsPOSIXAware(fromTo) {
		return errors.New("to use the --preserve-posix-properties flag, both the source and destination must be POSIX-aware. Valid combinations are: Linux -> Blob, Blob -> Linux, or Blob -> Blob")
	}
	return nil
}

func validatePreserveForSMBAware(fromTo common.FromTo, toPreserve bool, flagName string) error {
	// 1. Upload (Windows/Linux -> Azure File)
	// 2. Download (Azure File -> Windows/Linux)
	// 3. S2S (Azure File -> Azure File)
	// TODO: (gapra) I kind of don't like this check being here. It should live in ValidateSMBOptions I think.
	if toPreserve && flagName == "preserve-permissions" &&
		(fromTo == common.EFromTo.BlobBlob() || fromTo == common.EFromTo.BlobFSBlob() || fromTo == common.EFromTo.BlobBlobFS() || fromTo == common.EFromTo.BlobFSBlobFS()) {
		// the user probably knows what they're doing if they're trying to persist permissions between blob-type endpoints.
		return nil
	} else if toPreserve && !(fromTo == common.EFromTo.LocalFile() ||
		fromTo == common.EFromTo.FileLocal() ||
		fromTo == common.EFromTo.FileFile()) {
		return fmt.Errorf("%s is set but the job is not between %s-aware resources", flagName, common.Iff(flagName == "preserve-permission", "permission", "SMB"))
	}

	if toPreserve && (fromTo.IsUpload() || fromTo.IsDownload()) &&
		runtime.GOOS != "windows" && runtime.GOOS != "linux" {
		return fmt.Errorf("%s is set but persistence for up/downloads is supported only in Windows and Linux", flagName)
	}

	return nil
}

func validateShareProtocolCompatibility(
	ctx context.Context,
	resource common.ResourceString,
	serviceClient *common.ServiceClient,
	isSource bool,
	protocol string,
) error {
	if protocol == "" {
		return nil
	}

	direction := "from"
	if !isSource {
		direction = "to"
	}

	// We can ignore the error if we fail to get the share properties.
	shareProtocol, _ := getShareProtocolType(ctx, serviceClient, resource, protocol)

	if shareProtocol == "SMB" && common.IsNFSCopy() {
		return fmt.Errorf("The %s share has SMB protocol enabled. To copy %s a SMB share, use the appropriate --from-to flag value", direction, direction)
	}

	if shareProtocol == "NFS" && !common.IsNFSCopy() {
		return fmt.Errorf("The %s share has NFS protocol enabled. To copy %s a NFS share, use the appropriate --from-to flag value", direction, direction)
	}

	return nil
}

// getShareProtocolType returns "SMB", "NFS", or "UNKNOWN" based on the share's enabled protocols.
// If retrieval fails, it logs a warning and returns the fallback givenValue ("SMB" or "NFS").
func getShareProtocolType(
	ctx context.Context,
	serviceClient *common.ServiceClient,
	resource common.ResourceString,
	givenValue string,
) (string, error) {

	fileURLParts, err := file.ParseURL(resource.Value)
	if err != nil {
		return "UNKNOWN", fmt.Errorf("failed to parse resource URL: %w", err)
	}
	shareName := fileURLParts.ShareName

	fileServiceClient, err := serviceClient.FileServiceClient()
	if err != nil {
		return "UNKNOWN", fmt.Errorf("failed to create file service client: %w", err)
	}

	shareClient := fileServiceClient.NewShareClient(shareName)
	properties, err := shareClient.GetProperties(ctx, nil)
	if err != nil {
		common.GetLifecycleMgr().Info(fmt.Sprintf("Warning: Failed to fetch share properties for '%s'. Assuming the share uses '%s' protocol based on --from-to flag.", shareName, givenValue))
		return givenValue, err
	}

	if properties.EnabledProtocols == nil {
		return "SMB", nil // Default assumption
	}

	return *properties.EnabledProtocols, nil
}

// Protocol compatibility validation for SMB and NFS transfers
func validateProtocolCompatibility(ctx context.Context, fromTo common.FromTo, src, dst common.ResourceString, srcClient, dstClient *common.ServiceClient) error {

	getUploadDownloadProtocol := func(fromTo common.FromTo) string {
		switch fromTo {
		case common.EFromTo.LocalFile(), common.EFromTo.FileLocal():
			return "SMB"
		case common.EFromTo.LocalFileNFS(), common.EFromTo.FileNFSLocal():
			return "NFS"
		default:
			return ""
		}
	}

	var protocol string

	// S2S Transfers
	if fromTo.IsS2S() {
		switch fromTo {
		case common.EFromTo.FileFile():
			protocol = "SMB"
		case common.EFromTo.FileNFSFileNFS():
			protocol = "NFS"
		default:
			if common.IsNFSCopy() {
				return errors.New("NFS copy is not supported for cross-protocol transfers, i.e., Files SMB to Files NFS or vice versa")
			}
		}

		// Validate both source and destination
		if err := validateShareProtocolCompatibility(ctx, src, srcClient, true, protocol); err != nil {
			return err
		}
		return validateShareProtocolCompatibility(ctx, dst, dstClient, false, protocol)
	}

	// Uploads to File Shares
	if fromTo.IsUpload() {
		protocol = getUploadDownloadProtocol(fromTo)
		return validateShareProtocolCompatibility(ctx, dst, dstClient, false, protocol)
	}

	// Downloads from File Shares
	if fromTo.IsDownload() {
		protocol = getUploadDownloadProtocol(fromTo)
		return validateShareProtocolCompatibility(ctx, src, srcClient, true, protocol)
	}

	return nil
}
