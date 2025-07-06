package cmd

import (
	"context"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-storage-azcopy/v10/common"
)

// RawMoverSyncCmdArgs - Represents the raw command line arguments for the mover sync command.
// This struct is a subset of rawSyncCmdArgs, specifically tailored for the mover sync command.
type RawMoverSyncCmdArgs struct {
	Src                     string
	Dst                     string
	FromTo                  string
	Recursive               bool
	ExcludeRegex            string
	FollowSymlinks          bool
	DeleteDestination       string
	PreservePOSIXProperties bool
	PreservePermissions     bool
	PreserveSMBInfo         bool
	ForceIfReadOnly         bool
	Md5ValidationOption     string
	CompareHash             string
	LocalHashStorageMode    string
}

// ============================================================================
// CookedCopyCmdArgs - Property Getters
// ============================================================================

// File and directory preservation options
func (cooked *CookedCopyCmdArgs) TrailingDot() common.TrailingDotOption {
	return cooked.trailingDot
}

func (cooked *CookedCopyCmdArgs) PreserveInfo() bool {
	return cooked.preserveInfo
}

func (cooked *CookedCopyCmdArgs) PreservePOSIXProperties() bool {
	return cooked.preservePOSIXProperties
}

func (cooked *CookedCopyCmdArgs) PreservePermissions() common.PreservePermissionsOption {
	return cooked.preservePermissions
}

// Deletion and cleanup options
func (cooked *CookedCopyCmdArgs) PermanentDeleteOption() common.PermanentDeleteOption {
	return cooked.permanentDeleteOption
}

// Service-to-Service (S2S) transfer options
func (cooked *CookedCopyCmdArgs) S2sPreserveProperties() bool {
	return cooked.s2sPreserveProperties.value
}

func (cooked *CookedCopyCmdArgs) S2sPreserveAccessTier() bool {
	return cooked.s2sPreserveAccessTier.value
}

func (cooked *CookedCopyCmdArgs) S2sInvalidMetadataHandleOption() common.InvalidMetadataHandleOption {
	return cooked.s2sInvalidMetadataHandleOption
}

func (cooked *CookedCopyCmdArgs) S2sSourceChangeValidation() bool {
	return cooked.s2sSourceChangeValidation
}

func (cooked *CookedCopyCmdArgs) S2sGetPropertiesInBackend() bool {
	return cooked.s2sGetPropertiesInBackend
}

// Directory and path handling options
func (cooked *CookedCopyCmdArgs) AsSubdir() bool {
	return cooked.asSubdir
}

// Authentication and credential options
func (cooked *CookedCopyCmdArgs) CredentialInfo() common.CredentialInfo {
	return cooked.credentialInfo
}

func (cooked *CookedCopyCmdArgs) Hardlinks() common.HardlinkHandlingType {
	return cooked.hardlinks
}

func (cooked *CookedCopyCmdArgs) IsNfsCopy() bool {
	return cooked.isNFSCopy
}

// ============================================================================
// End CookedCopyCmdArgs - Property Getters
// ============================================================================

// ============================================================================
// CookedCopyCmdArgs - Property Setters
// ============================================================================

func (cooked *CookedCopyCmdArgs) SetPreservePOSIXProperties(preservePOSIXProperties bool) {
	cooked.preservePOSIXProperties = preservePOSIXProperties
}

func (cooked *CookedCopyCmdArgs) SetAsSubdir(asSubdir bool) {
	cooked.asSubdir = asSubdir
}

func (cooked *CookedCopyCmdArgs) SetPreservePermissions(preservePermissions common.PreservePermissionsOption) {
	cooked.preservePermissions = preservePermissions
}

func (cooked *CookedCopyCmdArgs) SetPreserveInfo(preserveInfo bool) {
	cooked.preserveInfo = preserveInfo
}

func (cooked *CookedCopyCmdArgs) SetPermanentDeleteOption(permanentDeleteOptionStr string) error {
	return cooked.permanentDeleteOption.Parse(permanentDeleteOptionStr)
}

func (cooked *CookedCopyCmdArgs) SetIsNfsCopy(isNfsCopy bool) {
	cooked.isNFSCopy = isNfsCopy
}

// ============================================================================
// End CookedCopyCmdArgs - Property Setters
// ============================================================================

// ============================================================================
// rawSyncCmdArgs - Property Getters
// ============================================================================

func (raw *rawSyncCmdArgs) Src() string {
	return raw.src
}

func (raw *rawSyncCmdArgs) Dst() string {
	return raw.dst
}

func (raw *rawSyncCmdArgs) Recursive() bool {
	return raw.recursive
}

func (raw *rawSyncCmdArgs) FromTo() string {
	return raw.fromTo
}

func (raw *rawSyncCmdArgs) ExcludeRegex() string {
	return raw.excludeRegex
}

func (raw *rawSyncCmdArgs) CompareHash() string {
	return raw.compareHash
}

func (raw *rawSyncCmdArgs) LocalHashStorageMode() string {
	return raw.localHashStorageMode
}

func (raw *rawSyncCmdArgs) PreservePermissions() bool {
	return raw.preservePermissions
}

func (raw *rawSyncCmdArgs) PreserveSMBInfo() bool {
	return raw.preserveSMBInfo
}

func (raw *rawSyncCmdArgs) PreservePOSIXProperties() bool {
	return raw.preservePOSIXProperties
}

func (raw *rawSyncCmdArgs) FollowSymlinks() bool {
	return raw.followSymlinks
}

func (raw *rawSyncCmdArgs) Md5ValidationOption() string {
	return raw.md5ValidationOption
}

func (raw *rawSyncCmdArgs) DeleteDestination() string {
	return raw.deleteDestination
}

func (raw *rawSyncCmdArgs) ForceIfReadOnly() bool {
	return raw.forceIfReadOnly
}

// ============================================================================
// End rawSyncCmdArgs - Property Getters
// ============================================================================

// ============================================================================
// rawSyncCmdArgs - Property Setters
// ============================================================================

func (raw *rawSyncCmdArgs) SetSrc(src string) {
	raw.src = src
}

func (raw *rawSyncCmdArgs) SetDst(dst string) {
	raw.dst = dst
}

func (raw *rawSyncCmdArgs) SetRecursive(recursive bool) {
	raw.recursive = recursive
}

func (raw *rawSyncCmdArgs) SetFromTo(fromTo string) {
	raw.fromTo = fromTo
}

func (raw *rawSyncCmdArgs) SetExcludeRegex(excludeRegex string) {
	raw.excludeRegex = excludeRegex
}

func (raw *rawSyncCmdArgs) SetCompareHash(compareHash string) {
	raw.compareHash = compareHash
}

func (raw *rawSyncCmdArgs) SetLocalHashStorageMode(localHashStorageMode string) {
	raw.localHashStorageMode = localHashStorageMode
}

func (raw *rawSyncCmdArgs) SetPreservePermissions(preservePermissions bool) {
	raw.preservePermissions = preservePermissions
}

func (raw *rawSyncCmdArgs) SetPreserveSMBInfo(preserveSMBInfo bool) {
	raw.preserveSMBInfo = preserveSMBInfo
}

func (raw *rawSyncCmdArgs) SetPreservePOSIXProperties(preservePOSIXProperties bool) {
	raw.preservePOSIXProperties = preservePOSIXProperties
}

func (raw *rawSyncCmdArgs) SetFollowSymlinks(followSymlinks bool) {
	raw.followSymlinks = followSymlinks
}

func (raw *rawSyncCmdArgs) SetMd5ValidationOption(md5ValidationOption string) {
	raw.md5ValidationOption = md5ValidationOption
}

func (raw *rawSyncCmdArgs) SetDeleteDestination(deleteDestination string) {
	raw.deleteDestination = deleteDestination
}

func (raw *rawSyncCmdArgs) SetForceIfReadOnly(forceIfReadOnly bool) {
	raw.forceIfReadOnly = forceIfReadOnly
}

// ============================================================================
// End rawSyncCmdArgs - Property Setters
// ============================================================================

func (cooked *cookedSyncCmdArgs) Destination() common.ResourceString {
	return cooked.destination
}

func (cooked *cookedSyncCmdArgs) Source() common.ResourceString {
	return cooked.source
}

func (cooked *cookedSyncCmdArgs) JobId() common.JobID {
	return cooked.jobID
}

func (cooked *cookedSyncCmdArgs) FromTo() common.FromTo {
	return cooked.fromTo
}

func (cooked *cookedSyncCmdArgs) StripTopDir() bool {
	return cooked.stripTopDir
}

func (cooked *cookedSyncCmdArgs) FirstPartOrdered() bool {
	return cooked.firstPartOrdered()
}

func (cooked *cookedSyncCmdArgs) ScanningComplete() bool {
	return cooked.scanningComplete()
}

func (cooked *cookedSyncCmdArgs) GetDeletionCount() uint32 {
	return cooked.getDeletionCount()
}

func (cooked *cookedSyncCmdArgs) SetJobId(jobID common.JobID) {
	cooked.jobID = jobID
}

// ============================================================================
// Utility Functions - Misellaneous
// ============================================================================

func CreateClientOptionsExt(
	logger common.ILoggerResetable,
	srcCred *common.ScopedToken,
	reauthCred *common.ScopedAuthenticator) azcore.ClientOptions {
	return createClientOptions(logger, srcCred, reauthCred)
}

type SyncCmdArgsInput struct {
	Src                     string
	Dst                     string
	FromTo                  string
	Recursive               bool
	ExcludeRegex            string
	FollowSymlinks          bool
	DeleteDestination       string
	PreservePOSIXProperties bool
	PreservePermissions     bool
	PreserveSMBInfo         bool
	ForceIfReadOnly         bool
	Md5ValidationOption     string
	CompareHash             string
	LocalHashStorageMode    string
}

func CookRawSyncCmdArgs(args RawMoverSyncCmdArgs) (cookedSyncCmdArgs, error) {
	raw := rawSyncCmdArgs{
		src:                     args.Src,
		dst:                     args.Dst,
		fromTo:                  args.FromTo,
		recursive:               args.Recursive,
		excludeRegex:            args.ExcludeRegex,
		followSymlinks:          args.FollowSymlinks,
		deleteDestination:       args.DeleteDestination,
		preservePOSIXProperties: args.PreservePOSIXProperties,
		preservePermissions:     args.PreservePermissions,
		preserveSMBInfo:         args.PreserveSMBInfo,
		forceIfReadOnly:         args.ForceIfReadOnly,
		md5ValidationOption:     args.Md5ValidationOption,
		compareHash:             args.CompareHash,
		localHashStorageMode:    args.LocalHashStorageMode,
	}
	return raw.cook()
}

func (cca *cookedSyncCmdArgs) SetCredentialInfo(ctx context.Context) error {
	return cca.setCredentialInfo(ctx)
}

func InitializeAzCopyFolders(
	logPathFolder,
	jobPlanFolder,
	appPathFolder string) (azcopyLogPathFolder, azcopyJobPlanFolder string) {
	return initializeFolders(logPathFolder, jobPlanFolder, appPathFolder)
}

// ============================================================================
// End Utility Functions - Misellaneous
// ============================================================================
