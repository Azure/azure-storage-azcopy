package cmd

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-storage-azcopy/v10/common"
)

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

func (cooked *CookedCopyCmdArgs) SetHardlinks(hardlinkHandlingType common.HardlinkHandlingType) {
	cooked.hardlinks = hardlinkHandlingType
}

// Service-to-Service (S2S) transfer options
func (cooked *CookedCopyCmdArgs) SetS2sPreserveProperties(value bool) {
	cooked.s2sPreserveProperties.value = value
}

func (cooked *CookedCopyCmdArgs) SetS2sPreserveAccessTier(value bool) {
	cooked.s2sPreserveAccessTier.value = value
}

func (cooked *CookedCopyCmdArgs) SetS2sInvalidMetadataHandleOption(value common.InvalidMetadataHandleOption) {
	cooked.s2sInvalidMetadataHandleOption = value
}

func (cooked *CookedCopyCmdArgs) SetS2sSourceChangeValidation(value bool) {
	cooked.s2sSourceChangeValidation = value
}

func (cooked *CookedCopyCmdArgs) SetS2sGetPropertiesInBackend(value bool) {
	cooked.s2sGetPropertiesInBackend = value
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

func (cooked *cookedSyncCmdArgs) SetStripTopDir(stripTopDir bool) {
	cooked.stripTopDir = stripTopDir
}

func (cooked *cookedSyncCmdArgs) SetJobId(jobID common.JobID) {
	cooked.jobID = jobID
}

func (cooked *cookedSyncCmdArgs) SetDestination(destination common.ResourceString) {
	cooked.destination = destination
}

func (cooked *cookedSyncCmdArgs) SetDestinationValue(destination string) {
	cooked.destination.Value = destination
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
	PreserveInfo            bool
	ForceIfReadOnly         bool
	Md5ValidationOption     string
	CompareHash             string
	LocalHashStorageMode    string
	Hardlinks               string
	IncludeDirectoryStubs   bool
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
	Hardlinks               string
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
		preserveInfo:            args.PreserveInfo,
		forceIfReadOnly:         args.ForceIfReadOnly,
		md5ValidationOption:     args.Md5ValidationOption,
		compareHash:             args.CompareHash,
		localHashStorageMode:    args.LocalHashStorageMode,
		hardlinks:               args.Hardlinks,
		includeDirectoryStubs:   args.IncludeDirectoryStubs,
	}
	return raw.cook()
}

func (cca *cookedSyncCmdArgs) SetCredentialInfo(ctx context.Context) error {
	return cca.setCredentialInfo(ctx)
}

// ToStringMap returns a map representation of cookedSyncCmdArgs
// It masks sensitive information and only includes non-empty/valid values
func (cooked *cookedSyncCmdArgs) ToStringMap() map[string]string {
	if cooked == nil {
		return map[string]string{"<nil>": ""}
	}

	result := make(map[string]string)

	// Add source and destination with SAS token masking
	if cooked.source.Value != "" {
		safeSource := cooked.source.Value
		if strings.Contains(safeSource, "?") {
			urlParts := strings.Split(safeSource, "?")
			safeSource = urlParts[0] + "?<SAS-REDACTED>"
		}
		result["source"] = safeSource
	}

	if cooked.destination.Value != "" {
		safeDest := cooked.destination.Value
		if strings.Contains(safeDest, "?") {
			urlParts := strings.Split(safeDest, "?")
			safeDest = urlParts[0] + "?<SAS-REDACTED>"
		}
		result["destination"] = safeDest
	}

	// Add fromTo if valid
	if cooked.fromTo != common.EFromTo.Unknown() {
		result["fromTo"] = cooked.fromTo.String()
	}

	// Add jobID if valid
	if cooked.jobID != (common.JobID{}) {
		result["jobID"] = cooked.jobID.String()
	}

	// Add boolean flags only if true
	if cooked.recursive {
		result["recursive"] = "true"
	}
	if cooked.stripTopDir {
		result["stripTopDir"] = "true"
	}
	if cooked.dryrunMode {
		result["dryrunMode"] = "true"
	}
	if cooked.mirrorMode {
		result["mirrorMode"] = "true"
	}
	if cooked.forceIfReadOnly {
		result["forceIfReadOnly"] = "true"
	}
	if cooked.backupMode {
		result["backupMode"] = "true"
	}
	if cooked.putMd5 {
		result["putMd5"] = "true"
	}
	if cooked.preserveInfo {
		result["preserveInfo"] = "true"
	}
	if cooked.preservePOSIXProperties {
		result["preservePOSIXProperties"] = "true"
	}
	if cooked.s2sPreserveBlobTags {
		result["s2sPreserveBlobTags"] = "true"
	}
	if cooked.preserveAccessTier {
		result["preserveAccessTier"] = "true"
	}
	if cooked.includeDirectoryStubs {
		result["includeDirectoryStubs"] = "true"
	}
	if cooked.includeRoot {
		result["includeRoot"] = "true"
	}

	// Add enums/options if not default/empty
	if cooked.preservePermissions != common.EPreservePermissionsOption.None() {
		// PreservePermissionsOption doesn't have String() method, so we handle it manually
		permStr := "None"
		switch cooked.preservePermissions {
		case common.EPreservePermissionsOption.ACLsOnly():
			permStr = "ACLsOnly"
		case common.EPreservePermissionsOption.OwnershipAndACLs():
			permStr = "OwnershipAndACLs"
		}
		result["preservePermissions"] = permStr
	}
	if cooked.deleteDestination != common.EDeleteDestination.False() {
		result["deleteDestination"] = cooked.deleteDestination.String()
	}
	if cooked.compareHash != common.ESyncHashType.None() {
		result["compareHash"] = cooked.compareHash.String()
	}
	if cooked.md5ValidationOption != common.EHashValidationOption.NoCheck() {
		result["md5ValidationOption"] = cooked.md5ValidationOption.String()
	}
	if cooked.symlinkHandling != common.ESymlinkHandlingType.Skip() {
		// SymlinkHandlingType doesn't have String() method either
		symlinkStr := "Skip"
		if cooked.symlinkHandling.Follow() {
			symlinkStr = "Follow"
		} else if cooked.symlinkHandling.Preserve() {
			symlinkStr = "Preserve"
		}
		result["symlinkHandling"] = symlinkStr
	}
	if cooked.hardlinks != common.EHardlinkHandlingType.Follow() {
		result["hardlinks"] = cooked.hardlinks.String()
	}
	if cooked.trailingDot != common.ETrailingDotOption.Enable() {
		result["trailingDot"] = cooked.trailingDot.String()
	}

	// Add filters if present
	if len(cooked.includePatterns) > 0 {
		result["includePatterns"] = fmt.Sprintf("%v", cooked.includePatterns)
	}
	if len(cooked.excludePatterns) > 0 {
		result["excludePatterns"] = fmt.Sprintf("%v", cooked.excludePatterns)
	}
	if len(cooked.excludePaths) > 0 {
		result["excludePaths"] = fmt.Sprintf("%v", cooked.excludePaths)
	}
	if len(cooked.includeRegex) > 0 {
		result["includeRegex"] = fmt.Sprintf("%v", cooked.includeRegex)
	}
	if len(cooked.excludeRegex) > 0 {
		result["excludeRegex"] = fmt.Sprintf("%v", cooked.excludeRegex)
	}

	// Add sizes if non-zero
	if cooked.blockSize > 0 {
		result["blockSize"] = fmt.Sprintf("%d", cooked.blockSize)
	}
	if cooked.putBlobSize > 0 {
		result["putBlobSize"] = fmt.Sprintf("%d", cooked.putBlobSize)
	}

	// Add counters only if non-zero
	srcFiles := atomic.LoadUint64(&cooked.atomicSourceFilesScanned)
	if srcFiles > 0 {
		result["sourceFilesScanned"] = fmt.Sprintf("%d", srcFiles)
	}
	dstFiles := atomic.LoadUint64(&cooked.atomicDestinationFilesScanned)
	if dstFiles > 0 {
		result["destinationFilesScanned"] = fmt.Sprintf("%d", dstFiles)
	}
	deletions := atomic.LoadUint32(&cooked.atomicDeletionCount)
	if deletions > 0 {
		result["deletionCount"] = fmt.Sprintf("%d", deletions)
	}

	// Always mask credential info
	if cooked.credentialInfo.CredentialType != common.ECredentialType.Unknown() {
		result["credentialType"] = cooked.credentialInfo.CredentialType.String()
	}

	// Add CPK info if present (without exposing keys)
	if cooked.cpkOptions.CpkScopeInfo != "" {
		result["cpkByName"] = "<PRESENT>"
	}
	if cooked.cpkOptions.CpkInfo {
		result["cpkByValue"] = "true"
	}

	// Scanning status
	if cooked.scanningComplete() {
		result["scanningComplete"] = "true"
	}
	if cooked.firstPartOrdered() {
		result["firstPartOrdered"] = "true"
	}

	return result
}

// ToString returns a safe string representation of cookedSyncCmdArgs
// It masks sensitive information and only includes non-empty/valid values
func (cooked *cookedSyncCmdArgs) ToString() string {
	stringMap := cooked.ToStringMap()

	// Convert map to sorted slice for consistent output
	var parts []string
	for k, v := range stringMap {
		if k == "<nil>" {
			return "<nil>"
		}
		parts = append(parts, fmt.Sprintf("%s: %s", k, v))
	}

	// Sort parts for consistent output
	sort.Strings(parts)

	return fmt.Sprintf("cookedSyncCmdArgs{%s}", strings.Join(parts, ", "))
}

// ToStringMap returns a map representation of CookedCopyCmdArgs
// It masks sensitive information and only includes non-empty/valid values
func (cooked *CookedCopyCmdArgs) ToStringMap() map[string]string {
	if cooked == nil {
		return map[string]string{"<nil>": ""}
	}

	result := make(map[string]string)

	// Add source and destination with SAS token masking
	if cooked.Source.Value != "" {
		safeSource := cooked.Source.Value
		if strings.Contains(safeSource, "?") {
			urlParts := strings.Split(safeSource, "?")
			safeSource = urlParts[0] + "?<SAS-REDACTED>"
		}
		result["source"] = safeSource
	}

	if cooked.Destination.Value != "" {
		safeDest := cooked.Destination.Value
		if strings.Contains(safeDest, "?") {
			urlParts := strings.Split(safeDest, "?")
			safeDest = urlParts[0] + "?<SAS-REDACTED>"
		}
		result["destination"] = safeDest
	}

	// Add fromTo if valid
	if cooked.FromTo != common.EFromTo.Unknown() {
		result["fromTo"] = cooked.FromTo.String()
	}

	// Add jobID if valid
	if cooked.jobID != (common.JobID{}) {
		result["jobID"] = cooked.jobID.String()
	}

	// Add boolean flags only if true
	if cooked.Recursive {
		result["recursive"] = "true"
	}
	if cooked.StripTopDir {
		result["stripTopDir"] = "true"
	}
	if cooked.IsSourceDir {
		result["isSourceDir"] = "true"
	}
	if cooked.ForceIfReadOnly {
		result["forceIfReadOnly"] = "true"
	}
	if cooked.autoDecompress {
		result["autoDecompress"] = "true"
	}
	if cooked.preserveLastModifiedTime {
		result["preserveLastModifiedTime"] = "true"
	}
	if cooked.putMd5 {
		result["putMd5"] = "true"
	}
	if cooked.CheckLength {
		result["checkLength"] = "true"
	}
	if cooked.noGuessMimeType {
		result["noGuessMimeType"] = "true"
	}
	if cooked.preservePOSIXProperties {
		result["preservePOSIXProperties"] = "true"
	}
	if cooked.backupMode {
		result["backupMode"] = "true"
	}
	if cooked.asSubdir {
		result["asSubdir"] = "true"
	}
	if cooked.s2sGetPropertiesInBackend {
		result["s2sGetPropertiesInBackend"] = "true"
	}
	if cooked.s2sSourceChangeValidation {
		result["s2sSourceChangeValidation"] = "true"
	}
	if cooked.S2sPreserveBlobTags {
		result["s2sPreserveBlobTags"] = "true"
	}
	if cooked.IncludeDirectoryStubs {
		result["includeDirectoryStubs"] = "true"
	}
	if cooked.disableAutoDecoding {
		result["disableAutoDecoding"] = "true"
	}
	if cooked.dryrunMode {
		result["dryrunMode"] = "true"
	}
	if cooked.deleteDestinationFileIfNecessary {
		result["deleteDestinationFileIfNecessary"] = "true"
	}
	if cooked.preserveInfo {
		result["preserveInfo"] = "true"
	}
	if cooked.preserveOwner {
		result["preserveOwner"] = "true"
	}
	if cooked.isCleanupJob {
		result["isCleanupJob"] = "true"
	}

	// Add enums/options if not default/empty
	if cooked.ForceWrite != common.EOverwriteOption.True() {
		result["forceWrite"] = cooked.ForceWrite.String()
	}
	if cooked.SymlinkHandling != common.ESymlinkHandlingType.Skip() {
		// SymlinkHandlingType doesn't have String() method
		symlinkStr := "Skip"
		if cooked.SymlinkHandling.Follow() {
			symlinkStr = "Follow"
		} else if cooked.SymlinkHandling.Preserve() {
			symlinkStr = "Preserve"
		}
		result["symlinkHandling"] = symlinkStr
	}
	if cooked.preservePermissions != common.EPreservePermissionsOption.None() {
		// PreservePermissionsOption doesn't have String() method
		permStr := "None"
		switch cooked.preservePermissions {
		case common.EPreservePermissionsOption.ACLsOnly():
			permStr = "ACLsOnly"
		case common.EPreservePermissionsOption.OwnershipAndACLs():
			permStr = "OwnershipAndACLs"
		}
		result["preservePermissions"] = permStr
	}
	if cooked.blobType != common.EBlobType.Detect() {
		result["blobType"] = cooked.blobType.String()
	}
	if cooked.blockBlobTier != common.EBlockBlobTier.None() {
		result["blockBlobTier"] = cooked.blockBlobTier.String()
	}
	if cooked.pageBlobTier != common.EPageBlobTier.None() {
		result["pageBlobTier"] = cooked.pageBlobTier.String()
	}
	if cooked.deleteSnapshotsOption != common.EDeleteSnapshotsOption.None() {
		result["deleteSnapshotsOption"] = cooked.deleteSnapshotsOption.String()
	}
	if cooked.md5ValidationOption != common.EHashValidationOption.NoCheck() {
		result["md5ValidationOption"] = cooked.md5ValidationOption.String()
	}
	if cooked.s2sInvalidMetadataHandleOption != common.EInvalidMetadataHandleOption.ExcludeIfInvalid() {
		result["s2sInvalidMetadataHandleOption"] = cooked.s2sInvalidMetadataHandleOption.String()
	}
	if cooked.permanentDeleteOption != common.EPermanentDeleteOption.None() {
		result["permanentDeleteOption"] = cooked.permanentDeleteOption.String()
	}
	if cooked.rehydratePriority != common.ERehydratePriorityType.Standard() {
		result["rehydratePriority"] = cooked.rehydratePriority.String()
	}
	if cooked.trailingDot != common.ETrailingDotOption.Enable() {
		result["trailingDot"] = cooked.trailingDot.String()
	}
	if cooked.hardlinks != common.EHardlinkHandlingType.Follow() {
		result["hardlinks"] = cooked.hardlinks.String()
	}

	// Add sizes if non-zero
	if cooked.blockSize > 0 {
		result["blockSize"] = fmt.Sprintf("%d", cooked.blockSize)
	}
	if cooked.putBlobSize > 0 {
		result["putBlobSize"] = fmt.Sprintf("%d", cooked.putBlobSize)
	}

	// Add strings if not empty
	if cooked.metadata != "" && cooked.metadata != common.MetadataAndBlobTagsClearFlag {
		result["metadata"] = cooked.metadata
	}
	if cooked.contentType != "" {
		result["contentType"] = cooked.contentType
	}
	if cooked.contentEncoding != "" {
		result["contentEncoding"] = cooked.contentEncoding
	}
	if cooked.contentLanguage != "" {
		result["contentLanguage"] = cooked.contentLanguage
	}
	if cooked.contentDisposition != "" {
		result["contentDisposition"] = cooked.contentDisposition
	}
	if cooked.cacheControl != "" {
		result["cacheControl"] = cooked.cacheControl
	}
	if cooked.blobTags != "" {
		result["blobTags"] = cooked.blobTags
	}

	// Add filters if present
	if len(cooked.IncludePatterns) > 0 {
		result["includePatterns"] = fmt.Sprintf("%v", cooked.IncludePatterns)
	}
	if len(cooked.ExcludePatterns) > 0 {
		result["excludePatterns"] = fmt.Sprintf("%v", cooked.ExcludePatterns)
	}
	if len(cooked.ExcludePathPatterns) > 0 {
		result["excludePathPatterns"] = fmt.Sprintf("%v", cooked.ExcludePathPatterns)
	}
	if len(cooked.excludeContainer) > 0 {
		result["excludeContainer"] = fmt.Sprintf("%v", cooked.excludeContainer)
	}
	if len(cooked.IncludeFileAttributes) > 0 {
		result["includeFileAttributes"] = fmt.Sprintf("%v", cooked.IncludeFileAttributes)
	}
	if len(cooked.ExcludeFileAttributes) > 0 {
		result["excludeFileAttributes"] = fmt.Sprintf("%v", cooked.ExcludeFileAttributes)
	}
	if len(cooked.includeRegex) > 0 {
		result["includeRegex"] = fmt.Sprintf("%v", cooked.includeRegex)
	}
	if len(cooked.excludeRegex) > 0 {
		result["excludeRegex"] = fmt.Sprintf("%v", cooked.excludeRegex)
	}
	if len(cooked.excludeBlobType) > 0 {
		result["excludeBlobType"] = fmt.Sprintf("%v", cooked.excludeBlobType)
	}

	// Add time filters if present
	if cooked.IncludeBefore != nil {
		result["includeBefore"] = cooked.IncludeBefore.Format(time.RFC3339)
	}
	if cooked.IncludeAfter != nil {
		result["includeAfter"] = cooked.IncludeAfter.Format(time.RFC3339)
	}

	// Add boolean default true values only if false
	if cooked.s2sPreserveProperties.isManuallySet && !cooked.s2sPreserveProperties.value {
		result["s2sPreserveProperties"] = "false"
	}
	if cooked.s2sPreserveAccessTier.isManuallySet && !cooked.s2sPreserveAccessTier.value {
		result["s2sPreserveAccessTier"] = "false"
	}

	// Add counters only if non-zero
	skippedSymlinks := atomic.LoadUint32(&cooked.atomicSkippedSymlinkCount)
	if skippedSymlinks > 0 {
		result["skippedSymlinkCount"] = fmt.Sprintf("%d", skippedSymlinks)
	}
	skippedSpecialFiles := atomic.LoadUint32(&cooked.atomicSkippedSpecialFileCount)
	if skippedSpecialFiles > 0 {
		result["skippedSpecialFileCount"] = fmt.Sprintf("%d", skippedSpecialFiles)
	}

	// Always mask credential info
	if cooked.credentialInfo.CredentialType != common.ECredentialType.Unknown() {
		result["credentialType"] = cooked.credentialInfo.CredentialType.String()
	}

	// Add CPK info if present (without exposing keys)
	if cooked.CpkOptions.CpkScopeInfo != "" {
		result["cpkByName"] = "<PRESENT>"
	}
	if cooked.CpkOptions.CpkInfo {
		result["cpkByValue"] = "true"
	}

	// Add properties to transfer if not default
	if cooked.propertiesToTransfer != common.ESetPropertiesFlags.None() {
		result["propertiesToTransfer"] = fmt.Sprintf("0x%x", cooked.propertiesToTransfer)
	}

	// Add status flags
	if cooked.isEnumerationComplete {
		result["isEnumerationComplete"] = "true"
	}

	// Add cleanup job message if present
	if cooked.cleanupJobMessage != "" {
		result["cleanupJobMessage"] = cooked.cleanupJobMessage
	}

	return result
}

// ToString returns a safe string representation of CookedCopyCmdArgs
// It masks sensitive information and only includes non-empty/valid values
func (cooked *CookedCopyCmdArgs) ToString() string {
	stringMap := cooked.ToStringMap()

	// Convert map to sorted slice for consistent output
	var parts []string
	for k, v := range stringMap {
		if k == "<nil>" {
			return "<nil>"
		}
		parts = append(parts, fmt.Sprintf("%s: %s", k, v))
	}

	// Sort parts for consistent output
	sort.Strings(parts)

	return fmt.Sprintf("CookedCopyCmdArgs{%s}", strings.Join(parts, ", "))
}

// ============================================================================
// End Utility Functions - Misellaneous
// ============================================================================
