package client

import (
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-storage-azcopy/v10/cmd"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"time"
)

type CopyOptions struct {
	src string
	dst string

	FollowSymlinks                   bool
	IncludeBefore                    *time.Time
	IncludeAfter                     *time.Time
	IncludePattern                   []string
	IncludePath                      []string
	ExcludePath                      []string
	IncludeRegex                     []string
	ExcludeRegex                     []string
	listOfFiles                      string
	ExcludePattern                   []string
	Overwrite                        common.OverwriteOption // Default true
	Decompress                       bool
	Recursive                        bool
	FromTo                           common.FromTo
	ExcludeBlobType                  []blob.BlobType
	BlockSizeMB                      float64
	PutBlobSizeMB                    float64
	BlobType                         common.BlobType
	BlockBlobTier                    common.BlockBlobTier
	PageBlobTier                     common.PageBlobTier
	Metadata                         common.Metadata // Note: for --metadata=clear, pass in common.Metadata{}
	ContentType                      string
	ContentEncoding                  string
	ContentDisposition               string
	ContentLanguage                  string
	CacheControl                     string
	NoGuessMimeType                  bool
	PreserveLastModifiedTime         bool
	preserveSMBPermissions           bool
	AsSubDir                         bool // Default true
	PreserveOwner                    bool // Default true
	PreserveSMBInfo                  bool // Default based on OS
	PreservePOSIXProperties          bool
	PreserveSymlinks                 bool
	ForceIfReadOnly                  bool
	BackupMode                       bool
	PutMd5                           bool
	HashValidation                   common.HashValidationOption // check md5
	IncludeAttributes                []string
	ExcludeAttributes                []string
	ExcludeContainer                 []string
	CheckLength                      bool // Default true
	S2SPreserveProperties            bool // Default true
	S2SPreserveAccessTier            bool // Default true
	S2SDetectSourceChanged           bool
	S2SHandleInvalidMetadata         common.InvalidMetadataHandleOption
	ListOfVersions                   string
	BlobTags                         common.BlobTags // Note: for --blob-tags=clear, pass in common.BlobTags{}
	S2SPreserveBlobTags              bool
	IncludeDirectoryStubs            bool
	DisableAutoDecoding              bool
	DryRun                           bool
	s2SGetPropertiesInBackend        bool // Default true
	TrailingDot                      common.TrailingDotOption
	CpkByName                        string
	CpkByValue                       bool
	PreservePermissions              bool
	deleteDestinationFileIfNecessary bool
}

func (opts CopyOptions) convert() (cca cmd.CookedCopyCmdArgs, err error) {
	cca = cmd.CookedCopyCmdArgs{
		Src:             opts.src,
		Dst:             opts.dst,
		FromTo:          opts.FromTo,
		Recursive:       opts.Recursive,
		ForceIfReadOnly: opts.ForceIfReadOnly,
	}
	err = cca.SymlinkHandling.Determine(opts.FollowSymlinks, opts.PreserveSymlinks)
	if err != nil {
		return
	}
	cca.ForceWrite = opts.Overwrite
	cca.AutoDecompress = opts.Decompress
	cca.BlockSizeMB = opts.BlockSizeMB
	cca.PutBlobSizeMB = opts.PutBlobSizeMB
	cca.BlobType = opts.BlobType
	cca.BlockBlobTier = opts.BlockBlobTier
	cca.PageBlobTier = opts.PageBlobTier
	cca.ListOfFilesChannel, err = cmd.NormalizeIncludePaths(opts.listOfFiles, opts.IncludePath)
	if err != nil {
		return cca, err
	}
	cca.IncludeBefore = opts.IncludeBefore
	cca.IncludeAfter = opts.IncludeAfter
	cca.ListOfVersionIDs, err = cmd.ProcessVersionIds(opts.ListOfVersions)
	if err != nil {
		return cca, err
	}
	cca.Metadata = opts.Metadata.ToString()
	cca.ContentType = opts.ContentType
	cca.ContentEncoding = opts.ContentEncoding
	cca.ContentLanguage = opts.ContentLanguage
	cca.ContentDisposition = opts.ContentDisposition
	cca.CacheControl = opts.CacheControl
	cca.NoGuessMimeType = opts.NoGuessMimeType
	cca.PreserveLastModifiedTime = opts.PreserveLastModifiedTime
	cca.DisableAutoDecoding = opts.DisableAutoDecoding
	cca.TrailingDot = opts.TrailingDot
	cca.BlobTags = opts.BlobTags
	cca.S2sPreserveBlobTags = opts.S2SPreserveBlobTags
	cca.CpkByName = opts.CpkByName
	cca.CpkByValue = opts.CpkByValue
	cca.PutMd5 = opts.PutMd5
	cca.HashValidationOption = opts.HashValidation
	cca.CheckLength = opts.CheckLength
	cca.PreserveSMBInfo = opts.PreserveSMBInfo || opts.PreservePermissions
	cca.PreservePOSIXProperties = opts.PreservePOSIXProperties
	cca.PreserveOwner = opts.PreserveOwner
	cca.AsSubdir = opts.AsSubDir
	cca.IncludeDirectoryStubs = opts.IncludeDirectoryStubs
	cca.BackupMode = opts.BackupMode
	cca.S2sPreserveProperties = opts.S2SPreserveProperties
	cca.S2sGetPropertiesInBackend = opts.s2SGetPropertiesInBackend
	cca.S2sPreserveAccessTier = opts.S2SPreserveAccessTier
	cca.S2sSourceChangeValidation = opts.S2SDetectSourceChanged
	cca.ExcludeBlobType = opts.ExcludeBlobType
	cca.S2sInvalidMetadataHandleOption = opts.S2SHandleInvalidMetadata
	cca.IncludePatterns = opts.IncludePattern
	cca.ExcludePatterns = opts.ExcludePattern
	cca.ExcludePathPatterns = opts.ExcludePath
	cca.ExcludeContainer = opts.ExcludeContainer
	cca.IncludeFileAttributes = opts.IncludeAttributes
	cca.ExcludeFileAttributes = opts.ExcludeAttributes
	cca.IncludeRegex = opts.IncludeRegex
	cca.ExcludeRegex = opts.ExcludeRegex
	cca.DryrunMode = opts.DryRun
	cca.DeleteDestinationFileIfNecessary = opts.deleteDestinationFileIfNecessary
	return
}

func (cc Client) Copy(source string, destination string, options CopyOptions) error {
	options.src = source
	options.dst = destination

	return nil
}
