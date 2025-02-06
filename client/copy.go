package client

import (
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
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
	Metadata                         string
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
	BlobTags                         string
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

func (cc Client) Copy(source string, destination string, options CopyOptions) error {
	return nil
}
