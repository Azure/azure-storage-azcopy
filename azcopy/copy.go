package azcopy

import (
	"context"
	"fmt"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

type CopyResult struct {
	common.ListJobSummaryResponse
	ElapsedTime time.Duration
}

type CopyJobHandler interface {
	OnStart(ctx common.JobContext)
}

// CopyOptions contains the optional parameters for the Copy operation.
type CopyOptions struct {
	IncludeBefore               time.Time
	IncludeAfter                time.Time
	IncludePatterns             []string
	IncludePaths                []string
	ExcludePaths                []string
	IncludeRegex                []string
	ExcludeRegex                []string
	ExcludePatterns             []string
	Overwrite                   *bool // Default true
	AutoDecompress              bool
	Recursive                   bool
	FromTo                      common.FromTo
	ExcludeBlobTypes            []common.BlobType
	BlockSizeMB                 float64
	PutBlobSizeMB               float64
	BlobType                    common.BlobType
	BlockBlobTier               common.BlockBlobTier
	PageBlobTier                common.PageBlobTier
	Metadata                    *map[string]string
	ContentType                 string
	ContentEncoding             string
	ContentDisposition          string
	ContentLanguage             string
	CacheControl                string
	NoGuessMimeType             bool
	PreserveLastModifiedTime    bool
	PreserveSMBPermissions      bool
	AsSubDir                    *bool //Default true
	PreserveOwner               *bool // Default true
	PreserveInfo                *bool // Custom default logic
	PreservePosixProperties     bool
	Symlinks                    common.SymlinkHandlingType
	ForceIfReadOnly             bool
	BackupMode                  bool
	PutMd5                      bool // TODO: (gapra) Should we make this an enum called PutHash for None/MD5? So user can set the HashType?
	CheckMd5                    common.HashValidationOption
	IncludeAttributes           []string
	ExcludeAttributes           []string
	ExcludeContainers           []string
	CheckLength                 bool
	S2SPreserveProperties       *bool // Default true
	S2SPreserveAccessTier       *bool // Default true
	S2SDetectSourceChanged      bool
	S2SHandleInvalidateMetadata common.InvalidMetadataHandleOption
	ListOfVersionIds            string
	BlobTags                    *map[string]string
	S2SPreserveBlobTags         bool
	IncludeDirectoryStubs       bool
	DisableAutoDecoding         bool
	TrailingDot                 common.TrailingDotOption
	CpkByName                   string
	CpkByValue                  bool
	Hardlinks                   common.HardlinkHandlingType

	listOfFiles                      string
	dryrun                           bool
	commandString                    string
	dryrunJobPartOrderHandler        func(request common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse
	s2SGetPropertiesInBackend        *bool // Default true
	flushThreshold                   int64
	deleteDestinationFileIfNecessary bool
}

func (c *CopyOptions) WithInternalOptions(listOfFiles string) {
	c.listOfFiles = listOfFiles
}

// Copy copies the contents from source to destination.
func (c *Client) Copy(ctx context.Context, src, dest string, opts CopyOptions, handler CopyJobHandler) (CopyResult, error) {
	// Input
	if src == "" || dest == "" {
		return CopyResult{}, fmt.Errorf("source and destination must be specified for copy")
	}

	return CopyResult{}, nil
}
