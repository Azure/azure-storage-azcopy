package client

import "github.com/Azure/azure-storage-azcopy/v10/common"

type SyncOptions struct {
	Recursive                        bool
	FromTo                           common.FromTo
	IncludeDirectoryStubs            bool
	preserveSMBPermissions           bool
	PreserveSMBInfo                  bool // Default based on OS
	PreservePOSIXProperties          bool
	ForceIfReadOnly                  bool
	BlockSizeMB                      float64
	PutBlobSizeMB                    float64
	IncludePattern                   []string
	ExcludePattern                   []string
	ExcludePath                      []string
	IncludeAttributes                []string
	ExcludeAttributes                []string
	IncludeRegex                     []string
	ExcludeRegex                     []string
	DeleteDestination                bool
	PutMd5                           bool
	HashValidation                   common.HashValidationOption // check md5
	S2SPreserveAccessTier            bool
	S2SPreserveBlobTags              bool
	CpkByName                        string
	CpkByValue                       bool
	MirrorMode                       bool
	DryRun                           bool
	TrailingDot                      common.TrailingDotOption
	IncludeRoot                      bool
	CompareHash                      common.SyncHashType
	HashMetaDir                      string
	LocalHashStorageMode             common.HashStorageMode
	PreservePermissions              bool
	deleteDestinationFileIfNecessary bool
}

func (cc Client) Sync(source string, destination string, options SyncOptions) error {
	return nil
}
