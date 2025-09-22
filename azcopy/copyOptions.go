package azcopy

import (
	"fmt"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/traverser"
)

type CookedCopyOptions struct {
	source      common.ResourceString
	destination common.ResourceString

	fromTo        common.FromTo
	filterOptions traverser.FilterOptions

	listOfVersionIds chan string
	listOFFiles      chan string

	recursive                        bool
	stripTopDir                      bool
	symlink                          common.SymlinkHandlingType
	hardlink                         common.HardlinkHandlingType
	forceWrite                       common.OverwriteOption
	forceIfReadOnly                  bool
	isSourceDir                      bool
	autoDecompress                   bool
	blockSize                        int64
	putBlobSize                      int64
	blobType                         common.BlobType
	blobTags                         string
	blockBlobTier                    common.BlockBlobTier
	pageBlobTier                     common.PageBlobTier
	metadata                         string
	contentType                      string
	contentEncoding                  string
	contentLanguage                  string
	contentDisposition               string
	cacheControl                     string
	noGuessMimeType                  bool
	preserveLastModifiedTime         bool
	deleteSnapshotsOption            common.DeleteSnapshotsOption
	putMd5                           bool
	checkMd5                         common.HashValidationOption
	checkLength                      bool
	preservePermissions              common.PreservePermissionsOption
	preservePOSIXProperties          bool
	backupMode                       bool
	asSubdir                         bool
	s2sPreserveProperties            bool
	s2sGetPropertiesInBackend        bool
	s2sPreserveAccessTier            bool
	s2sSourceChangeValidation        bool
	s2sPreserveBlobTags              bool
	s2sInvalidMetadataHandleOption   common.InvalidMetadataHandleOption
	includeDirectoryStubs            bool
	disableAutoDecoding              bool
	dryrun                           bool
	cpkOptions                       common.CpkOptions
	trailingDot                      common.TrailingDotOption
	deleteDestinationFileIfNecessary bool
	preserveInfo                     bool

	commandString string
}

func newCookedCopyOptions(src, dst string, opts CopyOptions) (s *CookedCopyOptions, err error) {
	c = &CookedCopyOptions{}

	err = c.applyFromToSrcDest(src, dst, opts.FromTo)
	if err != nil {
		return nil, err
	}

	err = c.applyDefaultsAndInferOptions(opts)
	if err != nil {
		return nil, err
	}

	err = c.validateOptions()
	if err != nil {
		return nil, err
	}

	return c, nil
}

func (c *CookedCopyOptions) applyFromToSrcDest(src, dst string, fromTo common.FromTo) (err error) {
	// fromTo, source and destination
	userFromTo := common.Iff(fromTo == common.EFromTo.Unknown(), "", fromTo.String())
	c.fromTo, err = InferAndValidateFromTo(src, dst, userFromTo)
	if err != nil {
		return err
	}
	common.SetNFSFlag(AreBothLocationsNFSAware(c.fromTo))

	return nil
}

func (c *CookedCopyOptions) applyDefaultsAndInferOptions(opts CopyOptions) (err error) {
	// defaults

	// 1:1 mapping
	c.recursive = opts.Recursive
	c.forceIfReadOnly = opts.ForceIfReadOnly
	c.autoDecompress = opts.AutoDecompress
	c.blockSize, err = BlockSizeInBytes(opts.BlockSizeMB)
	if err != nil {
		return err
	}
	c.putBlobSize, err = BlockSizeInBytes(opts.PutBlobSizeMB)
	if err != nil {
		return err
	}
	// TODO:  list of files
	// TODO: list of Version IDs

	// inference
}
