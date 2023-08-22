package e2etest

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"github.com/Azure/azure-storage-azcopy/v10/cmd"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/google/uuid"
	"io"
	"time"
)

func init() {
	// Enforce interfaces
	var sm ServiceResourceManager
	var cm ContainerResourceManager
	var rrm RemoteResourceManager
	sm = &BlobServiceResourceManager{}
	cm = &BlobContainerResourceManager{}
	rrm = sm
	rrm = cm.(*BlobContainerResourceManager)
	_, _, _ = sm, cm, rrm // "use" vars
}

type BlobServiceResourceManager struct {
	parent        AzureAccountResourceManager
	ServiceClient azblob.ServiceURL
}

func (b *BlobServiceResourceManager) AccountType() AccountType {
	return b.parent.AccountType()
}

func (b *BlobServiceResourceManager) ResourceClient() any {
	return b.ServiceClient
}

func (b *BlobServiceResourceManager) ValidAuthTypes() ExplicitCredentialTypes {
	return EExplicitCredentialType.PublicAuth() | EExplicitCredentialType.SASToken() | EExplicitCredentialType.OAuth()
}

func (b *BlobServiceResourceManager) Location() common.Location {
	return common.ELocation.Blob()
}

func (b *BlobServiceResourceManager) Level() cmd.LocationLevel {
	return cmd.ELocationLevel.Service()
}

type BlobServiceCreateContainerOptions struct {
	Metadata          azblob.Metadata
	PublicAccessLevel azblob.PublicAccessType
}

// CreateContainer utilizes BlobServiceCreateContainerOptions for ResourceSpecificOptions.
func (b *BlobServiceResourceManager) CreateContainer(name string, options *CreateContainerOptions) (ContainerResourceManager, error) {
	containerURL := b.ServiceClient.NewContainerURL(name)

	opts := DerefOrZero(options)
	cco := GetTypeOrZero[BlobServiceCreateContainerOptions](opts.ResourceSpecificOptions)

	_, err := containerURL.Create(ctx, cco.Metadata, cco.PublicAccessLevel)

	return &BlobContainerResourceManager{parent: b, client: containerURL}, err
}

type BlobServiceDeleteContainerOptions struct {
	AccessConditions azblob.ContainerAccessConditions
}

func (b *BlobServiceResourceManager) DeleteContainer(name string, options *DeleteContainerOptions) error {
	containerURL := b.ServiceClient.NewContainerURL(name)

	opts := DerefOrZero(options)
	dco := GetTypeOrZero[BlobServiceDeleteContainerOptions](opts.ResourceSpecificOptions)

	_, err := containerURL.Delete(ctx, dco.AccessConditions)

	return err
}

func (b *BlobServiceResourceManager) GetContainer(name string) ContainerResourceManager {
	return &BlobContainerResourceManager{parent: b, client: b.ServiceClient.NewContainerURL(name)}
}

type BlobContainerResourceManager struct {
	parent *BlobServiceResourceManager
	client azblob.ContainerURL
}

func (b *BlobContainerResourceManager) AccountType() AccountType {
	return b.parent.AccountType()
}

func (b *BlobContainerResourceManager) ValidAuthTypes() ExplicitCredentialTypes {
	return b.parent.ValidAuthTypes()
}

func (b *BlobContainerResourceManager) ResourceClient() any {
	return b.client
}

func (b *BlobContainerResourceManager) Location() common.Location {
	return common.ELocation.Blob()
}

func (b *BlobContainerResourceManager) Level() cmd.LocationLevel {
	return cmd.ELocationLevel.Container()
}

type BlobContainerCreateObjectOptions struct {
	BlobType           azblob.BlobType // Default = Block Blob
	AccessTier         azblob.AccessTierType
	PageBlobAccessTier azblob.PremiumPageBlobAccessTierType
	BlobTags           azblob.BlobTagsMap
	CPKOptions         azblob.ClientProvidedKeyOptions
	Immutability       azblob.ImmutabilityPolicyOptions

	//block/append blob option(s)
	BlockSize *int64 // default: 4000 MiB
}

func (b *BlobContainerResourceManager) Create(path string, entityType common.EntityType, options *CreateObjectOptions) error {
	baseBlobURL := b.client.NewBlobURL(path)

	opts := DerefOrZero(options)
	blobOptions := GetTypeOrZero[BlobContainerCreateObjectOptions](opts.ResourceSpecificOptions)
	meta := azblob.Metadata(opts.Metadata)

	switch entityType {
	case common.EEntityType.Folder():
		meta[common.POSIXFolderMeta] = "true" // mark it as a folder
		_, err := baseBlobURL.ToBlockBlobURL().Upload(
			ctx,
			bytes.NewReader(make([]byte, 0)), // Folders cannot have content.
			opts.Headers.ToBlob(),
			meta,
			azblob.BlobAccessConditions{},
			blobOptions.AccessTier,
			blobOptions.BlobTags,
			blobOptions.CPKOptions,
			blobOptions.Immutability,
		)
		return err
	case common.EEntityType.Symlink():
		meta[common.POSIXSymlinkMeta] = "true"
		_, err := baseBlobURL.ToBlockBlobURL().Upload(
			ctx,
			opts.Content.Reader(),
			opts.Headers.ToBlob(),
			meta,
			azblob.BlobAccessConditions{},
			blobOptions.AccessTier,
			blobOptions.BlobTags,
			blobOptions.CPKOptions,
			blobOptions.Immutability,
		)
		return err
	case common.EEntityType.File(): // break
	default:
	}

	msu := MultiStepUploader{Parallel: true}
	content := opts.Content
	switch blobOptions.BlobType { // prepare the MSU
	case azblob.BlobBlockBlob, azblob.BlobNone:
		msu.BlockSize = 4 * 1024 * 1024 * 1000 // 4,000 MiB
		blockList := make([]string, msu.GetBlockCount(content.Size()))
		bbClient := baseBlobURL.ToBlockBlobURL()

		msu.Init = nil // Block blobs require no special initialization
		msu.UploadRange = func(block io.ReadSeeker, state MultiStepUploaderState) error {
			blockId := base64.StdEncoding.EncodeToString([]byte(uuid.NewString()))
			blockList[state.BlockIndex] = blockId // register the block ID

			_, err := bbClient.StageBlock(
				ctx,
				blockId,
				block,
				azblob.LeaseAccessConditions{}, // No lease until we're done creating the blob
				nil,                            // todo: transactional md5?
				blobOptions.CPKOptions,
			)

			return err
		}
		msu.Finalize = func() error {
			_, err := bbClient.CommitBlockList(
				ctx,
				blockList,
				opts.Headers.ToBlob(),
				meta,
				azblob.BlobAccessConditions{},
				blobOptions.AccessTier,
				blobOptions.BlobTags,
				blobOptions.CPKOptions,
				blobOptions.Immutability)
			return err
		}
	case azblob.BlobPageBlob:
		msu.BlockSize = 4 * 1024 * 1024 // 4 MiB
		pbClient := baseBlobURL.ToPageBlobURL()

		msu.Init = func(size int64) error {
			_, err := pbClient.Create(
				ctx,
				content.Size(),
				0,
				opts.Headers.ToBlob(),
				meta,
				azblob.BlobAccessConditions{},
				blobOptions.PageBlobAccessTier,
				blobOptions.BlobTags,
				blobOptions.CPKOptions,
				azblob.ImmutabilityPolicyOptions{}, // Don't apply immutability until end
			)

			return err
		}
		msu.UploadRange = func(block io.ReadSeeker, state MultiStepUploaderState) error {
			_, err := pbClient.UploadPages(
				ctx,
				state.Offset,
				block,
				azblob.PageBlobAccessConditions{},
				nil,
				blobOptions.CPKOptions,
			)

			return err
		}
	case azblob.BlobAppendBlob:
		msu.Parallel = false
		msu.BlockSize = 4 * 1024 * 1024 * 1000 // 4,000 MiB
		abClient := baseBlobURL.ToAppendBlobURL()

		msu.Init = func(size int64) (err error) {
			_, err = abClient.Create(
				ctx,
				opts.Headers.ToBlob(),
				meta,
				azblob.BlobAccessConditions{},
				blobOptions.BlobTags,
				blobOptions.CPKOptions,
				azblob.ImmutabilityPolicyOptions{}, // hold until completion
			)

			return
		}
		msu.UploadRange = func(block io.ReadSeeker, state MultiStepUploaderState) (err error) {
			_, err = abClient.AppendBlock(
				ctx,
				block,
				azblob.AppendBlobAccessConditions{},
				nil,
				blobOptions.CPKOptions,
			)

			return
		}
	}

	// add immutability
	originalFinalize := msu.Finalize
	msu.Finalize = func() (err error) {
		if originalFinalize != nil {
			err := originalFinalize() // Call original finalize first
			if err != nil {
				return err
			}
		}

		if blobOptions.Immutability.LegalHold != nil && *blobOptions.Immutability.LegalHold {
			_, err = baseBlobURL.SetLegalHold(ctx, true)
		} else if blobOptions.Immutability.ImmutabilityPolicyMode != azblob.BlobImmutabilityPolicyModeNone {
			var expiry time.Time
			if blobOptions.Immutability.ImmutabilityPolicyUntilDate != nil {
				expiry = *blobOptions.Immutability.ImmutabilityPolicyUntilDate
			}

			_, err = baseBlobURL.SetImmutabilityPolicy(ctx, expiry, blobOptions.Immutability.ImmutabilityPolicyMode, nil)
		}

		return
	}

	msu.UploadContents(opts.Content)

	return nil
}

type BlobObjectReadOptions struct {
	CPKOptions         azblob.ClientProvidedKeyOptions
	RetryReaderOptions azblob.RetryReaderOptions
}

func (b *BlobContainerResourceManager) Read(path string, options *ReadObjectOptions) ([]byte, error) {
	baseBlobURL := b.client.NewBlobURL(path)
	opts := DerefOrZero(options)
	blobOpts := GetTypeOrZero[BlobObjectReadOptions](opts.ResourceSpecificOptions)
	resp, err := baseBlobURL.Download(ctx, opts.offset, opts.count, azblob.BlobAccessConditions{}, false, blobOpts.CPKOptions)
	if err != nil {
		return nil, err
	}

	blobOpts.RetryReaderOptions.ClientProvidedKeyOptions = blobOpts.CPKOptions // ensure parity
	return io.ReadAll(resp.Body(blobOpts.RetryReaderOptions))                  // todo: should we return the io.ReadCloser instead?
}

type BlobContainerObjectPropertiesOptions struct {
	CPKOptions azblob.ClientProvidedKeyOptions
}

func (b *BlobContainerResourceManager) GetProperties(path string, options *GetObjectPropertiesOptions) (props GenericObjectProperties, err error) {
	baseBlobURL := b.client.NewBlobURL(path)
	opts := DerefOrZero(options)
	blobOptions := GetTypeOrZero[BlobContainerObjectPropertiesOptions](opts.ResourceSpecificOptions)
	var resp *azblob.BlobGetPropertiesResponse
	resp, err = baseBlobURL.GetProperties(ctx, azblob.BlobAccessConditions{}, blobOptions.CPKOptions)

	props.OriginalResponse = resp
	props.headers = common.ResourceHTTPHeaders(resp.NewHTTPHeaders())
	props.metadata = common.FromAzBlobMetadataToCommonMetadata(resp.NewMetadata())
	props.ResourceSpecificProperties = BlobObjectProperties{
		BlobType:      common.FromAzBlobType(resp.BlobType()),
		AccessTier:    azblob.AccessTierType(resp.AccessTier()),
		LeaseStatus:   resp.LeaseStatus(),
		LeaseDuration: resp.LeaseDuration(),
		LeaseState:    resp.LeaseState(),
		ArchiveStatus: azblob.ArchiveStatusType(resp.ArchiveStatus()),
	}

	return
}

type BlobContainerObjectSetPropertiesOptions struct {
	CPKOptions azblob.ClientProvidedKeyOptions
}

// SetProperties also uses BlobContainerObjectPropertiesOptions for ResourceSpecificOptions
func (b *BlobContainerResourceManager) SetProperties(path string, props GenericObjectProperties, options *SetObjectPropertiesOptions) error {
	//blobProps := GetTypeOrZero[BlobObjectProperties](props.ResourceSpecificProperties)
	blobClient := b.client.NewBlobURL(path)

	_, err := blobClient.SetHTTPHeaders(ctx, props.headers.ToAzBlobHTTPHeaders(), azblob.BlobAccessConditions{})
	if err != nil {
		return fmt.Errorf("when setting HTTP headers: %w", err)
	}

	blobOpts := GetTypeOrZero[BlobContainerObjectSetPropertiesOptions](DerefOrZero(options).ResourceSpecificOptions)
	_, err = blobClient.SetMetadata(ctx, props.metadata.ToAzBlobMetadata(), azblob.BlobAccessConditions{}, blobOpts.CPKOptions)
	return err
}

type BlobContainerObjectDeleteOptions struct {
	Snapshots azblob.DeleteSnapshotsOptionType
}

func (b *BlobContainerResourceManager) Delete(path string, options *DeleteObjectProperties) error {
	opts := DerefOrZero(options)
	blobOpts := GetTypeOrZero[BlobContainerObjectDeleteOptions](opts.ResourceSpecificOptions)
	baseBlobURL := b.client.NewBlobURL(path)
	_, err := baseBlobURL.Delete(ctx, blobOpts.Snapshots, azblob.BlobAccessConditions{})
	return err
}
