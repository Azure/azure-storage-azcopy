package e2etest

import (
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/appendblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/pageblob"
	blobsas "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/sas"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	"github.com/Azure/azure-storage-azcopy/v10/cmd"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"io"
	"regexp"
	"runtime"
	"strings"
)

/*
TODOs:
- Legal Hold
- Immutability Policy
- Leases
- CPK
*/

// check that everything aligns with interfaces
func init() {
	void := func(_ ...any) {} // prevent go from erroring from unused vars

	var sm ServiceResourceManager = &BlobServiceResourceManager{}
	var cm ContainerResourceManager = &BlobContainerResourceManager{}
	var om ObjectResourceManager = &BlobObjectResourceManager{}

	var rrm RemoteResourceManager

	rrm = &BlobServiceResourceManager{}
	rrm = &BlobContainerResourceManager{}
	rrm = &BlobObjectResourceManager{}

	void(rrm, sm, cm, om)
}

func blobStripSAS(uri string) string {
	parts, err := blob.ParseURL(uri)
	common.PanicIfErr(err)

	parts.SAS = blobsas.QueryParameters{} // remove SAS

	return parts.String()
}

// ==================== SERVICE ====================

type BlobServiceResourceManager struct {
	Account        AccountResourceManager // todo AzureAccountResourceManager
	internalClient *service.Client
}

func (b *BlobServiceResourceManager) ListContainers(a Asserter) []string {
	pager := b.internalClient.NewListContainersPager(&service.ListContainersOptions{})
	out := make([]string, 0)

	for pager.More() {
		page, err := pager.NextPage(ctx)
		a.NoError("retrieve container list page", err)

		for _, item := range page.ContainerItems {
			out = append(out, DerefOrZero(item.Name))
		}
	}

	return out
}

func (b *BlobServiceResourceManager) URI() string {
	return blobStripSAS(b.internalClient.URL())
}

func (b *BlobServiceResourceManager) Location() common.Location {
	return common.ELocation.Blob()
}

func (b *BlobServiceResourceManager) Level() cmd.LocationLevel {
	return cmd.ELocationLevel.Service()
}

func (b *BlobServiceResourceManager) ValidAuthTypes() ExplicitCredentialTypes {
	// Technically AcctKey is valid because of backwards compat integrated for dfs
	// But we don't want to
	return EExplicitCredentialType.With(EExplicitCredentialType.PublicAuth(), EExplicitCredentialType.SASToken(), EExplicitCredentialType.OAuth())
}

func (b *BlobServiceResourceManager) ResourceClient() any {
	return b.internalClient
}

func (b *BlobServiceResourceManager) GetContainer(name string) ContainerResourceManager {
	containerClient := b.internalClient.NewContainerClient(name)
	return &BlobContainerResourceManager{
		Account:        b.Account,
		Service:        b,
		containerName:  name,
		internalClient: containerClient,
	}
}

func (b *BlobServiceResourceManager) IsHierarchical() bool {
	return b.Account.AccountType() == EAccountType.HierarchicalNamespaceEnabled()
}

// ==================== CONTAINER ====================

type BlobContainerResourceManager struct {
	Account        AccountResourceManager // todo AzureAccountResourceManager
	Service        *BlobServiceResourceManager
	containerName  string
	internalClient *container.Client
}

var premiumRegex = regexp.MustCompile("P\\d{2}")

func (b *BlobContainerResourceManager) ListObjects(a Asserter, prefix string, recursive bool) map[string]ObjectProperties {
	out := make(map[string]ObjectProperties)

	processBlobItem := func(v *container.BlobItem) {
		out[*v.Name] = ObjectProperties{
			HTTPHeaders: contentHeaders{
				cacheControl:       v.Properties.CacheControl,
				contentDisposition: v.Properties.ContentDisposition,
				contentEncoding:    v.Properties.ContentEncoding,
				contentLanguage:    v.Properties.ContentLanguage,
				contentType:        v.Properties.ContentType,
				contentMD5:         v.Properties.ContentMD5,
			},
			Metadata: v.Metadata,
			BlobProperties: BlobProperties{
				Type: v.Properties.BlobType,
				Tags: func() map[string]string {
					out := make(map[string]string)

					for _, v := range DerefOrZero(v.BlobTags).BlobTagSet {
						if v.Key == nil || v.Value == nil {
							continue
						}

						out[*v.Key] = *v.Value
					}

					return out
				}(),
				BlockBlobAccessTier: v.Properties.AccessTier,
				PageBlobAccessTier: func() *pageblob.PremiumPageBlobAccessTier {
					if DerefOrZero(v.Properties.BlobType) == blob.BlobTypePageBlob && premiumRegex.MatchString(string(DerefOrZero(v.Properties.AccessTier))) {
						return pointerTo(pageblob.PremiumPageBlobAccessTier(*v.Properties.AccessTier))
					}

					return nil
				}(),
			},
		}
	}

	if !recursive {
		pager := b.internalClient.NewListBlobsHierarchyPager("/", &container.ListBlobsHierarchyOptions{
			Include: container.ListBlobsInclude{Metadata: true, Tags: true, Versions: true, LegalHold: true},
			Prefix:  &prefix,
		})

		for pager.More() {
			page, err := pager.NextPage(ctx)
			a.NoError("get page", err)

			for _, v := range page.Segment.BlobItems {
				processBlobItem(v)
			}
		}
	} else {
		pager := b.internalClient.NewListBlobsFlatPager(&container.ListBlobsFlatOptions{
			Include: container.ListBlobsInclude{Metadata: true, Tags: true, Versions: true, LegalHold: true},
			Prefix:  &prefix,
		})

		for pager.More() {
			page, err := pager.NextPage(ctx)
			a.NoError("get page", err)

			for _, v := range page.Segment.BlobItems {
				processBlobItem(v)
			}
		}
	}

	return out
}

func (b *BlobContainerResourceManager) Create(a Asserter) {
	b.CreateWithOptions(a, nil)
}

type BlobContainerCreateOptions = container.CreateOptions

func (b *BlobContainerResourceManager) CreateWithOptions(a Asserter, options *BlobContainerCreateOptions) {
	_, err := b.internalClient.Create(ctx, options)
	a.NoError("create container", err)
}

func (b *BlobContainerResourceManager) GetObject(a Asserter, path string, eType common.EntityType) ObjectResourceManager {
	return &BlobObjectResourceManager{
		Account:        b.Account,
		Service:        b.Service,
		Container:      b,
		Path:           path,
		entityType:     eType,
		internalClient: b.internalClient.NewBlobClient(path),
	}
}

func (b *BlobContainerResourceManager) ValidAuthTypes() ExplicitCredentialTypes {
	return b.Service.ValidAuthTypes()
}

func (b *BlobContainerResourceManager) ResourceClient() any {
	return b.internalClient
}

func (b *BlobContainerResourceManager) Location() common.Location {
	return b.Service.Location()
}

func (b *BlobContainerResourceManager) Level() cmd.LocationLevel {
	return cmd.ELocationLevel.Container()
}

func (b *BlobContainerResourceManager) URI() string {
	return blobStripSAS(b.internalClient.URL())
}

func (b *BlobContainerResourceManager) HasMetadata() PropertiesAvailability {
	return PropertiesAvailabilityReadWrite
}

func (b *BlobContainerResourceManager) GetMetadata(a Asserter) common.Metadata {
	resp, err := b.internalClient.GetProperties(ctx, &container.GetPropertiesOptions{})
	a.NoError("Get container properties", err)

	return resp.Metadata
}

func (b *BlobContainerResourceManager) SetMetadata(a Asserter, metadata common.Metadata) {
	_, err := b.internalClient.SetMetadata(ctx, &container.SetMetadataOptions{Metadata: metadata})
	a.NoError("Set container metadata", err)
}

func (b *BlobContainerResourceManager) Delete(a Asserter) {
	_, err := b.internalClient.Delete(ctx, nil)
	a.NoError("Delete container", err)
}

func (b *BlobContainerResourceManager) ContainerName() string {
	return b.containerName
}

// ==================== OBJECT ====================

type BlobObjectResourceManager struct {
	Account    AccountResourceManager // todo AzureAccountResourceManager
	Service    *BlobServiceResourceManager
	Container  *BlobContainerResourceManager
	Path       string
	entityType common.EntityType

	internalClient *blob.Client
}

func (b *BlobObjectResourceManager) ValidAuthTypes() ExplicitCredentialTypes {
	return b.Service.ValidAuthTypes()
}

func (b *BlobObjectResourceManager) ResourceClient() any {
	return b.internalClient
}

func (b *BlobObjectResourceManager) EntityType() common.EntityType {
	return b.entityType
}

// Create defaults to Block Blob. For implementation-specific options, GetTypeOrZero[T] / GetTypeOrAssert[T] to BlobObjectResourceManager and call CreateWithOptions
func (b *BlobObjectResourceManager) Create(a Asserter, body ObjectContentContainer, properties ObjectProperties) {
	b.CreateWithOptions(a, body, properties, nil)
}

type BlobObjectCreateOptions struct {
	BlockSize  *int64
	CpkOptions common.CpkOptions
}

func (b *BlobObjectResourceManager) CreateWithOptions(a Asserter, body ObjectContentContainer, properties ObjectProperties, options *BlobObjectCreateOptions) {
	opts := DerefOrZero(options)
	blobProps := properties.BlobProperties

	copyMeta := func() common.Metadata {
		out := make(common.Metadata)

		for k, v := range properties.Metadata {
			out[k] = pointerTo(*v) // deep copy props
		}

		return out
	}

	switch b.entityType {
	case common.EEntityType.Folder():
		// Override body; must be empty
		body = NewZeroObjectContentContainer(0)

		// Set folder meta
		properties.Metadata = copyMeta()
		properties.Metadata[common.POSIXFolderMeta] = pointerTo("true")

		// Override blob type
		properties.BlobProperties.Type = pointerTo(blob.BlobTypeBlockBlob)
	case common.EEntityType.Symlink():
		// body should already be path

		// Set symlink meta
		properties.Metadata = copyMeta()
		properties.Metadata[common.POSIXSymlinkMeta] = pointerTo("true")

		// Override blob type
		properties.BlobProperties.Type = pointerTo(blob.BlobTypeBlockBlob)
	case common.EEntityType.File(): // no-op
	}

	switch DerefOrZero(blobProps.Type) {
	case "", blob.BlobTypeBlockBlob:
		blockSize := DerefOrDefault(opts.BlockSize, common.DefaultBlockBlobBlockSize)
		bodySize := body.Size()

		if bodySize < blockSize*common.MaxNumberOfBlocksPerBlob {
			// resize until fits
			for ; bodySize >= common.MaxNumberOfBlocksPerBlob*blockSize; blockSize = 2 * blockSize {
			}
		}

		_, err := b.Container.internalClient.NewBlockBlobClient(b.Path).UploadStream(ctx, body.Reader(), &blockblob.UploadStreamOptions{
			BlockSize:               blockSize,
			Concurrency:             runtime.NumCPU(),
			TransactionalValidation: blob.TransferValidationTypeComputeCRC64(),
			HTTPHeaders:             properties.HTTPHeaders.ToBlob(),
			Metadata:                properties.Metadata,
			AccessTier:              blobProps.BlockBlobAccessTier,
			Tags:                    blobProps.Tags,
			CPKInfo:                 opts.CpkOptions.GetCPKInfo(),
			CPKScopeInfo:            opts.CpkOptions.GetCPKScopeInfo(),
		})
		a.NoError("Block blob upload", err)
	case blob.BlobTypePageBlob:
		client := b.Container.internalClient.NewPageBlobClient(b.Path)
		blockSize := DerefOrDefault(opts.BlockSize, common.DefaultPageBlobChunkSize)

		msu := &MultiStepUploader{
			Parallel:  true,
			BlockSize: blockSize,
			Init: func(size int64) error {
				_, err := client.Create(
					ctx,
					size,
					&pageblob.CreateOptions{
						Tags:         blobProps.Tags,
						Metadata:     properties.Metadata,
						Tier:         blobProps.PageBlobAccessTier,
						HTTPHeaders:  properties.HTTPHeaders.ToBlob(),
						CPKInfo:      opts.CpkOptions.GetCPKInfo(),
						CPKScopeInfo: opts.CpkOptions.GetCPKScopeInfo(),
					})

				return err
			},
			UploadRange: func(block io.ReadSeekCloser, state MultiStepUploaderState) error {
				_, err := client.UploadPages(
					ctx,
					block,
					blob.HTTPRange{Offset: state.Offset, Count: state.BlockSize},
					&pageblob.UploadPagesOptions{
						TransactionalValidation: blob.TransferValidationTypeComputeCRC64(),
						CPKInfo:                 opts.CpkOptions.GetCPKInfo(),
						CPKScopeInfo:            opts.CpkOptions.GetCPKScopeInfo(),
					})
				return err
			},
		}

		a.NoError("Upload Page Blob", msu.UploadContents(body))
	case blob.BlobTypeAppendBlob:
		blockSize := DerefOrDefault(opts.BlockSize, common.DefaultBlockBlobBlockSize)
		bodySize := body.Size()

		if bodySize < blockSize*common.MaxNumberOfBlocksPerBlob {
			// resize until fits
			for ; bodySize >= common.MaxNumberOfBlocksPerBlob*blockSize; blockSize = 2 * blockSize {
			}
		}

		client := b.Container.internalClient.NewAppendBlobClient(b.Path)

		msu := &MultiStepUploader{
			BlockSize: blockSize,
			Parallel:  false, // Must be serial
			Init: func(size int64) error {
				_, err := client.Create(ctx, &appendblob.CreateOptions{
					HTTPHeaders:  properties.HTTPHeaders.ToBlob(),
					CPKInfo:      opts.CpkOptions.GetCPKInfo(),
					CPKScopeInfo: opts.CpkOptions.GetCPKScopeInfo(),
					Tags:         blobProps.Tags,
					Metadata:     properties.Metadata,
				})

				return err
			},
			UploadRange: func(block io.ReadSeekCloser, state MultiStepUploaderState) error {
				_, err := client.AppendBlock(ctx, block, &appendblob.AppendBlockOptions{
					TransactionalValidation: blob.TransferValidationTypeComputeCRC64(),
					AppendPositionAccessConditions: &appendblob.AppendPositionAccessConditions{
						AppendPosition: &state.Offset,
						MaxSize:        pointerTo(state.Offset + state.BlockSize - 1),
					},
					CPKInfo:      opts.CpkOptions.GetCPKInfo(),
					CPKScopeInfo: opts.CpkOptions.GetCPKScopeInfo(),
				})

				return err
			},
		}

		a.NoError("Upload append blob", msu.UploadContents(body))
	}
}

func (b *BlobObjectResourceManager) ListChildren(a Asserter, recursive bool) map[string]ObjectProperties {
	return b.Container.ListObjects(a, b.Path, recursive)
}

func (b *BlobObjectResourceManager) GetProperties(a Asserter) ObjectProperties {
	return b.GetPropertiesWithOptions(a, nil)
}

type BlobObjectGetPropertiesOptions struct {
	CPKOptions common.CpkOptions
}

func (b *BlobObjectResourceManager) GetPropertiesWithOptions(a Asserter, options *BlobObjectGetPropertiesOptions) ObjectProperties {
	resp, err := b.internalClient.GetProperties(ctx, &blob.GetPropertiesOptions{
		CPKInfo: nil,
	})
	a.NoError("Get properties", err)

	eType := common.EEntityType.File()
	switch {
	case strings.EqualFold(DerefOrZero(resp.Metadata[common.POSIXFolderMeta]), "true"):
		eType = common.EEntityType.Folder()
	case strings.EqualFold(DerefOrZero(resp.Metadata[common.POSIXSymlinkMeta]), "true"):
		eType = common.EEntityType.Symlink()
	}

	return ObjectProperties{
		EntityType: eType,
		HTTPHeaders: contentHeaders{
			cacheControl:       resp.CacheControl,
			contentDisposition: resp.ContentDisposition,
			contentEncoding:    resp.ContentEncoding,
			contentLanguage:    resp.ContentLanguage,
			contentType:        resp.ContentType,
			contentMD5:         resp.ContentMD5,
		},
		Metadata: resp.Metadata,
		BlobProperties: BlobProperties{
			Type: resp.BlobType,
			Tags: func() map[string]string {
				out := make(map[string]string)
				resp, err := b.internalClient.GetTags(ctx, nil)
				a.NoError("Get tags", err)
				for _, tag := range resp.BlobTagSet {
					if tag.Key == nil || tag.Value == nil {
						continue
					}

					out[*tag.Key] = *tag.Value
				}

				return out
			}(),
			BlockBlobAccessTier: nil,
			PageBlobAccessTier:  nil,
		},
	}
}

func (b *BlobObjectResourceManager) SetHTTPHeaders(a Asserter, h contentHeaders) {
	_, err := b.internalClient.SetHTTPHeaders(ctx, DerefOrZero(h.ToBlob()), nil)
	a.NoError("Set HTTP Headers", err)
}

func (b *BlobObjectResourceManager) SetMetadata(a Asserter, metadata common.Metadata) {
	_, err := b.internalClient.SetMetadata(ctx, metadata, nil)
	a.NoError("set metadata", err)
}

func (b *BlobObjectResourceManager) SetObjectProperties(a Asserter, props ObjectProperties) {
	//TODO implement me
	panic("implement me")
}

func (b *BlobObjectResourceManager) Location() common.Location {
	return b.Service.Location()
}

func (b *BlobObjectResourceManager) Level() cmd.LocationLevel {
	return cmd.ELocationLevel.Object()
}

func (b *BlobObjectResourceManager) URI() string {
	return blobStripSAS(b.internalClient.URL())
}