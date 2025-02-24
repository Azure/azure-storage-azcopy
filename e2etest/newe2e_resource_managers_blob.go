package e2etest

import (
	"bytes"
	"fmt"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/streaming"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/appendblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
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
	"time"
)

/*
TODOs:
- Legal Hold
- Immutability Policy
- Leases
- CPK
*/

// enforce interface compliance at compile time
func init() {
	void := func(_ ...any) {} // prevent go from erroring from unused vars

	void(
		ServiceResourceManager(&BlobServiceResourceManager{}),
		ContainerResourceManager(&BlobContainerResourceManager{}),
		ObjectResourceManager(&BlobObjectResourceManager{}),

		RemoteResourceManager(&BlobServiceResourceManager{}),
		RemoteResourceManager(&BlobContainerResourceManager{}),
		RemoteResourceManager(&BlobObjectResourceManager{}),
	)
}

func blobStripSAS(uri string) string {
	parts, err := blob.ParseURL(uri)
	common.PanicIfErr(err)

	parts.SAS = blobsas.QueryParameters{} // remove SAS

	return parts.String()
}

func buildCanonForAzureResourceManager(manager ResourceManager) string {
	// None of the Azure resource managers rely upon Asserter at this moment.
	// This is *OK* for the time being, but also, mentally prepare yourself for the footgun down the line.
	uri := manager.URI()
	// Similarly, the err is ignored.
	// BlobSAS can be used here (for now, again, prepare for the footgun) because
	// we're really interested in extracting details that are shared across all Azure services
	// e.g. acct name, container name, object name
	parsedURI, err := blobsas.ParseURL(uri)
	common.PanicIfErr(err)

	out := ""
	// First, try to extract the account name.
	if parsedURI.IPEndpointStyleInfo.AccountName != "" { // IP endpoints are the easiest
		out += parsedURI.IPEndpointStyleInfo.AccountName
	} else { // (footgun incoming) In public & gov clouds, the account name always comes first. THIS DOES NOT SUPPORT CUSTOM HOSTNAMES.
		out += strings.Split(parsedURI.Host, ".")[0]
	}

	out += "/" + manager.Location().String()

	if manager.Level() >= cmd.ELocationLevel.Container() {
		out += "/" + parsedURI.ContainerName

		if manager.Level() >= cmd.ELocationLevel.Object() {
			out += "/" + parsedURI.BlobName
		}
	}

	return out
}

// ==================== SERVICE ====================

type BlobServiceResourceManager struct {
	InternalAccount *AzureAccountResourceManager
	InternalClient  *service.Client
}

func (b *BlobServiceResourceManager) ValidAuthTypes() ExplicitCredentialTypes {
	// Technically AcctKey is valid because of backwards compat integrated for dfs
	// But we don't want to support that, so we won't test for it.
	return EExplicitCredentialType.With(EExplicitCredentialType.PublicAuth(), EExplicitCredentialType.SASToken(), EExplicitCredentialType.OAuth())
}

func (b *BlobServiceResourceManager) DefaultAuthType() ExplicitCredentialTypes {
	return EExplicitCredentialType.SASToken()
}

func (b *BlobServiceResourceManager) WithSpecificAuthType(cred ExplicitCredentialTypes, a Asserter, opts ...CreateAzCopyTargetOptions) AzCopyTarget {
	a.HelperMarker().Helper()
	return CreateAzCopyTarget(b, cred, a, opts...)
}

func (b *BlobServiceResourceManager) Parent() ResourceManager {
	return nil
}

func (b *BlobServiceResourceManager) Account() AccountResourceManager {
	return b.InternalAccount
}

func (b *BlobServiceResourceManager) Canon() string {
	return buildCanonForAzureResourceManager(b)
}

func (b *BlobServiceResourceManager) ListContainers(a Asserter) []string {
	a.HelperMarker().Helper()
	pager := b.InternalClient.NewListContainersPager(&service.ListContainersOptions{})
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

func (b *BlobServiceResourceManager) URI(opts ...GetURIOptions) string {
	base := blobStripSAS(b.InternalClient.URL())
	base = b.InternalAccount.ApplySAS(base, b.Location(), opts...)
	base = addWildCard(base, opts...)

	return base
}

func (b *BlobServiceResourceManager) Location() common.Location {
	return common.ELocation.Blob()
}

func (b *BlobServiceResourceManager) Level() cmd.LocationLevel {
	return cmd.ELocationLevel.Service()
}

func (b *BlobServiceResourceManager) ResourceClient() any {
	return b.InternalClient
}

func (b *BlobServiceResourceManager) GetContainer(name string) ContainerResourceManager {
	containerClient := b.InternalClient.NewContainerClient(name)
	return &BlobContainerResourceManager{
		InternalAccount:       b.InternalAccount,
		Service:               b,
		InternalContainerName: name,
		InternalClient:        containerClient,
	}
}

func (b *BlobServiceResourceManager) IsHierarchical() bool {
	return b.InternalAccount.AccountType() == EAccountType.HierarchicalNamespaceEnabled()
}

// ==================== CONTAINER ====================

type BlobContainerResourceManager struct {
	InternalAccount       *AzureAccountResourceManager
	Service               *BlobServiceResourceManager
	InternalContainerName string
	InternalClient        *container.Client
}

func (b *BlobContainerResourceManager) GetDatalakeContainerManager(a Asserter) ContainerResourceManager {
	return b.InternalAccount.GetService(a, common.ELocation.BlobFS()).GetContainer(b.InternalContainerName)
}

func (b *BlobContainerResourceManager) ValidAuthTypes() ExplicitCredentialTypes {
	return (&BlobServiceResourceManager{}).ValidAuthTypes()
}

func (b *BlobContainerResourceManager) DefaultAuthType() ExplicitCredentialTypes {
	return (&BlobServiceResourceManager{}).DefaultAuthType()
}

func (b *BlobContainerResourceManager) WithSpecificAuthType(cred ExplicitCredentialTypes, a Asserter, opts ...CreateAzCopyTargetOptions) AzCopyTarget {
	a.HelperMarker().Helper()
	return CreateAzCopyTarget(b, cred, a, opts...)
}

func (b *BlobContainerResourceManager) Canon() string {
	return buildCanonForAzureResourceManager(b)
}

func (b *BlobContainerResourceManager) Exists() bool {
	_, err := b.InternalClient.GetProperties(ctx, nil)

	return err == nil || !bloberror.HasCode(err, bloberror.ContainerNotFound, bloberror.ContainerBeingDeleted, bloberror.ResourceNotFound)
}

func (b *BlobContainerResourceManager) Account() AccountResourceManager {
	return b.InternalAccount
}

func (b *BlobContainerResourceManager) Parent() ResourceManager {
	return b.Service
}

var premiumRegex = regexp.MustCompile("P\\d{2}")

func (b *BlobContainerResourceManager) ListObjects(a Asserter, prefix string, recursive bool) map[string]ObjectProperties {
	a.HelperMarker().Helper()
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
				VersionId: v.VersionID,
				Type:      v.Properties.BlobType,
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
		pager := b.InternalClient.NewListBlobsHierarchyPager("/", &container.ListBlobsHierarchyOptions{
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
		pager := b.InternalClient.NewListBlobsFlatPager(&container.ListBlobsFlatOptions{
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

func (b *BlobContainerResourceManager) Create(a Asserter, props ContainerProperties) {
	a.HelperMarker().Helper()
	b.CreateWithOptions(a, &BlobContainerCreateOptions{
		Access:       props.BlobContainerProperties.Access,
		Metadata:     props.Metadata,
		CPKScopeInfo: props.BlobContainerProperties.CPKScopeInfo,
	})
}

func (b *BlobContainerResourceManager) GetProperties(a Asserter) ContainerProperties {
	a.HelperMarker().Helper()
	props, err := b.InternalClient.GetProperties(ctx, nil)
	a.NoError("Get container properties", err)

	return ContainerProperties{
		Metadata: props.Metadata,
		BlobContainerProperties: BlobContainerProperties{
			Access: props.BlobPublicAccess,
		},
	}
}

type BlobContainerCreateOptions = container.CreateOptions

func (b *BlobContainerResourceManager) CreateWithOptions(a Asserter, options *BlobContainerCreateOptions) {
	a.HelperMarker().Helper()
	_, err := b.InternalClient.Create(ctx, options)

	created := true
	if bloberror.HasCode(err, bloberror.ContainerAlreadyExists) {
		created = false
		err = nil
	}

	a.NoError("create container", err)
	if created {
		TrackResourceCreation(a, b)
	}
}

func (b *BlobContainerResourceManager) GetObject(a Asserter, path string, eType common.EntityType) ObjectResourceManager {
	return &BlobObjectResourceManager{
		internalAccount: b.InternalAccount,
		Service:         b.Service,
		Container:       b,
		Path:            path,
		entityType:      eType,
		internalClient:  b.InternalClient.NewBlobClient(path),
	}
}

func (b *BlobContainerResourceManager) ResourceClient() any {
	return b.InternalClient
}

func (b *BlobContainerResourceManager) Location() common.Location {
	return b.Service.Location()
}

func (b *BlobContainerResourceManager) Level() cmd.LocationLevel {
	return cmd.ELocationLevel.Container()
}

func (b *BlobContainerResourceManager) URI(opts ...GetURIOptions) string {
	base := blobStripSAS(b.InternalClient.URL())
	base = b.InternalAccount.ApplySAS(base, b.Location(), opts...)
	base = addWildCard(base, opts...)

	return base
}

func (b *BlobContainerResourceManager) HasMetadata() PropertiesAvailability {
	return PropertiesAvailabilityReadWrite
}

func (b *BlobContainerResourceManager) GetMetadata(a Asserter) common.Metadata {
	a.HelperMarker().Helper()
	resp, err := b.InternalClient.GetProperties(ctx, &container.GetPropertiesOptions{})
	a.NoError("Get container properties", err)

	return resp.Metadata
}

func (b *BlobContainerResourceManager) SetMetadata(a Asserter, metadata common.Metadata) {
	a.HelperMarker().Helper()
	_, err := b.InternalClient.SetMetadata(ctx, &container.SetMetadataOptions{Metadata: metadata})
	a.NoError("Set container metadata", err)
}

func (b *BlobContainerResourceManager) Delete(a Asserter) {
	a.HelperMarker().Helper()
	_, err := b.InternalClient.Delete(ctx, nil)
	a.NoError("Delete container", err)
}

func (b *BlobContainerResourceManager) ContainerName() string {
	return b.InternalContainerName
}

// ==================== OBJECT ====================

type BlobObjectResourceManager struct {
	internalAccount *AzureAccountResourceManager
	Service         *BlobServiceResourceManager
	Container       *BlobContainerResourceManager
	Path            string
	entityType      common.EntityType

	internalClient *blob.Client
}

func (b *BlobObjectResourceManager) ValidAuthTypes() ExplicitCredentialTypes {
	return (&BlobServiceResourceManager{}).ValidAuthTypes()
}

func (b *BlobObjectResourceManager) ResourceClient() any {
	return b.internalClient
}

func (b *BlobObjectResourceManager) DefaultAuthType() ExplicitCredentialTypes {
	return (&BlobServiceResourceManager{}).ValidAuthTypes()
}

func (b *BlobObjectResourceManager) WithSpecificAuthType(cred ExplicitCredentialTypes, a Asserter, opts ...CreateAzCopyTargetOptions) AzCopyTarget {
	a.HelperMarker().Helper()
	return CreateAzCopyTarget(b, cred, a, opts...)
}

func (b *BlobObjectResourceManager) Canon() string {
	return buildCanonForAzureResourceManager(b)
}

func (b *BlobObjectResourceManager) Account() AccountResourceManager {
	return b.internalAccount
}

func (b *BlobObjectResourceManager) Parent() ResourceManager {
	return b.Container
}

func (b *BlobObjectResourceManager) EntityType() common.EntityType {
	return b.entityType
}

func (b *BlobObjectResourceManager) ContainerName() string {
	return b.Container.ContainerName()
}

func (b *BlobObjectResourceManager) ObjectName() string {
	return b.Path
}

// Create defaults to Block Blob. For implementation-specific options, GetTypeOrZero[T] / GetTypeOrAssert[T] to BlobObjectResourceManager and call CreateWithOptions
func (b *BlobObjectResourceManager) Create(a Asserter, body ObjectContentContainer, properties ObjectProperties) {
	a.HelperMarker().Helper()
	b.CreateWithOptions(a, body, properties, nil)
}

func (b *BlobObjectResourceManager) Delete(a Asserter) {
	a.HelperMarker().Helper()
	_, err := b.internalClient.Delete(ctx, nil)

	if bloberror.HasCode(err, bloberror.BlobNotFound, bloberror.ResourceNotFound, bloberror.ContainerNotFound) {
		err = nil
	}

	a.NoError("delete blob", err)
}

type BlobObjectCreateOptions struct {
	BlockSize  *int64
	CpkOptions common.CpkOptions
}

func (b *BlobObjectResourceManager) CreateWithOptions(a Asserter, body ObjectContentContainer, properties ObjectProperties, options *BlobObjectCreateOptions) {
	a.HelperMarker().Helper()
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

		_, err := b.Container.InternalClient.NewBlockBlobClient(b.Path).UploadStream(ctx, body.Reader(), &blockblob.UploadStreamOptions{
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
		// TODO : Investigate bug in multistep uploader for PageBlob. (WI 28334208)
		client := b.Container.InternalClient.NewPageBlobClient(b.Path)
		blockSize := DerefOrDefault(opts.BlockSize, common.DefaultPageBlobChunkSize)
		size := body.Size()
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
		a.NoError("Page blob create", err)

		msu := &MultiStepUploader{BlockSize: blockSize}
		blockCount := msu.GetBlockCount(size)
		reader := body.Reader()

		offset := int64(0)
		blockIndex := int64(0)

		for range blockCount {
			buf := make([]byte, blockSize)
			n, err := reader.Read(buf)
			if err != nil && err != io.EOF {
				a.Assert(fmt.Sprintf("failed to read content (offset %d (block %d/%d), total %d): %s", offset, blockIndex, blockCount, size, err.Error()), Equal{}, true)
			}
			buf = buf[:n] // reduce buffer size for block

			_, err = client.UploadPages(
				ctx,
				streaming.NopCloser(bytes.NewReader(buf)),
				blob.HTTPRange{Offset: offset, Count: int64(n)},
				&pageblob.UploadPagesOptions{
					TransactionalValidation: blob.TransferValidationTypeComputeCRC64(),
					CPKInfo:                 opts.CpkOptions.GetCPKInfo(),
					CPKScopeInfo:            opts.CpkOptions.GetCPKScopeInfo(),
				})
			a.NoError("Page blob upload", err)
			offset += int64(n)
			blockIndex++
		}
	case blob.BlobTypeAppendBlob:
		// TODO : Investigate bug in multistep uploader for AppendBlob. (WI 28334208)
		blockSize := DerefOrDefault(opts.BlockSize, common.DefaultBlockBlobBlockSize)
		size := body.Size()

		if size < blockSize*common.MaxNumberOfBlocksPerBlob {
			// resize until fits
			for ; size >= common.MaxNumberOfBlocksPerBlob*blockSize; blockSize = 2 * blockSize {
			}
		}

		client := b.Container.InternalClient.NewAppendBlobClient(b.Path)

		_, err := client.Create(ctx, &appendblob.CreateOptions{
			HTTPHeaders:  properties.HTTPHeaders.ToBlob(),
			CPKInfo:      opts.CpkOptions.GetCPKInfo(),
			CPKScopeInfo: opts.CpkOptions.GetCPKScopeInfo(),
			Tags:         blobProps.Tags,
			Metadata:     properties.Metadata,
		})
		a.NoError("Append blob create", err)

		msu := &MultiStepUploader{BlockSize: blockSize}
		blockCount := msu.GetBlockCount(size)
		reader := body.Reader()

		offset := int64(0)
		blockIndex := int64(0)

		for range blockCount {
			buf := make([]byte, blockSize)
			n, err := reader.Read(buf)
			if err != nil && err != io.EOF {
				a.Assert(fmt.Sprintf("failed to read content (offset %d (block %d/%d), total %d): %s", offset, blockIndex, blockCount, size, err.Error()), Equal{}, true)
			}
			buf = buf[:n] // reduce buffer size for block

			_, err = client.AppendBlock(ctx, streaming.NopCloser(bytes.NewReader(buf)), &appendblob.AppendBlockOptions{
				TransactionalValidation: blob.TransferValidationTypeComputeCRC64(),
				AppendPositionAccessConditions: &appendblob.AppendPositionAccessConditions{
					AppendPosition: pointerTo(offset),
					MaxSize:        pointerTo(offset + int64(n)),
				},
				CPKInfo:      opts.CpkOptions.GetCPKInfo(),
				CPKScopeInfo: opts.CpkOptions.GetCPKScopeInfo(),
			})
			a.NoError("Append blob upload", err)
			offset += int64(n)
			blockIndex++
		}
	}

	TrackResourceCreation(a, b)
}

func (b *BlobObjectResourceManager) ListChildren(a Asserter, recursive bool) map[string]ObjectProperties {
	a.HelperMarker().Helper()
	return b.Container.ListObjects(a, b.Path, recursive)
}

func (b *BlobObjectResourceManager) GetProperties(a Asserter) ObjectProperties {
	a.HelperMarker().Helper()
	return b.GetPropertiesWithOptions(a, nil)
}

type BlobObjectGetPropertiesOptions struct {
	CPKOptions common.CpkOptions
}

func (b *BlobObjectResourceManager) GetPropertiesWithOptions(a Asserter, options *BlobObjectGetPropertiesOptions) ObjectProperties {
	a.HelperMarker().Helper()

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
		LastModifiedTime: func() *time.Time {
			if resp.LastModified == nil {
				return nil
			}
			return to.Ptr(resp.LastModified.UTC())
		}(),
		BlobProperties: BlobProperties{
			VersionId: resp.VersionID,
			Type:      resp.BlobType,
			Tags: func() map[string]string {
				out := make(map[string]string)
				if b.internalAccount.AccountType() == EAccountType.PremiumPageBlobs() || b.internalAccount.AccountType() == EAccountType.HierarchicalNamespaceEnabled() {
					return out
				}

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
			BlockBlobAccessTier: func() *blob.AccessTier {
				if resp.AccessTier == nil {
					return nil
				}
				return to.Ptr(blob.AccessTier(*resp.AccessTier))
			}(),
			PageBlobAccessTier: func() *pageblob.PremiumPageBlobAccessTier {
				if resp.AccessTier == nil {
					return nil
				}
				return to.Ptr(pageblob.PremiumPageBlobAccessTier(*resp.AccessTier))
			}(),
			LeaseState:    resp.LeaseState,
			LeaseDuration: resp.LeaseDuration,
			LeaseStatus:   resp.LeaseStatus,
			ArchiveStatus: func() *blob.ArchiveStatus {
				if resp.ArchiveStatus == nil {
					return nil
				}
				return to.Ptr(blob.ArchiveStatus(*resp.ArchiveStatus))
			}(),
		},
	}
}

func (b *BlobObjectResourceManager) SetHTTPHeaders(a Asserter, h contentHeaders) {
	a.HelperMarker().Helper()
	_, err := b.internalClient.SetHTTPHeaders(ctx, DerefOrZero(h.ToBlob()), nil)
	a.NoError("Set HTTP Headers", err)
}

func (b *BlobObjectResourceManager) SetMetadata(a Asserter, metadata common.Metadata) {
	a.HelperMarker().Helper()
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

func (b *BlobObjectResourceManager) URI(opts ...GetURIOptions) string {
	base := blobStripSAS(b.internalClient.URL())
	base = b.internalAccount.ApplySAS(base, b.Location(), opts...)
	base = addWildCard(base, opts...)

	return base
}

func (b *BlobObjectResourceManager) Download(a Asserter) io.ReadSeeker {
	a.HelperMarker().Helper()
	resp, err := b.internalClient.DownloadStream(ctx, nil)
	a.NoError("Download stream", err)

	if resp.Body == nil {
		return bytes.NewReader(make([]byte, 0))
	}

	buf := &bytes.Buffer{}
	if err == nil && resp.Body != nil {
		_, err = io.Copy(buf, resp.Body)
		a.NoError("Read body", err)
	}

	return bytes.NewReader(buf.Bytes())
}

func (b *BlobObjectResourceManager) Exists() bool {
	_, err := b.internalClient.GetProperties(ctx, nil)

	return err == nil || !bloberror.HasCode(err, bloberror.BlobNotFound, bloberror.ContainerNotFound, bloberror.ContainerBeingDeleted, bloberror.ResourceNotFound)
}
