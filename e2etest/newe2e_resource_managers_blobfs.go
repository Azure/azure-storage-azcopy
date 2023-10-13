package e2etest

import (
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake/directory"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake/file"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake/filesystem"
	datalakeSAS "github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake/sas"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake/service"
	"github.com/Azure/azure-storage-azcopy/v10/cmd"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"runtime"
)

// check that everything aligns with interfaces
func init() {
	void := func(_ ...any) {} // prevent go from erroring from unused vars

	var sm ServiceResourceManager = &BlobFSServiceResourceManager{}
	var cm ContainerResourceManager = &BlobFSFileSystemResourceManager{}
	var om ObjectResourceManager = &BlobFSPathResourceProvider{}

	var rrm RemoteResourceManager

	rrm = &BlobFSServiceResourceManager{}
	rrm = &BlobFSFileSystemResourceManager{}
	rrm = &BlobFSPathResourceProvider{}

	void(rrm, sm, cm, om)
}

func dfsStripSAS(uri string) string {
	parts, err := datalakeSAS.ParseURL(uri)
	common.PanicIfErr(err)

	parts.SAS = datalakeSAS.QueryParameters{} // remove SAS

	return parts.String()
}

type BlobFSServiceResourceManager struct {
	Account AccountResourceManager

	internalClient *service.Client
}

func (b *BlobFSServiceResourceManager) Location() common.Location {
	return common.ELocation.BlobFS()
}

func (b *BlobFSServiceResourceManager) Level() cmd.LocationLevel {
	return cmd.ELocationLevel.Service()
}

func (b *BlobFSServiceResourceManager) URI() string {
	return dfsStripSAS(b.internalClient.DFSURL())
}

func (b *BlobFSServiceResourceManager) ValidAuthTypes() ExplicitCredentialTypes {
	return EExplicitCredentialType.With(EExplicitCredentialType.OAuth(), EExplicitCredentialType.SASToken(), EExplicitCredentialType.AcctKey())
}

func (b *BlobFSServiceResourceManager) ResourceClient() any {
	return b.internalClient
}

func (b *BlobFSServiceResourceManager) ListContainers(a Asserter) []string {
	pager := b.internalClient.NewListFileSystemsPager(nil)

	out := make([]string, 0)

	for pager.More() {
		page, err := pager.NextPage(ctx)
		a.NoError("Get filesystems page", err)
		for _, v := range page.FileSystemItems {
			if v == nil || v.Name == nil {
				continue
			}

			out = append(out, *v.Name)
		}
	}

	return out
}

func (b *BlobFSServiceResourceManager) GetContainer(containerName string) ContainerResourceManager {
	return &BlobFSFileSystemResourceManager{
		Account:        b.Account,
		Service:        b,
		containerName:  containerName,
		internalClient: b.internalClient.NewFileSystemClient(containerName),
	}
}

func (b *BlobFSServiceResourceManager) IsHierarchical() bool {
	return true
}

type BlobFSFileSystemResourceManager struct {
	Account AccountResourceManager
	Service *BlobFSServiceResourceManager

	containerName  string
	internalClient *filesystem.Client
}

func (b *BlobFSFileSystemResourceManager) ValidAuthTypes() ExplicitCredentialTypes {
	return b.Service.ValidAuthTypes()
}

func (b *BlobFSFileSystemResourceManager) ResourceClient() any {
	return b.internalClient
}

func (b *BlobFSFileSystemResourceManager) Location() common.Location {
	return b.Service.Location()
}

func (b *BlobFSFileSystemResourceManager) Level() cmd.LocationLevel {
	return cmd.ELocationLevel.Container()
}

func (b *BlobFSFileSystemResourceManager) URI() string {
	return dfsStripSAS(b.internalClient.DFSURL())
}

func (b *BlobFSFileSystemResourceManager) ContainerName() string {
	return b.containerName
}

func (b *BlobFSFileSystemResourceManager) Create(a Asserter) {
	b.CreateWithOptions(a, nil)
}

func (b *BlobFSFileSystemResourceManager) CreateWithOptions(a Asserter, opts *filesystem.CreateOptions) {
	_, err := b.internalClient.Create(ctx, opts)
	a.NoError("Create filesystem", err)
}

func (b *BlobFSFileSystemResourceManager) Delete(a Asserter) {
	b.DeleteWithOptions(a, nil)
}

func (b *BlobFSFileSystemResourceManager) DeleteWithOptions(a Asserter, opts *filesystem.DeleteOptions) {
	_, err := b.internalClient.Delete(ctx, opts)
	a.NoError("Delete filesystem", err)
}

func (b *BlobFSFileSystemResourceManager) ListObjects(a Asserter, prefixOrDirectory string, recursive bool) map[string]ObjectProperties {
	pager := b.internalClient.NewListPathsPager(recursive, &filesystem.ListPathsOptions{
		Prefix: &prefixOrDirectory,
	})

	out := make(map[string]ObjectProperties)
	for pager.More() {
		page, err := pager.NextPage(ctx)
		a.NoError("Get next page", err)

		for _, v := range page.Paths {
			out[*v.Name] = ObjectProperties{
				EntityType:     0,
				HTTPHeaders:    contentHeaders{},
				Metadata:       nil,
				BlobProperties: BlobProperties{},
				FileProperties: FileProperties{},
			}

		}
	}

	return out
}

func (b *BlobFSFileSystemResourceManager) GetObject(a Asserter, path string, eType common.EntityType) ObjectResourceManager {
	//TODO implement me
	panic("implement me")
}

type BlobFSPathResourceProvider struct {
	Account   AccountResourceManager
	Service   *BlobFSServiceResourceManager
	Container *BlobFSFileSystemResourceManager

	entityType common.EntityType
	objectPath string
}

func (b *BlobFSPathResourceProvider) ValidAuthTypes() ExplicitCredentialTypes {
	return b.Service.ValidAuthTypes()
}

func (b *BlobFSPathResourceProvider) ResourceClient() any {
	switch b.entityType {
	case common.EEntityType.Folder():
		return b.getDirClient()
	default: // lump files in with other types, because that's how they're implemented in azcopy
		return b.getFileClient()
	}
}

func (b *BlobFSPathResourceProvider) Location() common.Location {
	return b.Service.Location()
}

func (b *BlobFSPathResourceProvider) Level() cmd.LocationLevel {
	return cmd.ELocationLevel.Object()
}

func (b *BlobFSPathResourceProvider) URI() string {
	return b.getFileClient().DFSURL()
}

func (b *BlobFSPathResourceProvider) EntityType() common.EntityType {
	return b.entityType
}

func (b *BlobFSPathResourceProvider) Create(a Asserter, body ObjectContentContainer, properties ObjectProperties) {
	switch b.entityType {
	case common.EEntityType.Folder():
		_, err := b.getDirClient().Create(ctx, &directory.CreateOptions{
			HTTPHeaders: properties.HTTPHeaders.ToBlobFS(),
			Permissions: properties.BlobFSProperties.Permissions,
			Owner:       properties.BlobFSProperties.Owner,
			Group:       properties.BlobFSProperties.Group,
			ACL:         properties.BlobFSProperties.ACL,
		})
		a.NoError("Create directory", err)
	case common.EEntityType.File(), common.EEntityType.Symlink(): // Symlinks just need an extra metadata tag
		_, err := b.getFileClient().Create(ctx, &file.CreateOptions{
			HTTPHeaders: properties.HTTPHeaders.ToBlobFS(),
		})
		a.NoError("Create file", err)

		err = b.getFileClient().UploadStream(ctx, body.Reader(), &file.UploadStreamOptions{
			Concurrency: uint16(runtime.NumCPU()),
			HTTPHeaders: properties.HTTPHeaders.ToBlobFS(),
		})
		a.NoError("Upload stream", err)

		_, err = b.getFileClient().SetAccessControl(ctx, &file.SetAccessControlOptions{ // Set access control after we write to prevent locking ourselves out
			Permissions: properties.BlobFSProperties.Permissions,
			Owner:       properties.BlobFSProperties.Owner,
			Group:       properties.BlobFSProperties.Group,
			ACL:         properties.BlobFSProperties.ACL,
		})
		a.NoError("Set access control", err)
	}

	meta := properties.Metadata
	if b.entityType == common.EEntityType.Symlink() {
		meta = make(common.Metadata)

		for k, v := range properties.Metadata {
			meta[k] = v
		}

		meta[common.POSIXSymlinkMeta] = pointerTo("true")
	}
	b.SetMetadata(a, meta)

	blobClient := b.getBlobClient(a)
	if properties.BlobProperties.Tags != nil {
		_, err := blobClient.SetTags(ctx, properties.BlobProperties.Tags, nil)
		a.NoError("Set tags", err)
	}

	if properties.BlobProperties.BlockBlobAccessTier != nil {
		_, err := blobClient.SetTier(ctx, *properties.BlobProperties.BlockBlobAccessTier, nil)
		a.NoError("Set tier", err)
	}
}

func (b *BlobFSPathResourceProvider) ListChildren(a Asserter, recursive bool) map[string]ObjectProperties {
	return b.Container.ListObjects(a, b.objectPath, recursive)
}

func (b *BlobFSPathResourceProvider) GetProperties(a Asserter) ObjectProperties {
	return b.GetPropertiesWithOptions(a, nil)
}

type BlobFSPathGetPropertiesOptions struct {
	AccessConditions *file.AccessConditions
	CPKInfo          *file.CPKInfo
	UPN              *bool
}

func (b *BlobFSPathResourceProvider) GetPropertiesWithOptions(a Asserter, options *BlobFSPathGetPropertiesOptions) ObjectProperties {
	opts := DerefOrZero(options)

	// As far as BlobFS (and it's SDK) are concerned, the REST API call is the same for files and directories. Using the same call doesn't hurt.
	resp, err := b.getFileClient().GetProperties(ctx, &file.GetPropertiesOptions{
		AccessConditions: opts.AccessConditions,
		CPKInfo:          opts.CPKInfo,
	})
	a.NoError("Get properties", err)

	permResp, err := b.getFileClient().GetAccessControl(ctx, &file.GetAccessControlOptions{
		UPN:              options.UPN,
		AccessConditions: opts.AccessConditions,
	})

	return ObjectProperties{
		EntityType: 0,
		HTTPHeaders: contentHeaders{
			cacheControl:       resp.CacheControl,
			contentDisposition: resp.ContentDisposition,
			contentEncoding:    resp.ContentEncoding,
			contentLanguage:    resp.ContentLanguage,
			contentType:        resp.ContentType,
			contentMD5:         resp.ContentMD5,
		},
		Metadata: resp.Metadata,
		BlobFSProperties: BlobFSProperties{
			Permissions: resp.Permissions,
			Owner:       resp.Owner,
			Group:       resp.Group,
			ACL:         permResp.ACL,
		},
	}
}

func (b *BlobFSPathResourceProvider) SetHTTPHeaders(a Asserter, h contentHeaders) {
	_, err := b.getFileClient().SetHTTPHeaders(ctx, DerefOrZero(h.ToBlobFS()), nil)
	a.NoError("Set HTTP headers", err)
}

func (b *BlobFSPathResourceProvider) SetMetadata(a Asserter, metadata common.Metadata) {
	_, err := b.getFileClient().SetMetadata(ctx, metadata, nil)
	a.NoError("Set metadata", err)
}

func (b *BlobFSPathResourceProvider) SetObjectProperties(a Asserter, props ObjectProperties) {
	b.SetHTTPHeaders(a, props.HTTPHeaders)
	b.SetMetadata(a, props.Metadata)

	_, err := b.getFileClient().SetAccessControl(ctx, &file.SetAccessControlOptions{
		Owner:       props.BlobFSProperties.Owner,
		Group:       props.BlobFSProperties.Group,
		ACL:         props.BlobFSProperties.ACL,
		Permissions: props.BlobFSProperties.Permissions,
	})
	a.NoError("Set access control", err)

	blobClient := b.getBlobClient(a)
	if props.BlobProperties.Tags != nil {
		_, err := blobClient.SetTags(ctx, props.BlobProperties.Tags, nil)
		a.NoError("Set tags", err)
	}

	if props.BlobProperties.BlockBlobAccessTier != nil {
		_, err := blobClient.SetTier(ctx, *props.BlobProperties.BlockBlobAccessTier, nil)
		a.NoError("Set tier", err)
	}
}

func (b *BlobFSPathResourceProvider) getDirClient() *directory.Client {
	return b.Container.internalClient.NewDirectoryClient(b.objectPath)
}

func (b *BlobFSPathResourceProvider) getFileClient() *file.Client {
	return b.Container.internalClient.NewFileClient(b.objectPath)
}

func (b *BlobFSPathResourceProvider) getBlobClient(a Asserter) *blob.Client {
	blobService := b.Account.GetService(a, common.ELocation.Blob()).(*BlobServiceResourceManager) // Blob and BlobFS are synonymous, so simply getting the same path is fine.
	container := blobService.internalClient.NewContainerClient(b.Container.containerName)
	return container.NewBlobClient(b.objectPath) // Generic blob client for now, we can specialize if we want in the future.
}