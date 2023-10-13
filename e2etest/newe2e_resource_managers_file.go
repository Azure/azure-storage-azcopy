package e2etest

import (
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/directory"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/file"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/service"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/share"
	"github.com/Azure/azure-storage-azcopy/v10/cmd"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/sddl"
	"github.com/Azure/azure-storage-azcopy/v10/ste"
	"path"
	"runtime"
)

// check that everything aligns with interfaces
func init() {
	void := func(_ ...any) {} // prevent go from erroring from unused vars

	var sm ServiceResourceManager = &FileServiceResourceManager{}
	var cm ContainerResourceManager = &FileShareResourceManager{}
	var om ObjectResourceManager = &FileObjectResourceManager{}

	var rrm RemoteResourceManager

	rrm = &FileServiceResourceManager{}
	rrm = &FileShareResourceManager{}
	rrm = &FileObjectResourceManager{}

	void(rrm, sm, cm, om)
}

// ==================== SERVICE ====================

type FileServiceResourceManager struct {
	Account        AccountResourceManager
	internalClient *service.Client
}

func (s *FileServiceResourceManager) Location() common.Location {
	return common.ELocation.File()
}

func (s *FileServiceResourceManager) Level() cmd.LocationLevel {
	return cmd.ELocationLevel.Object()
}

func (s *FileServiceResourceManager) URI() string {
	return s.internalClient.URL()
}

func (s *FileServiceResourceManager) ValidAuthTypes() ExplicitCredentialTypes {
	return EExplicitCredentialType.With(EExplicitCredentialType.SASToken())
}

func (s *FileServiceResourceManager) ResourceClient() any {
	return s.internalClient
}

func (s *FileServiceResourceManager) ListContainers(a Asserter) []string {
	pager := s.internalClient.NewListSharesPager(nil)
	out := make([]string, 0)

	for pager.More() {
		page, err := pager.NextPage(ctx)
		a.NoError("get page", err)

		for _, shareEntry := range page.Shares {
			if shareEntry == nil || shareEntry.Name == nil {
				continue
			}

			out = append(out, *shareEntry.Name)
		}
	}

	return out
}

func (s *FileServiceResourceManager) GetContainer(container string) ContainerResourceManager {
	return &FileShareResourceManager{
		Account:        s.Account,
		Service:        s,
		containerName:  container,
		internalClient: s.internalClient.NewShareClient(container),
	}
}

func (s *FileServiceResourceManager) IsHierarchical() bool {
	return true
}

// ==================== CONTAINER ====================

type FileShareResourceManager struct {
	Account AccountResourceManager
	Service *FileServiceResourceManager

	containerName  string
	internalClient *share.Client
}

func (s *FileShareResourceManager) ValidAuthTypes() ExplicitCredentialTypes {
	return s.Service.ValidAuthTypes()
}

func (s *FileShareResourceManager) ResourceClient() any {
	return s.internalClient
}

func (s *FileShareResourceManager) Location() common.Location {
	return s.Service.Location()
}

func (s *FileShareResourceManager) Level() cmd.LocationLevel {
	return cmd.ELocationLevel.Container()
}

func (s *FileShareResourceManager) URI() string {
	return s.internalClient.URL()
}

func (s *FileShareResourceManager) ContainerName() string {
	return s.containerName
}

func (s *FileShareResourceManager) Create(a Asserter) {
	s.CreateWithOptions(a, nil)
}

type FileShareCreateOptions = share.CreateOptions

func (s *FileShareResourceManager) CreateWithOptions(a Asserter, options *FileShareCreateOptions) {
	_, err := s.internalClient.Create(ctx, options)
	a.NoError("Create container", err)

}

func (s *FileShareResourceManager) Delete(a Asserter) {
	s.DeleteWithOptions(a, nil)
}

type FileShareDeleteOptions = share.DeleteOptions

func (s *FileShareResourceManager) DeleteWithOptions(a Asserter, options *FileShareDeleteOptions) {
	_, err := s.internalClient.Delete(ctx, options)
	a.NoError("delete share", err)
}

func (s *FileShareResourceManager) ListObjects(a Asserter, targetDir string, recursive bool) map[string]ObjectProperties {
	queue := []string{targetDir}
	out := make(map[string]ObjectProperties)

	for len(queue) > 0 {
		parent := queue[0] // pop from queue
		queue = queue[1:]

		dirClient := s.internalClient.NewDirectoryClient(parent)
		pager := dirClient.NewListFilesAndDirectoriesPager(&directory.ListFilesAndDirectoriesOptions{
			Include:             directory.ListFilesInclude{Timestamps: true, Attributes: true, PermissionKey: true},
			IncludeExtendedInfo: pointerTo(true),
		})

		for pager.More() {
			page, err := pager.NextPage(ctx)
			a.NoError("Get page", err)

			// List directories and add to queue
			for _, v := range page.Segment.Directories {
				fullPath := path.Join(parent, *v.Name)

				if recursive {
					queue = append(queue, fullPath)
				}

				subdirClient := s.internalClient.NewDirectoryClient(fullPath)
				resp, err := subdirClient.GetProperties(ctx, &directory.GetPropertiesOptions{})
				a.NoError("Get dir properties", err)

				var permissions *string
				if resp.FilePermissionKey != nil {
					permResp, err := s.internalClient.GetPermission(ctx, *v.PermissionKey, nil)
					a.NoError("Get permissions", err)
					permissions = permResp.Permission
				}

				out[fullPath] = ObjectProperties{
					EntityType: common.EEntityType.Folder(),
					Metadata:   resp.Metadata,
					FileProperties: FileProperties{
						FileAttributes:    v.Attributes,
						FileChangeTime:    v.Properties.ChangeTime,
						FileCreationTime:  v.Properties.CreationTime,
						FileLastWriteTime: v.Properties.LastWriteTime,
						FilePermissions:   permissions,
					},
				}
			}

			// List files
			for _, v := range page.Segment.Files {
				fullPath := path.Join(parent, *v.Name)

				fileClient := s.internalClient.NewRootDirectoryClient().NewFileClient(fullPath)
				resp, err := fileClient.GetProperties(ctx, &file.GetPropertiesOptions{})
				a.NoError("Get file properties", err)

				var permissions *string
				if resp.FilePermissionKey != nil {
					permResp, err := s.internalClient.GetPermission(ctx, *v.PermissionKey, nil)
					a.NoError("Get permissions", err)
					permissions = permResp.Permission
				}

				out[fullPath] = ObjectProperties{
					EntityType: common.EEntityType.Folder(),
					HTTPHeaders: contentHeaders{
						cacheControl:       resp.CacheControl,
						contentDisposition: resp.ContentDisposition,
						contentEncoding:    resp.ContentEncoding,
						contentLanguage:    resp.ContentLanguage,
						contentType:        resp.ContentType,
						contentMD5:         resp.ContentMD5,
					},
					Metadata: resp.Metadata,
					FileProperties: FileProperties{
						FileAttributes:    v.Attributes,
						FileChangeTime:    v.Properties.ChangeTime,
						FileCreationTime:  v.Properties.CreationTime,
						FileLastWriteTime: v.Properties.LastWriteTime,
						FilePermissions:   permissions,
					},
				}
			}
		}
	}

	return out
}

func (s *FileShareResourceManager) GetObject(a Asserter, path string, eType common.EntityType) ObjectResourceManager {
	return &FileObjectResourceManager{
		Account:    s.Account,
		Service:    s.Service,
		Share:      s,
		path:       path,
		entityType: eType,
	}
}

// ==================== FILE ====================

type FileObjectResourceManager struct {
	Account AccountResourceManager
	Service *FileServiceResourceManager
	Share   *FileShareResourceManager

	path       string
	entityType common.EntityType
}

func (f *FileObjectResourceManager) ValidAuthTypes() ExplicitCredentialTypes {
	return f.Service.ValidAuthTypes()
}

func (f *FileObjectResourceManager) ResourceClient() any {
	switch f.entityType {
	case common.EEntityType.Folder():
		return f.getDirClient()
	default: // For now, bundle up other entity types as files. That's how they should be implemented in AzCopy, at least.
		return f.getFileClient()
	}
}

func (f *FileObjectResourceManager) Location() common.Location {
	return f.Service.Location()
}

func (f *FileObjectResourceManager) Level() cmd.LocationLevel {
	return cmd.ELocationLevel.Object()
}

func (f *FileObjectResourceManager) URI() string {
	return f.getFileClient().URL()
}

func (f *FileObjectResourceManager) EntityType() common.EntityType {
	return f.entityType
}

func (f *FileObjectResourceManager) PreparePermissions(a Asserter, p *string) *file.Permissions {
	if p == nil {
		return nil
	}
	perm := *p

	fSDDL, err := sddl.ParseSDDL(perm)
	a.NoError("parse input SDDL", err)
	a.AssertNow("parsed string equivalence sanity check", Equal{}, fSDDL.String(), perm)

	perm = fSDDL.PortableString()

	if len(perm) >= ste.FilesServiceMaxSDDLSize {
		resp, err := f.Share.internalClient.CreatePermission(ctx, perm, nil)
		a.NoError("Create share permission", err)
		return &file.Permissions{PermissionKey: resp.FilePermissionKey}
	}

	return &file.Permissions{Permission: &perm}
}

func (f *FileObjectResourceManager) Create(a Asserter, body ObjectContentContainer, props ObjectProperties) {
	var attr *file.NTFSFileAttributes
	if DerefOrZero(props.FileProperties.FileAttributes) != "" {
		var err error
		attr, err = file.ParseNTFSFileAttributes(props.FileProperties.FileAttributes)
		a.NoError("Parse attributes", err)
	}

	perms := f.PreparePermissions(a, props.FileProperties.FilePermissions)

	switch f.entityType {
	case common.EEntityType.File():
		client := f.getFileClient()
		_, err := client.Create(ctx, body.Size(), &file.CreateOptions{
			SMBProperties: &file.SMBProperties{
				Attributes:    attr,
				CreationTime:  props.FileProperties.FileCreationTime,
				LastWriteTime: props.FileProperties.FileLastWriteTime,
			},
			Permissions: perms,
			HTTPHeaders: props.HTTPHeaders.ToFile(),
			Metadata:    props.Metadata,
		})
		a.NoError("Create file", err)
		err = client.UploadStream(ctx, body.Reader(), &file.UploadStreamOptions{
			Concurrency: runtime.NumCPU(),
		})
		a.NoError("Upload Stream", err)
	case common.EEntityType.Folder():
		client := f.getDirClient()
		_, err := client.Create(ctx, &directory.CreateOptions{
			FileSMBProperties: &file.SMBProperties{
				Attributes:    attr,
				CreationTime:  props.FileProperties.FileCreationTime,
				LastWriteTime: props.FileProperties.FileLastWriteTime,
			},
			FilePermissions: perms,
			Metadata:        props.Metadata,
		})
		a.NoError("Create directory", err)
	default:
		a.Error("File Objects only support Files and Folders")
	}
}

func (f *FileObjectResourceManager) ListChildren(a Asserter, recursive bool) map[string]ObjectProperties {
	a.AssertNow("must be folder to list children", Equal{}, f.entityType, common.EEntityType.Folder())

	return f.Share.ListObjects(a, f.path, recursive)
}

func (f *FileObjectResourceManager) GetProperties(a Asserter) (out ObjectProperties) {
	switch f.entityType {
	case common.EEntityType.Folder():
		resp, err := f.getDirClient().GetProperties(ctx, &directory.GetPropertiesOptions{})
		a.NoError("Get directory properties", err)

		var permissions *string
		if pkey := DerefOrZero(resp.FilePermissionKey); pkey != "" {
			permResp, err := f.Share.internalClient.GetPermission(ctx, pkey, nil)
			a.NoError("Get file permissions", err)

			permissions = permResp.Permission
		}

		out = ObjectProperties{
			EntityType: f.entityType,
			Metadata:   resp.Metadata,
			FileProperties: FileProperties{
				FileAttributes:    resp.FileAttributes,
				FileChangeTime:    resp.FileChangeTime,
				FileCreationTime:  resp.FileCreationTime,
				FileLastWriteTime: resp.FileLastWriteTime,
				FilePermissions:   permissions,
			},
		}
	case common.EEntityType.File():
		resp, err := f.getFileClient().GetProperties(ctx, &file.GetPropertiesOptions{})
		a.NoError("Get file properties", err)

		var permissions *string
		if pkey := DerefOrZero(resp.FilePermissionKey); pkey != "" {
			permResp, err := f.Share.internalClient.GetPermission(ctx, pkey, nil)
			a.NoError("Get file permissions", err)

			permissions = permResp.Permission
		}

		out = ObjectProperties{
			EntityType: f.entityType,
			HTTPHeaders: contentHeaders{
				cacheControl:       resp.CacheControl,
				contentDisposition: resp.ContentDisposition,
				contentEncoding:    resp.ContentEncoding,
				contentLanguage:    resp.ContentLanguage,
				contentType:        resp.ContentType,
				contentMD5:         resp.ContentMD5,
			},
			Metadata: resp.Metadata,
			FileProperties: FileProperties{
				FileAttributes:    resp.FileAttributes,
				FileChangeTime:    resp.FileChangeTime,
				FileCreationTime:  resp.FileCreationTime,
				FileLastWriteTime: resp.FileLastWriteTime,
				FilePermissions:   permissions,
			},
		}
	default:
		a.Error("EntityType must be Folder or File. Currently: " + f.entityType.String())
	}

	return
}

func (f *FileObjectResourceManager) SetHTTPHeaders(a Asserter, h contentHeaders) {
	a.AssertNow("HTTP headers are only available on files", Equal{}, f.entityType, common.EEntityType.File())
	client := f.getFileClient()

	_, err := client.SetHTTPHeaders(ctx, &file.SetHTTPHeadersOptions{
		HTTPHeaders: &file.HTTPHeaders{
			CacheControl:       h.cacheControl,
			ContentDisposition: h.contentDisposition,
			ContentEncoding:    h.contentEncoding,
			ContentLanguage:    h.contentLanguage,
			ContentMD5:         h.contentMD5,
			ContentType:        h.contentType,
		},
	})
	a.NoError("Set HTTP Headers", err)
}

func (f *FileObjectResourceManager) SetMetadata(a Asserter, metadata common.Metadata) {
	switch f.entityType {
	case common.EEntityType.File():
		_, err := f.getFileClient().SetMetadata(ctx, &file.SetMetadataOptions{Metadata: metadata})
		a.NoError("Set file metadata", err)
	case common.EEntityType.Folder():
		_, err := f.getDirClient().SetMetadata(ctx, &directory.SetMetadataOptions{Metadata: metadata})
		a.NoError("Set directory metadata", err)
	default:
		a.Error("EntityType must be Folder or File. Currently: " + f.entityType.String())
	}
}

func (f *FileObjectResourceManager) SetObjectProperties(a Asserter, props ObjectProperties) {
	var attr *file.NTFSFileAttributes
	if DerefOrZero(props.FileProperties.FileAttributes) != "" {
		var err error
		attr, err = file.ParseNTFSFileAttributes(props.FileProperties.FileAttributes)
		a.NoError("Parse attributes", err)
	}

	perms := f.PreparePermissions(a, props.FileProperties.FilePermissions)

	switch f.entityType {
	case common.EEntityType.File():
		client := f.getFileClient()
		var _, err = client.SetHTTPHeaders(ctx, &file.SetHTTPHeadersOptions{
			SMBProperties: &file.SMBProperties{
				Attributes:    attr,
				CreationTime:  props.FileProperties.FileCreationTime,
				LastWriteTime: props.FileProperties.FileLastWriteTime,
			},
			Permissions: perms,
			HTTPHeaders: props.HTTPHeaders.ToFile(),
		})
		a.NoError("Set file HTTP headers", err)

		_, err = client.SetMetadata(ctx, &file.SetMetadataOptions{Metadata: props.Metadata})
		a.NoError("Set file metadata", err)
	case common.EEntityType.Folder():
		client := f.getDirClient()
		var _, err = client.SetProperties(ctx, &directory.SetPropertiesOptions{
			FileSMBProperties: &file.SMBProperties{
				Attributes:    attr,
				CreationTime:  props.FileProperties.FileCreationTime,
				LastWriteTime: props.FileProperties.FileLastWriteTime,
			},
			FilePermissions: perms,
		})
		a.NoError("Set folder properties", err)

		_, err = f.getDirClient().SetMetadata(ctx, &directory.SetMetadataOptions{Metadata: props.Metadata})
		a.NoError("Set folder metadata", err)
	}
}

func (f *FileObjectResourceManager) getFileClient() *file.Client {
	return f.Share.internalClient.NewRootDirectoryClient().NewFileClient(f.path)
}

func (f *FileObjectResourceManager) getDirClient() *directory.Client {
	return f.Share.internalClient.NewDirectoryClient(f.path)
}