package e2etest

import (
	"bytes"
	"fmt"
	"io"
	"path"
	"runtime"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/directory"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/file"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/fileerror"
	filesas "github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/sas"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/service"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/share"
	"github.com/Azure/azure-storage-azcopy/v10/azcopy"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/sddl"
	"github.com/Azure/azure-storage-azcopy/v10/ste"
)

// check that everything complies with interfaces
func init() {
	void := func(_ ...any) {} // prevent go from erroring from unused vars

	void(
		ServiceResourceManager(&FileServiceResourceManager{}),
		ContainerResourceManager(&FileShareResourceManager{}),
		ObjectResourceManager(&FileObjectResourceManager{}),

		RemoteResourceManager(&FileServiceResourceManager{}),
		RemoteResourceManager(&FileShareResourceManager{}),
		RemoteResourceManager(&FileObjectResourceManager{}),
	)
}

func fileStripSAS(uri string) string {
	parts, err := filesas.ParseURL(uri)
	common.PanicIfErr(err)

	parts.SAS = filesas.QueryParameters{} // remove SAS

	return parts.String()
}

// ==================== SERVICE ====================

type FileServiceResourceManager struct {
	InternalAccount *AzureAccountResourceManager
	InternalClient  *service.Client
	Llocation       common.Location
}

func (s *FileServiceResourceManager) DefaultAuthType() ExplicitCredentialTypes {
	return EExplicitCredentialType.SASToken()
}

func (s *FileServiceResourceManager) ValidAuthTypes() ExplicitCredentialTypes {
	return EExplicitCredentialType.With(EExplicitCredentialType.SASToken(), EExplicitCredentialType.OAuth())
}

func (s *FileServiceResourceManager) WithSpecificAuthType(cred ExplicitCredentialTypes, a Asserter, opts ...CreateAzCopyTargetOptions) AzCopyTarget {
	return CreateAzCopyTarget(s, cred, a, opts...)
}

func (s *FileServiceResourceManager) Canon() string {
	return buildCanonForAzureResourceManager(s)
}

func (s *FileServiceResourceManager) Account() AccountResourceManager {
	return s.InternalAccount
}

func (s *FileServiceResourceManager) Parent() ResourceManager {
	return nil
}

func (s *FileServiceResourceManager) Location() common.Location {
	return s.Llocation
}

func (s *FileServiceResourceManager) Level() azcopy.LocationLevel {
	return azcopy.ELocationLevel.Service()
}

func (s *FileServiceResourceManager) URI(opts ...GetURIOptions) string {
	base := fileStripSAS(s.InternalClient.URL())
	base = s.InternalAccount.ApplySAS(base, s.Location(), opts...)
	base = addWildCard(base, opts...)

	return base
}

func (s *FileServiceResourceManager) ResourceClient() any {
	return s.InternalClient
}

func (s *FileServiceResourceManager) ListContainers(a Asserter) []string {
	a.HelperMarker().Helper()
	pager := s.InternalClient.NewListSharesPager(nil)
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
		InternalAccount:       s.InternalAccount,
		Service:               s,
		InternalContainerName: container,
		InternalClient:        s.InternalClient.NewShareClient(container),
	}
}

func (s *FileServiceResourceManager) IsHierarchical() bool {
	return true
}

// ==================== CONTAINER ====================

type FileShareResourceManager struct {
	InternalAccount *AzureAccountResourceManager
	Service         *FileServiceResourceManager

	InternalContainerName string
	InternalClient        *share.Client
}

func (s *FileShareResourceManager) DefaultAuthType() ExplicitCredentialTypes {
	return (&FileServiceResourceManager{}).DefaultAuthType()
}

func (s *FileShareResourceManager) ValidAuthTypes() ExplicitCredentialTypes {
	return (&FileServiceResourceManager{}).ValidAuthTypes()
}

func (s *FileShareResourceManager) WithSpecificAuthType(cred ExplicitCredentialTypes, a Asserter, opts ...CreateAzCopyTargetOptions) AzCopyTarget {
	return CreateAzCopyTarget(s, cred, a, opts...)
}

func (s *FileShareResourceManager) Canon() string {
	return buildCanonForAzureResourceManager(s)
}

func (s *FileShareResourceManager) Exists() bool {
	_, err := s.InternalClient.GetProperties(ctx, nil)

	return err == nil || !fileerror.HasCode(err, fileerror.ShareNotFound, fileerror.ShareBeingDeleted, fileerror.ResourceNotFound)
}

func (s *FileShareResourceManager) Parent() ResourceManager {
	return s.Service
}

func (s *FileShareResourceManager) Account() AccountResourceManager {
	return s.InternalAccount
}

func (s *FileShareResourceManager) ResourceClient() any {
	return s.InternalClient
}

func (s *FileShareResourceManager) Location() common.Location {
	return s.Service.Location()
}

func (s *FileShareResourceManager) Level() azcopy.LocationLevel {
	return azcopy.ELocationLevel.Container()
}

func (s *FileShareResourceManager) URI(opts ...GetURIOptions) string {
	base := fileStripSAS(s.InternalClient.URL())
	base = s.InternalAccount.ApplySAS(base, s.Location(), opts...)
	base = addWildCard(base, opts...)

	return base
}

func (s *FileShareResourceManager) ContainerName() string {
	return s.InternalContainerName
}

func (s *FileShareResourceManager) GetProperties(a Asserter) ContainerProperties {
	a.HelperMarker().Helper()
	resp, err := s.InternalClient.GetProperties(ctx, nil)
	a.NoError("get share properties", err)

	return ContainerProperties{
		Metadata: resp.Metadata,
		FileContainerProperties: FileContainerProperties{
			AccessTier:       (*share.AccessTier)(resp.AccessTier),
			EnabledProtocols: resp.EnabledProtocols,
			Quota:            resp.Quota,
			RootSquash:       resp.RootSquash,
		},
	}
}

// SetProperties Sets the quota of a file share
func (s *FileShareResourceManager) SetProperties(a Asserter, properties *ContainerProperties) {
	a.HelperMarker().Helper()
	props := DerefOrZero(properties)

	_, err := s.InternalClient.SetProperties(ctx, &share.SetPropertiesOptions{
		Quota: props.FileContainerProperties.Quota})

	a.NoError("set share properties", err)

	return
}

func (s *FileShareResourceManager) Create(a Asserter, props ContainerProperties) {
	a.HelperMarker().Helper()
	s.CreateWithOptions(a, &FileShareCreateOptions{
		AccessTier:       props.FileContainerProperties.AccessTier,
		EnabledProtocols: props.FileContainerProperties.EnabledProtocols,
		Metadata:         props.Metadata,
		Quota:            props.FileContainerProperties.Quota,
		RootSquash:       props.FileContainerProperties.RootSquash,
	})
}

type FileShareCreateOptions = share.CreateOptions

func (s *FileShareResourceManager) CreateWithOptions(a Asserter, options *FileShareCreateOptions) {
	a.HelperMarker().Helper()
	_, err := s.InternalClient.Create(ctx, options)

	created := true
	if fileerror.HasCode(err, fileerror.ShareAlreadyExists) {
		created = false
		err = nil
	}

	a.NoError("Create container", err)
	if created {
		TrackResourceCreation(a, s)
	}
}

func (s *FileShareResourceManager) Delete(a Asserter) {
	s.DeleteWithOptions(a, nil)
}

type FileShareDeleteOptions = share.DeleteOptions

func (s *FileShareResourceManager) DeleteWithOptions(a Asserter, options *FileShareDeleteOptions) {
	a.HelperMarker().Helper()
	_, err := s.InternalClient.Delete(ctx, options)
	a.NoError("delete share", err)
}

func (s *FileShareResourceManager) ListObjects(a Asserter, targetDir string, recursive bool) map[string]ObjectProperties {
	a.HelperMarker().Helper()
	queue := []string{targetDir}
	out := make(map[string]ObjectProperties)

	for len(queue) > 0 {
		parent := queue[0] // pop from queue
		queue = queue[1:]

		dirClient := s.InternalClient.NewDirectoryClient(parent)
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

				subdirClient := s.InternalClient.NewDirectoryClient(fullPath)
				resp, err := subdirClient.GetProperties(ctx, &directory.GetPropertiesOptions{})
				a.NoError("Get dir properties", err)

				var permissions *string
				if resp.FilePermissionKey != nil {
					permResp, err := s.InternalClient.GetPermission(ctx, *v.PermissionKey, nil)
					a.NoError("Get permissions", err)
					permissions = permResp.Permission
				}

				out[fullPath] = ObjectProperties{
					EntityType:       common.EEntityType.Folder(),
					Metadata:         resp.Metadata,
					LastModifiedTime: v.Properties.LastModified,
					FileProperties: FileProperties{
						FileAttributes:    v.Attributes,
						FileCreationTime:  v.Properties.CreationTime,
						FileLastWriteTime: v.Properties.LastWriteTime,
						FilePermissions:   permissions,
						LastModifiedTime:  v.Properties.LastModified,
					},
				}
			}

			// List files
			for _, v := range page.Segment.Files {
				fullPath := path.Join(parent, *v.Name)

				fileClient := s.InternalClient.NewRootDirectoryClient().NewFileClient(fullPath)
				resp, err := fileClient.GetProperties(ctx, &file.GetPropertiesOptions{})
				a.NoError("Get file properties", err)

				var permissions *string
				if resp.FilePermissionKey != nil {
					permResp, err := s.InternalClient.GetPermission(ctx, *v.PermissionKey, nil)
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
					Metadata:         resp.Metadata,
					LastModifiedTime: v.Properties.LastModified,
					FileProperties: FileProperties{
						FileAttributes:    v.Attributes,
						FileCreationTime:  v.Properties.CreationTime,
						FileLastWriteTime: v.Properties.LastWriteTime,
						FilePermissions:   permissions,
						LastModifiedTime:  v.Properties.LastModified,
					},
				}
			}
		}
	}

	return out
}

func (s *FileShareResourceManager) GetObject(a Asserter, path string, eType common.EntityType) ObjectResourceManager {
	return &FileObjectResourceManager{
		internalAccount: s.InternalAccount,
		Service:         s.Service,
		Share:           s,
		path:            path,
		entityType:      eType,
	}
}

// ==================== FILE ====================

type FileObjectResourceManager struct {
	internalAccount *AzureAccountResourceManager
	Service         *FileServiceResourceManager
	Share           *FileShareResourceManager

	path                     string
	entityType               common.EntityType
	hardlinkOriginalFilePath string
}

func (f *FileObjectResourceManager) DefaultAuthType() ExplicitCredentialTypes {
	return (&FileServiceResourceManager{}).DefaultAuthType()
}

func (f *FileObjectResourceManager) WithSpecificAuthType(cred ExplicitCredentialTypes, a Asserter, opts ...CreateAzCopyTargetOptions) AzCopyTarget {
	return CreateAzCopyTarget(f, cred, a, opts...)
}

func (f *FileObjectResourceManager) ValidAuthTypes() ExplicitCredentialTypes {
	return (&FileServiceResourceManager{}).ValidAuthTypes()
}

func (f *FileObjectResourceManager) ResourceClient() any {
	switch f.entityType {
	case common.EEntityType.Folder():
		return f.getDirClient()
	default: // For now, bundle up other entity types as files. That's how they should be implemented in AzCopy, at least.
		return f.getFileClient()
	}
}

func (f *FileObjectResourceManager) Canon() string {
	return buildCanonForAzureResourceManager(f)
}

func (f *FileObjectResourceManager) Parent() ResourceManager {
	return f.Share
}

func (f *FileObjectResourceManager) Account() AccountResourceManager {
	return f.internalAccount
}

func (f *FileObjectResourceManager) Location() common.Location {
	return f.Service.Location()
}

func (f *FileObjectResourceManager) Level() azcopy.LocationLevel {
	return azcopy.ELocationLevel.Object()
}

func (f *FileObjectResourceManager) URI(opts ...GetURIOptions) string {
	base := fileStripSAS(f.getFileClient().URL()) // restype doesn't matter here, same URL under the hood
	base = f.internalAccount.ApplySAS(base, f.Location(), opts...)
	base = addWildCard(base, opts...)

	return base
}

func (f *FileObjectResourceManager) EntityType() common.EntityType {
	return f.entityType
}

func (f *FileObjectResourceManager) ContainerName() string {
	return f.Share.ContainerName()
}

func (f *FileObjectResourceManager) ObjectName() string {
	return f.path
}

func (f *FileObjectResourceManager) HardlinkedFileName() string {
	return f.hardlinkOriginalFilePath
}

func (f *FileObjectResourceManager) PreparePermissions(a Asserter, p *string) *file.Permissions {
	a.HelperMarker().Helper()
	if p == nil {
		return nil
	}
	perm := *p

	fSDDL, err := sddl.ParseSDDL(perm)
	a.NoError("parse input SDDL", err)
	a.AssertNow("parsed string equivalence sanity check", Equal{}, fSDDL.String(), perm)

	perm = fSDDL.PortableString()

	if len(perm) >= ste.FilesServiceMaxSDDLSize {
		resp, err := f.Share.InternalClient.CreatePermission(ctx, perm, nil)
		a.NoError("Create share permission", err)
		return &file.Permissions{PermissionKey: resp.FilePermissionKey}
	}

	return &file.Permissions{Permission: &perm}
}

func (f *FileObjectResourceManager) CreateParents(a Asserter) {
	if !f.Share.Exists() {
		f.Share.Create(a, ContainerProperties{})
	}

	dir, _ := path.Split(strings.TrimSuffix(f.path, "/"))
	if dir != "" {
		obj := f.Share.GetObject(a, dir, common.EEntityType.Folder()).(*FileObjectResourceManager)
		// Create recursively calls this function.
		if !obj.Exists() {
			obj.Create(a, nil, ObjectProperties{})
		}
	}
}

func (f *FileObjectResourceManager) Create(a Asserter, body ObjectContentContainer, props ObjectProperties) {
	a.HelperMarker().Helper()

	nfsProperties := &file.NFSProperties{}

	if props.FileNFSProperties != nil {
		nfsProperties.CreationTime = props.FileNFSProperties.FileCreationTime
		nfsProperties.LastWriteTime = props.FileNFSProperties.FileLastWriteTime
	}
	if props.FileNFSPermissions != nil {
		nfsProperties.Owner = props.FileNFSPermissions.Owner
		nfsProperties.Group = props.FileNFSPermissions.Group
		nfsProperties.FileMode = props.FileNFSPermissions.FileMode
	}

	var attr *file.NTFSFileAttributes
	if DerefOrZero(props.FileProperties.FileAttributes) != "" {
		var err error
		attr, err = file.ParseNTFSFileAttributes(props.FileProperties.FileAttributes)
		a.NoError("Parse attributes", err)
	}

	perms := f.PreparePermissions(a, props.FileProperties.FilePermissions)

	f.CreateParents(a)

	switch f.entityType {
	case common.EEntityType.File():
		client := f.getFileClient()

		_, err := client.Create(ctx, body.Size(), &file.CreateOptions{
			SMBProperties: &file.SMBProperties{
				Attributes:    attr,
				CreationTime:  props.FileProperties.FileCreationTime,
				LastWriteTime: props.FileProperties.FileLastWriteTime,
			},
			Permissions:   perms,
			NFSProperties: nfsProperties,
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
			FilePermissions:   perms,
			Metadata:          props.Metadata,
			FileNFSProperties: nfsProperties,
		})
		// This is fine. Instead let's set properties.
		if fileerror.HasCode(err, fileerror.ResourceAlreadyExists) {
			err = nil

			f.SetObjectProperties(a, props)
		}

		a.NoError("Create directory", err)
	case common.EEntityType.Hardlink():
		client := f.getFileClient()

		_, err := client.CreateHardLink(ctx, props.HardLinkedFileName, &file.CreateHardLinkOptions{})
		a.NoError("Create file", err)
		// fmt.Println("Name", f.ObjectName())
		// fmt.Println("Resp.LinkCount", *resp.LinkCount)
		// fmt.Println("Resp.NFSFileType", *resp.NFSFileType)
	case common.EEntityType.Other():
		client := f.getFileClient()

		_, err := client.Create(ctx, body.Size(), &file.CreateOptions{
			NFSProperties: nfsProperties,
		})
		a.NoError("Create file", err)
		err = client.UploadStream(ctx, body.Reader(), &file.UploadStreamOptions{
			Concurrency: runtime.NumCPU(),
		})
		a.NoError("Upload Stream", err)
	default:
		a.Error("File Objects only support Files,Folders,Special File and Hardlinks.Currently " + f.entityType.String())
	}
	// Reapply the properties after the resource is created, as the Last Write Time of the file will be reset when data is written.
	f.SetObjectProperties(a, props)
	TrackResourceCreation(a, f)
}

func (f *FileObjectResourceManager) Delete(a Asserter) {
	a.HelperMarker().Helper()
	var err error
	switch f.entityType {
	case common.EEntityType.File():
		_, err = f.getFileClient().Delete(ctx, nil)
	case common.EEntityType.Folder():
		_, err = f.getDirClient().Delete(ctx, nil)
	case common.EEntityType.Hardlink():
		_, err = f.getFileClient().Delete(ctx, nil)
	case common.EEntityType.Other():
		_, err = f.getFileClient().Delete(ctx, nil)
	default:
		a.Error(fmt.Sprintf("entity type %s is not currently supported", f.entityType))
	}

	if fileerror.HasCode(err, fileerror.ResourceNotFound, fileerror.ShareNotFound, fileerror.ParentNotFound) {
		err = nil
	}

	a.NoError("delete path", err)
}

func (f *FileObjectResourceManager) ListChildren(a Asserter, recursive bool) map[string]ObjectProperties {
	a.HelperMarker().Helper()
	a.AssertNow("must be folder to list children", Equal{}, f.entityType, common.EEntityType.Folder())

	return f.Share.ListObjects(a, f.path, recursive)
}

func (f *FileObjectResourceManager) GetProperties(a Asserter) (out ObjectProperties) {
	a.HelperMarker().Helper()
	switch f.entityType {
	case common.EEntityType.Folder():
		resp, err := f.getDirClient().GetProperties(ctx, &directory.GetPropertiesOptions{})
		a.NoError("Get directory properties", err)

		var permissions *string
		if pkey := DerefOrZero(resp.FilePermissionKey); pkey != "" {
			permResp, err := f.Share.InternalClient.GetPermission(ctx, pkey, nil)
			a.NoError("Get file permissions", err)

			permissions = permResp.Permission
		}

		out = ObjectProperties{
			EntityType:       f.entityType, // It should be OK to just return entity type, getproperties should fail with the wrong restype
			Metadata:         resp.Metadata,
			LastModifiedTime: resp.LastModified,
			FileProperties: FileProperties{
				FileAttributes:    resp.FileAttributes,
				FileCreationTime:  resp.FileCreationTime,
				FileLastWriteTime: resp.FileLastWriteTime,
				FilePermissions:   permissions,
				LastModifiedTime:  resp.LastModified,
			},
			FileNFSProperties: &FileNFSProperties{
				FileCreationTime:  resp.FileCreationTime,
				FileLastWriteTime: resp.FileLastWriteTime,
			},
			FileNFSPermissions: &FileNFSPermissions{
				Owner:    resp.Owner,
				Group:    resp.Group,
				FileMode: resp.FileMode,
			},
		}
	case common.EEntityType.File():
		resp, err := f.getFileClient().GetProperties(ctx, &file.GetPropertiesOptions{})
		a.NoError("Get file properties", err)

		var permissions *string
		if pkey := DerefOrZero(resp.FilePermissionKey); pkey != "" {
			permResp, err := f.Share.InternalClient.GetPermission(ctx, pkey, nil)
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
			Metadata:         resp.Metadata,
			LastModifiedTime: resp.LastModified,
			FileProperties: FileProperties{
				FileAttributes:    resp.FileAttributes,
				FileCreationTime:  resp.FileCreationTime,
				FileLastWriteTime: resp.FileLastWriteTime,
				FilePermissions:   permissions,
				LastModifiedTime:  resp.LastModified,
			},
			FileNFSProperties: &FileNFSProperties{
				FileCreationTime:  resp.FileCreationTime,
				FileLastWriteTime: resp.FileLastWriteTime,
			},
			FileNFSPermissions: &FileNFSPermissions{
				Owner:    resp.Owner,
				Group:    resp.Group,
				FileMode: resp.FileMode,
			},
		}
	case common.EEntityType.Symlink():
		resp, err := f.getFileClient().GetProperties(ctx, &file.GetPropertiesOptions{})
		a.NoError("Get file properties", err)

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
			Metadata:         resp.Metadata,
			LastModifiedTime: resp.LastModified,
			FileNFSProperties: &FileNFSProperties{
				FileCreationTime:  resp.FileCreationTime,
				FileLastWriteTime: resp.FileLastWriteTime,
			},
			FileNFSPermissions: &FileNFSPermissions{
				Owner:    resp.Owner,
				Group:    resp.Group,
				FileMode: resp.FileMode,
			},
		}

	case common.EEntityType.Hardlink():
		resp, err := f.getFileClient().GetProperties(ctx, &file.GetPropertiesOptions{})
		a.NoError("Get file properties", err)

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
			Metadata:         resp.Metadata,
			LastModifiedTime: resp.LastModified,
			FileProperties: FileProperties{
				FileAttributes:    resp.FileAttributes,
				FileCreationTime:  resp.FileCreationTime,
				FileLastWriteTime: resp.FileLastWriteTime,
				//FilePermissions:   permissions,
				LastModifiedTime: resp.LastModified,
			},
			FileNFSProperties: &FileNFSProperties{
				FileCreationTime:  resp.FileCreationTime,
				FileLastWriteTime: resp.FileLastWriteTime,
			},
			FileNFSPermissions: &FileNFSPermissions{
				Owner:    resp.Owner,
				Group:    resp.Group,
				FileMode: resp.FileMode,
			},
		}
	default:
		a.Error("EntityType must be Folder,File,Hardlink or Symlink. Currently: " + f.entityType.String())
	}
	return
}

func (f *FileObjectResourceManager) SetHTTPHeaders(a Asserter, h contentHeaders) {
	a.HelperMarker().Helper()
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
	a.HelperMarker().Helper()
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
	a.HelperMarker().Helper()

	nfsProperties := &file.NFSProperties{}

	if props.FileNFSProperties != nil {
		nfsProperties.CreationTime = props.FileNFSProperties.FileCreationTime
		nfsProperties.LastWriteTime = props.FileNFSProperties.FileLastWriteTime
	}
	if props.FileNFSPermissions != nil {
		nfsProperties.Owner = props.FileNFSPermissions.Owner
		nfsProperties.Group = props.FileNFSPermissions.Group
		nfsProperties.FileMode = props.FileNFSPermissions.FileMode
	}

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
			Permissions:   perms,
			HTTPHeaders:   props.HTTPHeaders.ToFile(),
			NFSProperties: nfsProperties,
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
			FilePermissions:   perms,
			FileNFSProperties: nfsProperties,
		})
		a.NoError("Set folder properties", err)

		_, err = f.getDirClient().SetMetadata(ctx, &directory.SetMetadataOptions{Metadata: props.Metadata})
		a.NoError("Set folder metadata", err)
	}
}

func (f *FileObjectResourceManager) getFileClient() *file.Client {
	return f.Share.InternalClient.NewRootDirectoryClient().NewFileClient(f.path)
}

func (f *FileObjectResourceManager) getDirClient() *directory.Client {
	return f.Share.InternalClient.NewDirectoryClient(f.path)
}

func (f *FileObjectResourceManager) Download(a Asserter) io.ReadSeeker {
	a.HelperMarker().Helper()
	a.Assert("Entity type must be file", Equal{}, f.entityType, common.EEntityType.File())

	resp, err := f.getFileClient().DownloadStream(ctx, nil)
	a.NoError("Download stream", err)

	buf := &bytes.Buffer{}
	if err == nil && resp.Body != nil {
		_, err = io.Copy(buf, resp.Body)
		a.NoError("Read body", err)
	}

	return bytes.NewReader(buf.Bytes())
}

func (f *FileObjectResourceManager) Exists() bool {
	var err error
	if f.entityType != common.EEntityType.Folder() {
		_, err = f.getFileClient().GetProperties(ctx, nil)
	} else {
		_, err = f.getDirClient().GetProperties(ctx, nil)
	}

	return err == nil || !fileerror.HasCode(err, fileerror.ParentNotFound, fileerror.ShareNotFound, fileerror.ShareBeingDeleted, fileerror.ResourceNotFound)
}
