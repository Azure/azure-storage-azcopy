package e2etest

import (
	"fmt"
	"github.com/Azure/azure-storage-azcopy/v10/cmd"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/sddl"
	"github.com/Azure/azure-storage-azcopy/v10/ste"
	"github.com/Azure/azure-storage-file-go/azfile"
	"io"
	"path/filepath"
	"strings"
	"time"
)

func init() {
	// Enforce interfaces
	var sm ServiceResourceManager
	var cm ContainerResourceManager
	var rrm RemoteResourceManager
	sm = &FileServiceResourceManager{}
	cm = &FileShareResourceManager{}
	rrm = sm
	rrm = cm.(*FileShareResourceManager)
	_, _, _ = sm, cm, rrm // "use" vars
}

type FileServiceResourceManager struct {
	ServiceClient azfile.ServiceURL
}

func (f *FileServiceResourceManager) ResourceClient() any {
	return f.ServiceClient
}

func (f *FileServiceResourceManager) ValidAuthTypes() ExplicitCredentialTypes {
	return EExplicitCredentialType.PublicAuth() | EExplicitCredentialType.SASToken()
}

func (f *FileServiceResourceManager) Location() common.Location {
	return common.ELocation.File()
}

func (f *FileServiceResourceManager) Level() cmd.LocationLevel {
	return cmd.ELocationLevel.Service()
}

type FileServiceCreateContainerOptions struct {
	Metadata common.Metadata
	QuotaGB  int32
}

func (f *FileServiceResourceManager) CreateContainer(name string, options *CreateContainerOptions) (ContainerResourceManager, error) {
	shareClient := f.ServiceClient.NewShareURL(name)
	fileOpts := GetTypeOrZero[FileServiceCreateContainerOptions](options.ResourceSpecificOptions)
	_, err := shareClient.Create(ctx, fileOpts.Metadata.ToAzFileMetadata(), fileOpts.QuotaGB)

	return &FileShareResourceManager{f, shareClient}, err
}

type FileServiceDeleteContainerOptions struct {
	Snapshots azfile.DeleteSnapshotsOptionType
}

func (f *FileServiceResourceManager) DeleteContainer(name string, options *DeleteContainerOptions) error {
	shareClient := f.ServiceClient.NewShareURL(name)
	opts := DerefOrZero(options)
	fileOpts := GetTypeOrZero[FileServiceDeleteContainerOptions](opts.ResourceSpecificOptions)
	_, err := shareClient.Delete(ctx, fileOpts.Snapshots)
	return err
}

func (f *FileServiceResourceManager) GetContainer(name string) ContainerResourceManager {
	shareClient := f.ServiceClient.NewShareURL(name)
	return &FileShareResourceManager{f, shareClient}
}

type FileShareResourceManager struct {
	parent *FileServiceResourceManager
	client azfile.ShareURL
}

func (f *FileShareResourceManager) ValidAuthTypes() ExplicitCredentialTypes {
	return f.parent.ValidAuthTypes()
}

func (f *FileShareResourceManager) ResourceClient() any {
	return f.client
}

func (f *FileShareResourceManager) Location() common.Location {
	return common.ELocation.File()
}

func (f *FileShareResourceManager) Level() cmd.LocationLevel {
	return cmd.ELocationLevel.Container()
}

func (f *FileShareResourceManager) recursiveCreateDirectories(path string) error {
	dir, name := filepath.Split(path)
	var queue []string
	for dir != "" {
		queue = append([]string{name}, queue...)
		dir, name = filepath.Split(dir)
	}

	newDir := f.client.NewRootDirectoryURL()
	for idx := 0; idx < len(queue); idx++ {
		newDir = newDir.NewDirectoryURL(queue[idx])
		_, err := newDir.Create(ctx, nil, azfile.SMBProperties{})
		if err != nil {
			return fmt.Errorf("when creating dir %s: %w", newDir.String(), err)
		}
	}

	return nil
}

func (f *FileShareResourceManager) prepareSMBProperties(properties *azfile.SMBProperties) error {
	// Ensure the SDDL string is portable first
	if pstr := DerefOrZero(properties.PermissionString); pstr != "" {
		fSDDL, err := sddl.ParseSDDL(pstr)

		if err != nil {
			return fmt.Errorf("when parsing SDDL: %w", err)
		}

		if strings.TrimSpace(fSDDL.String()) != strings.TrimSpace(pstr) {
			panic("SDDL sanity check failed (parsed string output != original string.)")
		}

		properties.PermissionString = pointerTo(fSDDL.PortableString())
	}

	// Upload SDDL if necessary
	if pstr := DerefOrZero(properties.PermissionString); len(pstr) >= ste.FilesServiceMaxSDDLSize {
		resp, err := f.client.CreatePermission(ctx, pstr)
		if err != nil {
			return fmt.Errorf("when uploading perm string: %w", err)
		}
		properties.PermissionKey = PtrOf(resp.FilePermissionKey())
		properties.PermissionString = nil
	}

	return nil
}

// Create uses azfile.SMBProperties for ResourceSpecificOptions
func (f *FileShareResourceManager) Create(path string, entityType common.EntityType, options *CreateObjectOptions) error {
	//baseFileURL := f.client.new
	dir, objectName := filepath.Split(path)
	err := f.recursiveCreateDirectories(dir)
	if err != nil {
		return fmt.Errorf("when creating directories: %w", err)
	}
	baseDirURL := f.client.NewDirectoryURL(dir)

	opts := DerefOrZero(options)

	fileOpts := GetTypeOrZero[azfile.SMBProperties](opts.ResourceSpecificOptions)
	err = f.prepareSMBProperties(&fileOpts)
	if err != nil {
		return fmt.Errorf("when preparing SMB properties: %w", err)
	}

	switch entityType {
	case common.EEntityType.File():
		fileURL := baseDirURL.NewFileURL(objectName)
		msu := &MultiStepUploader{}

		msu.Init = func(size int64) error {
			headers := opts.Headers.ToFile()
			headers.SMBProperties = fileOpts
			_, err = fileURL.Create(ctx, size, headers, opts.Metadata)
			return err
		}
		msu.UploadRange = func(block io.ReadSeeker, state MultiStepUploaderState) error {
			_, err = fileURL.UploadRange(ctx, state.Offset, block, nil)
			return err
		}

		msu.BlockSize = 10 * 1024 * 1024
		msu.Parallel = true
		err = msu.UploadContents(opts.Content)
		if err != nil {
			err = fmt.Errorf("when uploading file: %w", err)
		}
	case common.EEntityType.Folder():
		dirURL := baseDirURL.NewDirectoryURL(objectName)
		_, err = dirURL.Create(ctx, opts.Metadata, fileOpts)
		if err != nil {
			err = fmt.Errorf("when creating directory: %w", err)
		}
	default:
		err = fmt.Errorf("entity type %s not supported", entityType.String())
	}

	return err
}

type FileObjectReadOptions struct {
	RetryReaderOptions azfile.RetryReaderOptions
}

func (f *FileShareResourceManager) Read(path string, options *ReadObjectOptions) ([]byte, error) {
	baseFileURL := f.client.NewRootDirectoryURL().NewFileURL(path)
	opts := DerefOrZero(options)
	fileOpts := GetTypeOrZero[azfile.RetryReaderOptions](opts.ResourceSpecificOptions)
	resp, err := baseFileURL.Download(ctx, opts.offset, opts.count, false)
	if err != nil {
		return nil, err
	}

	return io.ReadAll(resp.Body(fileOpts))
}

// GetProperties returns azfile.SMBProperties in ResourceSpecificProperties
func (f *FileShareResourceManager) GetProperties(path string, options *GetObjectPropertiesOptions) (GenericObjectProperties, error) {
	baseFileURL := f.client.NewRootDirectoryURL().NewFileURL(path)
	resp, err := baseFileURL.GetProperties(ctx)
	if err != nil {
		return GenericObjectProperties{}, err
	}

	sharePerm, err := f.client.GetPermission(ctx, resp.FilePermissionKey())
	if err != nil {
		return GenericObjectProperties{}, err
	}

	creationTime, err := time.Parse(azfile.ISO8601, resp.FileCreationTime())
	if err != nil {
		return GenericObjectProperties{}, err
	}
	lastWriteTime, err := time.Parse(azfile.ISO8601, resp.FileLastWriteTime())
	if err != nil {
		return GenericObjectProperties{}, err
	}

	props := GenericObjectProperties{
		headers: common.ResourceHTTPHeaders{
			ContentType:        resp.ContentType(),
			ContentMD5:         resp.ContentMD5(),
			ContentEncoding:    resp.ContentEncoding(),
			ContentLanguage:    resp.ContentLanguage(),
			ContentDisposition: resp.ContentDisposition(),
			CacheControl:       resp.CacheControl(),
		},
		metadata: common.FromAzFileMetadataToCommonMetadata(resp.NewMetadata()),
		ResourceSpecificProperties: azfile.SMBProperties{
			PermissionKey:     pointerTo(resp.FilePermissionKey()),
			PermissionString:  &sharePerm.Permission,
			FileAttributes:    pointerTo(azfile.ParseFileAttributeFlagsString(resp.FileAttributes())),
			FileCreationTime:  &creationTime,
			FileLastWriteTime: &lastWriteTime,
		},
		OriginalResponse: resp,
	}

	return props, nil
}

// SetProperties uses azfile.SMBProperties in ResourceSpecificProperties
func (f *FileShareResourceManager) SetProperties(path string, props GenericObjectProperties, options *SetObjectPropertiesOptions) error {
	fileURL := f.client.NewRootDirectoryURL().NewFileURL(path)
	headers := props.headers.ToAzFileHTTPHeaders()
	err := f.prepareSMBProperties(&headers.SMBProperties)
	if err != nil {
		return fmt.Errorf("when preparing SMB properties: %w", err)
	}

	_, err = fileURL.SetHTTPHeaders(ctx, headers)
	if err != nil {
		return fmt.Errorf("when setting HTTP headers: %w", err)
	}

	_, err = fileURL.SetMetadata(ctx, props.metadata.ToAzFileMetadata())
	if err != nil {
		return fmt.Errorf("when setting HTTP headers: %w", err)
	}

	return nil
}

func (f *FileShareResourceManager) Delete(path string, options *DeleteObjectProperties) error {
	fileURL := f.client.NewRootDirectoryURL().NewFileURL(path)
	_, err := fileURL.Delete(ctx)
	return err
}
