package e2etest

import (
	"fmt"
	"github.com/Azure/azure-storage-azcopy/v10/azbfs"
	"github.com/Azure/azure-storage-azcopy/v10/cmd"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/ste"
	"io"
	"path/filepath"
)

func init() {
	// Enforce interfaces
	var sm ServiceResourceManager
	var cm ContainerResourceManager
	var rrm RemoteResourceManager
	sm = &BlobFSServiceResourceManager{}
	cm = &FileSystemResourceManager{}
	rrm = sm
	rrm = cm.(*FileSystemResourceManager)
	_, _, _ = sm, cm, rrm // "use" vars
}

type BlobFSServiceResourceManager struct {
	Client azbfs.ServiceURL
}

func (b *BlobFSServiceResourceManager) ValidAuthTypes() ExplicitCredentialTypes {
	return EExplicitCredentialType.SASToken() | EExplicitCredentialType.OAuth() | EExplicitCredentialType.AcctKey()
}

func (b *BlobFSServiceResourceManager) ResourceClient() any {
	return b.Client
}

func (b *BlobFSServiceResourceManager) Location() common.Location {
	return common.ELocation.BlobFS()
}

func (b *BlobFSServiceResourceManager) Level() cmd.LocationLevel {
	return cmd.ELocationLevel.Service()
}

func (b *BlobFSServiceResourceManager) CreateContainer(name string, options *CreateContainerOptions) (ContainerResourceManager, error) {
	fsClient := b.Client.NewFileSystemURL(name)
	_, err := fsClient.Create(ctx)
	return &FileSystemResourceManager{b, fsClient}, err
}

func (b *BlobFSServiceResourceManager) DeleteContainer(name string, options *DeleteContainerOptions) error {
	fsClient := b.Client.NewFileSystemURL(name)
	_, err := fsClient.Delete(ctx)
	return err
}

func (b *BlobFSServiceResourceManager) GetContainer(name string) ContainerResourceManager {
	fsClient := b.Client.NewFileSystemURL(name)
	return &FileSystemResourceManager{b, fsClient}
}

type FileSystemResourceManager struct {
	Parent *BlobFSServiceResourceManager
	Client azbfs.FileSystemURL
}

func (b *FileSystemResourceManager) ValidAuthTypes() ExplicitCredentialTypes {
	return b.Parent.ValidAuthTypes()
}

func (b *FileSystemResourceManager) ResourceClient() any {
	return b.Client
}

func (b *FileSystemResourceManager) recursiveCreateDirectories(path string) error {
	dir, name := filepath.Split(path)
	var queue []string
	for dir != "" {
		queue = append([]string{name}, queue...)
		dir, name = filepath.Split(dir)
	}

	newDir := b.Client.NewRootDirectoryURL()
	for idx := 0; idx < len(queue); idx++ {
		newDir = newDir.NewDirectoryURL(queue[idx])
		_, err := newDir.Create(ctx, false)
		if err != nil {
			return fmt.Errorf("when creating dir %s: %w", newDir.String(), err)
		}
	}

	return nil
}

type BlobFSObjectCreationOptions struct {
	AccessControl azbfs.BlobFSAccessControl
}

// Create uses BlobFSObjectCreationOptions for ResourceSpecificOptions
func (b FileSystemResourceManager) Create(path string, entityType common.EntityType, options *CreateObjectOptions) error {
	dir, objName := filepath.Split(path)
	err := b.recursiveCreateDirectories(dir)
	if err != nil {
		return fmt.Errorf("when creating directories: %w", err)
	}
	dirClient := b.Client.NewRootDirectoryURL().NewDirectoryURL(dir)

	opts := DerefOrZero(options)
	bfsOpts := GetTypeOrZero[BlobFSObjectCreationOptions](opts.ResourceSpecificOptions)
	content := opts.Content

	switch entityType {
	case common.EEntityType.File():
		fileClient := dirClient.NewFileURL(objName)
		msu := &MultiStepUploader{}
		msu.BlockSize = common.DefaultBlockBlobBlockSize

		msu.Init = func(size int64) error {
			_, err := fileClient.Create(ctx, opts.Headers.ToBlobFS(), bfsOpts.AccessControl)
			return err
		}
		msu.UploadRange = func(block io.ReadSeeker, state MultiStepUploaderState) error {
			_, err := fileClient.AppendData(ctx, state.Offset, block)
			return err
		}
		msu.Finalize = func() error {
			flushLimit := msu.BlockSize * int64(ste.ADLSFlushThreshold) // How many bytes before we flush?
			flushed := int64(0)
			for flushed < content.Size() {
				toFlush := flushLimit
				if rem := content.Size() - flushed; rem < toFlush { // if the remainder is less than the flush limit, flush only remaining
					toFlush = rem
				}

				flushed += toFlush
				_, err := fileClient.FlushData(ctx, flushed, nil, opts.Headers.ToBlobFS(), true, flushed == content.Size())
				if err != nil {
					return fmt.Errorf("while flushing data @ %d/%d bytes: %w", flushed, content.Size(), err)
				}
			}

			return nil
		}

		msu.UploadContents(opts.Content)
	case common.EEntityType.Folder():
		dirClient := dirClient.NewDirectoryURL(objName)
		_, err := dirClient.CreateWithOptions(ctx, azbfs.CreateDirectoryOptions{Metadata: opts.Metadata})
		if err != nil {
			return fmt.Errorf("while creating directory: %w", err)
		}

		if opts.ResourceSpecificOptions != nil {
			_, err = dirClient.SetAccessControl(ctx, bfsOpts.AccessControl)
			if err != nil {
				return fmt.Errorf("while setting access control: %w", err)
			}
		}
	default:
		err = fmt.Errorf("entity type %s not supported", entityType.String())
	}

	return err
}

// Read uses azbfs.RetryReaderOptions as ResourceSpecificOptions
func (b *FileSystemResourceManager) Read(path string, options *ReadObjectOptions) ([]byte, error) {
	fileClient := b.Client.NewRootDirectoryURL().NewFileURL(path)
	dlResp, err := fileClient.Download(ctx, options.offset, options.count)
	if err != nil {
		return nil, err
	}

	opts := DerefOrZero(options)
	retryOptions := GetTypeOrZero[azbfs.RetryReaderOptions](opts.ResourceSpecificOptions)

	return io.ReadAll(dlResp.Body(retryOptions))
}

// GetProperties uses azbfs.BlobFSAccessControl for ResourceSpecificProperties
func (b *FileSystemResourceManager) GetProperties(path string, options *GetObjectPropertiesOptions) (GenericObjectProperties, error) {
	fileClient := b.Client.NewRootDirectoryURL().NewFileURL(path)
	resp, err := fileClient.GetProperties(ctx)
	if err != nil {
		return GenericObjectProperties{}, fmt.Errorf("when get properties: %w", err)
	}

	accessControl, err := fileClient.GetAccessControl(ctx)
	if err != nil {
		return GenericObjectProperties{}, fmt.Errorf("when get access control: %w", err)
	}

	return GenericObjectProperties{
		headers: common.ResourceHTTPHeaders{
			ContentType:        resp.ContentType(),
			ContentMD5:         resp.ContentMD5(),
			ContentEncoding:    resp.ContentEncoding(),
			ContentLanguage:    resp.ContentLanguage(),
			ContentDisposition: resp.ContentDisposition(),
			CacheControl:       resp.CacheControl(),
		},
		metadata:                   nil, // Gen 2 doesn't present metadata
		ResourceSpecificProperties: accessControl,
		OriginalResponse:           resp,
	}, nil
}

// todo: Should we support the key-value properties (not metadata)? AzCopy itself doesn't bother.
func (b *FileSystemResourceManager) SetProperties(path string, props GenericObjectProperties, options *SetObjectPropertiesOptions) error {
	fileClient := b.Client.NewRootDirectoryURL().NewFileURL(path)
	_, err := fileClient.SetProperties(ctx, props.headers.ToBlobFSHTTPHeaders())
	return err
}

func (b *FileSystemResourceManager) Delete(path string, options *DeleteObjectProperties) error {
	fileClient := b.Client.NewRootDirectoryURL().NewFileURL(path)
	_, err := fileClient.Delete(ctx)
	return err
}

func (b *FileSystemResourceManager) Location() common.Location {
	return common.ELocation.BlobFS()
}

func (b *FileSystemResourceManager) Level() cmd.LocationLevel {
	return cmd.ELocationLevel.Container()
}
