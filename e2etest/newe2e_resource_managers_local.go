package e2etest

import (
	"bytes"
	"fmt"
	"github.com/Azure/azure-storage-azcopy/v10/cmd"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/ste"
	"github.com/google/uuid"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"time"
)

// enforce interface compliance at compile time
func init() {
	void := func(_ ...any) {}

	void(
		ContainerResourceManager(&LocalContainerResourceManager{}),
		ObjectResourceManager(&LocalObjectResourceManager{}),
	)
}

func NewLocalContainer(a Asserter) ContainerResourceManager {
	if d, ok := a.(DryrunAsserter); ok && d.Dryrun() {
		return &MockContainerResourceManager{
			containerName:    "mockContainer",
			overrideLocation: common.ELocation.Local(),
		}
	}

	tmp := os.TempDir()
	dirName := uuid.NewString()

	return &LocalContainerResourceManager{
		RootPath: filepath.Join(tmp, dirName),
	}
}

// LocalContainerResourceManager is effectively just the root temp folder for a transfer.
type LocalContainerResourceManager struct {
	RootPath string
}

func (l *LocalContainerResourceManager) Location() common.Location {
	return common.ELocation.Local()
}

func (l *LocalContainerResourceManager) Level() cmd.LocationLevel {
	return cmd.ELocationLevel.Container()
}

func (l *LocalContainerResourceManager) URI(opts ...GetURIOptions) string {
	return l.RootPath
}

func (l *LocalContainerResourceManager) Parent() ResourceManager {
	return nil
}

func (l *LocalContainerResourceManager) Account() AccountResourceManager {
	return nil
}

func (l *LocalContainerResourceManager) Canon() string {
	return fmt.Sprintf("accountless/local/%s", l.ContainerName())
}

func (l *LocalContainerResourceManager) ContainerName() string {
	return filepath.Base(l.RootPath)
}

func (l *LocalContainerResourceManager) Create(a Asserter, props ContainerProperties) {
	err := os.Mkdir(l.RootPath, 0777)
	if !os.IsExist(err) {
		a.NoError("Create local root directory", err)
	}

	TrackResourceCreation(a, l)
}

func (l *LocalContainerResourceManager) GetProperties(a Asserter) ContainerProperties {
	return ContainerProperties{}
}

func (l *LocalContainerResourceManager) Delete(a Asserter) {
	time.Sleep(time.Second)

	err := os.RemoveAll(l.RootPath)
	if !os.IsNotExist(err) {
		a.NoError("Delete local root directory", err)
	}
}

func (l *LocalContainerResourceManager) ListObjects(a Asserter, prefixOrDirectory string, recursive bool) map[string]ObjectProperties {
	out := make(map[string]ObjectProperties)

	root := l.GetObject(a, prefixOrDirectory, common.EEntityType.Folder()).(*LocalObjectResourceManager)

	err := filepath.WalkDir(l.RootPath, func(path string, d fs.DirEntry, err error) error {
		relPath, err := root.getRelPath(path)
		if err != nil {
			return err
		}

		out[relPath] = root.getChildObject(relPath, common.Iff(d.IsDir(), common.EEntityType.Folder(), common.EEntityType.File())).GetProperties(a)

		return common.Iff(d.IsDir() && !recursive, filepath.SkipDir, nil)
	})
	a.NoError("failed to walk", err)

	return out
}

func (l *LocalContainerResourceManager) GetObject(a Asserter, path string, eType common.EntityType) ObjectResourceManager {
	return &LocalObjectResourceManager{
		container:  l,
		entityType: eType,
		objectPath: path,
	}
}

func (l *LocalContainerResourceManager) Exists() bool {
	_, err := os.Stat(l.RootPath)
	return err == nil
}

type LocalObjectResourceManager struct {
	container  *LocalContainerResourceManager
	entityType common.EntityType

	// objectPath and rawPath are mutually exclusive fields.
	objectPath string
	rawPath    string
}

func (l *LocalObjectResourceManager) ContainerName() string {
	if l.container != nil {
		return filepath.Base(l.container.RootPath)
	}

	return "containerless"
}

func (l *LocalObjectResourceManager) ObjectName() string {
	if l.objectPath != "" {
		return l.objectPath
	} else {
		return filepath.Base(l.rawPath)
	}
}

type localSMBPropertiesManager interface {
	GetSDDL(Asserter) string
	PutSDDL(sddlstr string, a Asserter)
	GetSMBProperties(Asserter) ste.TypedSMBPropertyHolder
	PutSMBProperties(Asserter, ste.TypedSMBPropertyHolder)
}

func (l *LocalObjectResourceManager) getChildObject(relPath string, entityType common.EntityType) *LocalObjectResourceManager {
	if l.objectPath != "" { // We have a "container", this is relative
		newPath := filepath.Join(l.objectPath, relPath)

		return &LocalObjectResourceManager{
			container:  l.container,
			entityType: entityType,
			objectPath: newPath,
		}
	} else {
		newPath := filepath.Join(l.rawPath, relPath)

		return &LocalObjectResourceManager{
			entityType: entityType,
			rawPath:    newPath,
		}
	}
}

func (l *LocalObjectResourceManager) getWorkingPath() string {
	if l.objectPath != "" && l.rawPath != "" {
		panic("objectPath and rawPath are mutually exclusive fields, and should never be filled at the same time.")
	}

	if l.rawPath != "" {
		return l.rawPath
	}

	if l.container == nil {
		panic("objectPath (relative) must have a container as a parent.")
	}

	// l.objectPath can be "", indicating it is the folder at the root of the container.
	return path.Join(l.container.RootPath, l.objectPath)
}

func (l *LocalObjectResourceManager) getRelPath(fullPath string) (string, error) {
	rootPath := ""
	if l.objectPath != "" {
		rootPath = filepath.Join(l.container.RootPath, l.objectPath)
	} else {
		rootPath = l.rawPath
	}

	return filepath.Rel(rootPath, fullPath)
}

func (l *LocalObjectResourceManager) Location() common.Location {
	return common.ELocation.Local()
}

func (l *LocalObjectResourceManager) Level() cmd.LocationLevel {
	return cmd.ELocationLevel.Object()
}

func (l *LocalObjectResourceManager) URI(opts ...GetURIOptions) string {
	return filepath.Join(l.container.RootPath, l.objectPath)
}

func (l *LocalObjectResourceManager) Parent() ResourceManager {
	return l.container
}

func (l *LocalObjectResourceManager) Account() AccountResourceManager {
	return nil
}

func (l *LocalObjectResourceManager) Canon() string {
	if l.container != nil {
		return l.container.Canon() + "/" + l.objectPath
	} else {
		return fmt.Sprintf("accountless/local/containerless/%s", l.rawPath)
	}
}

func (l *LocalObjectResourceManager) EntityType() common.EntityType {
	return l.entityType
}

func (l *LocalObjectResourceManager) CreateParents(a Asserter) {
	if l.container != nil {
		l.container.Create(a, ContainerProperties{})
	}

	err := os.MkdirAll(filepath.Dir(l.getWorkingPath()), 0775)
	a.NoError("mkdirall", err)
}

func (l *LocalObjectResourceManager) Create(a Asserter, body ObjectContentContainer, properties ObjectProperties) {
	a.AssertNow("Object must be file to have content", Equal{})

	l.CreateParents(a)
	f, err := os.OpenFile(l.getWorkingPath(), os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0774)
	a.NoError("Open file", err)
	defer func(f *os.File) {
		err := f.Close()
		a.NoError("Close file", err)
	}(f)

	_, err = io.Copy(f, body.Reader())
	a.NoError("Write file", err)

	l.SetObjectProperties(a, properties)

	TrackResourceCreation(a, l)
}

func (l *LocalObjectResourceManager) Delete(a Asserter) {
	err := os.RemoveAll(l.getWorkingPath())
	if !os.IsNotExist(err) {
		a.NoError("Could not remove local object "+l.getWorkingPath(), err)
	}
}

func (l *LocalObjectResourceManager) ListChildren(a Asserter, recursive bool) map[string]ObjectProperties {
	a.AssertNow("Entity type must be folder to have children", Equal{}, l.entityType, common.EEntityType.Folder())
	out := make(map[string]ObjectProperties)

	err := filepath.WalkDir(l.getWorkingPath(), func(path string, d fs.DirEntry, err error) error {
		relPath, err := l.getRelPath(path)
		if err != nil {
			return err
		}

		out[relPath] = l.getChildObject(relPath, common.Iff(d.IsDir(), common.EEntityType.Folder(), common.EEntityType.File())).GetProperties(a)

		return common.Iff(d.IsDir() && !recursive, filepath.SkipDir, nil)
	})
	a.NoError("failed to walk", err)

	return out
}

func (l *LocalObjectResourceManager) GetProperties(a Asserter) ObjectProperties {
	out := ObjectProperties{}

	// OS-triggered code, implemented in newe2e_resource_managers_local_windows.go
	if smb, ok := any(l).(localSMBPropertiesManager); ok {
		props := smb.GetSMBProperties(a)

		attr, err := props.FileAttributes()
		a.NoError("get attributes", err)

		perms := smb.GetSDDL(a)

		out.FileProperties = FileProperties{
			FileAttributes:    PtrOf(attr.String()),
			FileCreationTime:  PtrOf(props.FileCreationTime()),
			FileLastWriteTime: PtrOf(props.FileLastWriteTime()),
			FilePermissions:   common.Iff(perms == "", nil, &perms),
		}
	}

	return out
}

func (l *LocalObjectResourceManager) SetHTTPHeaders(a Asserter, h contentHeaders) {
	// no-op on local
}

func (l *LocalObjectResourceManager) SetMetadata(a Asserter, metadata common.Metadata) {
	// no-op on local
	// todo: we could worry about xAttr on Linux, etc. but we don't officially support any of that outside of specific features (e.g. hash based sync)
}

func (l *LocalObjectResourceManager) SetObjectProperties(a Asserter, props ObjectProperties) {
	// todo: set SMB properties
	if smb, ok := any(l).(localSMBPropertiesManager); ok {
		if props.FileProperties.FilePermissions != nil {
			smb.PutSDDL(*props.FileProperties.FilePermissions, a)
		}
	}
}

func (l *LocalObjectResourceManager) Download(a Asserter) io.ReadSeeker {
	a.AssertNow("Entity type must be file to have content to download", Equal{}, l.entityType, common.EEntityType.File())

	f, err := os.Open(l.getWorkingPath())
	a.NoError("open file", err)
	defer func(f *os.File) {
		err = f.Close()
		a.NoError("Close file", err)
	}(f)

	buf := &bytes.Buffer{}
	_, err = io.Copy(buf, f)
	a.NoError("read file", err)

	return bytes.NewReader(buf.Bytes())
}

func (l *LocalObjectResourceManager) Exists() bool {
	_, err := os.Stat(l.getWorkingPath())
	return err == nil
}
