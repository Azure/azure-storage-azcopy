package e2etest

import (
	"bytes"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/cmd"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/ste"
	"github.com/google/uuid"
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
	a.HelperMarker().Helper()

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

func (l *LocalContainerResourceManager) URI(o ...GetURIOptions) string {
	base := l.RootPath
	base = addWildCard(base, o...)

	opts := FirstOrZero(o)
	if opts.LocalOpts.PreferUNCPath {
		base = common.ToExtendedPath(base)
	}

	return base
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
	a.HelperMarker().Helper()
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
	a.HelperMarker().Helper()
	time.Sleep(time.Second)

	err := os.RemoveAll(l.RootPath)
	if !os.IsNotExist(err) {
		a.NoError("Delete local root directory", err)
	}
}

func (l *LocalContainerResourceManager) ListObjects(a Asserter, prefixOrDirectory string, recursive bool) map[string]ObjectProperties {
	a.HelperMarker().Helper()
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
		container:                l,
		entityType:               eType,
		objectPath:               path,
		hardlinkOriginalFilePath: path,
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
	objectPath               string
	rawPath                  string
	hardlinkOriginalFilePath string
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

func (l *LocalObjectResourceManager) HardlinkedFileName() string {
	return l.hardlinkOriginalFilePath
}

type localPropertiesManager interface {
	GetSDDL(Asserter) string
	PutSDDL(sddlstr string, a Asserter)
	GetSMBProperties(Asserter) ste.TypedSMBPropertyHolder
	PutSMBProperties(Asserter, FileProperties)
	PutNFSPermissions(Asserter, FileNFSPermissions)
	PutNFSProperties(Asserter, FileNFSProperties)
	GetNFSPermissions(Asserter) ste.TypedNFSPermissionsHolder
	GetNFSProperties(Asserter) ste.TypedNFSPropertyHolder
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
	return filepath.Join(l.container.RootPath, l.objectPath)
}

func (l *LocalObjectResourceManager) getHardlinkedFilePath() string {

	if l.container == nil {
		panic("objectPath (relative) must have a container as a parent.")
	}
	fmt.Println("RootPath:", l.container.RootPath, "hardlinked.txt")
	return filepath.Join(l.container.RootPath, "hardlinked.txt")
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

func (l *LocalObjectResourceManager) URI(o ...GetURIOptions) string {
	base := filepath.Join(l.container.RootPath, l.objectPath)
	base = addWildCard(base, o...)

	opts := FirstOrZero(o)
	if opts.LocalOpts.PreferUNCPath {
		base = common.ToExtendedPath(base)
	}

	return base
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
	a.HelperMarker().Helper()
	a.AssertNow("Object must be file to have content", Equal{})
	if properties.EntityType != l.entityType {
		l.entityType = properties.EntityType
	}
	l.CreateParents(a)

	if l.entityType == common.EEntityType.File() {
		f, err := os.OpenFile(l.getWorkingPath(), os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0774)
		a.NoError("Open file", err)
		defer func(f *os.File) {
			err := f.Close()
			a.NoError("Close file", err)
		}(f)

		if body != nil {
			_, err = io.Copy(f, body.Reader())
			a.NoError("Write file", err)
		}
	} else if l.entityType == common.EEntityType.Folder() {
		err := os.Mkdir(l.getWorkingPath(), 0775)
		if !os.IsExist(err) {
			a.NoError("Mkdir", err)
		}
	} else if l.entityType == common.EEntityType.Symlink() {
		err := os.Symlink(filepath.Join(l.container.RootPath, properties.SymlinkedFileName), l.getWorkingPath())
		a.NoError("Create Symlink", err)
	} else if l.entityType == common.EEntityType.Hardlink() {
		err := os.Link(filepath.Join(l.container.RootPath, properties.HardLinkedFileName), l.getWorkingPath())
		a.NoError("Create hardlink", err)
	} else if l.entityType == common.EEntityType.Other() {
		err := osScenarioHelper{}.CreateSpecialFile(l.getWorkingPath())
		a.NoError("Create special file", err)
	}

	l.SetObjectProperties(a, properties)

	TrackResourceCreation(a, l)
}

func (l *LocalObjectResourceManager) Delete(a Asserter) {
	a.HelperMarker().Helper()
	err := os.RemoveAll(l.getWorkingPath())
	if !os.IsNotExist(err) {
		a.NoError("Could not remove local object "+l.getWorkingPath(), err)
	}
}

func (l *LocalObjectResourceManager) ListChildren(a Asserter, recursive bool) map[string]ObjectProperties {
	a.HelperMarker().Helper()
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
	a.HelperMarker().Helper()
	var stats fs.FileInfo
	var err error
	if l.entityType == common.EEntityType.Symlink() {
		stats, err = os.Lstat(l.getWorkingPath())
		if err != nil { // Prevent nil dereferences
			a.NoError("failed to get stat", err)
			return ObjectProperties{}
		}
	} else {
		stats, err = os.Stat(l.getWorkingPath())
		if err != nil { // Prevent nil dereferences
			a.NoError("failed to get stat", err)
			return ObjectProperties{}
		}
	}

	lmt := common.Iff(stats == nil, nil, PtrOf(stats.ModTime()))
	out := ObjectProperties{
		LastModifiedTime: lmt,
	}

	// OS-triggered code, implemented in newe2e_resource_managers_local_windows.go
	if localProp, ok := any(l).(localPropertiesManager); ok {
		props := localProp.GetSMBProperties(a)

		perms := localProp.GetSDDL(a)

		nfsProps := localProp.GetNFSProperties(a)
		nfsPerms := localProp.GetNFSPermissions(a)

		if props != nil {
			attr, err := props.FileAttributes()
			a.NoError("get attributes", err)
			out.FileProperties.FileAttributes = PtrOf(attr.String())
			out.FileProperties.FileCreationTime = PtrOf(props.FileCreationTime())
			out.FileProperties.FileLastWriteTime = PtrOf(props.FileLastWriteTime())
		}
		out.FileProperties.FilePermissions = common.Iff(perms == "", nil, &perms)
		if nfsProps != nil {
			out.FileNFSProperties = &FileNFSProperties{
				FileCreationTime:  PtrOf(nfsProps.FileCreationTime()),
				FileLastWriteTime: PtrOf(nfsProps.FileLastWriteTime()),
			}
		}
		if nfsPerms != nil {
			out.FileNFSPermissions = &FileNFSPermissions{
				Owner:    nfsPerms.GetOwner(),
				Group:    nfsPerms.GetGroup(),
				FileMode: nfsPerms.GetFileMode(),
			}
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
	a.HelperMarker().Helper()

	// todo: set SMB properties
	if localProps, ok := any(l).(localPropertiesManager); ok {
		if props.FileProperties.FilePermissions != nil {
			localProps.PutSDDL(*props.FileProperties.FilePermissions, a)
		}
		if props.FileNFSProperties != nil {
			localProps.PutNFSProperties(a, *props.FileNFSProperties)
		}
		if props.FileNFSPermissions != nil {
			localProps.PutNFSPermissions(a, *props.FileNFSPermissions)
		}

	}
}

func (l *LocalObjectResourceManager) Download(a Asserter) io.ReadSeeker {
	a.HelperMarker().Helper()
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

func (l *LocalObjectResourceManager) ReadLink(a Asserter) string {
	out, err := os.Readlink(l.getWorkingPath())
	a.NoError("ReadLink", err)

	return out
}
