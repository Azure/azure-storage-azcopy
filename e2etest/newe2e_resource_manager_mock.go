package e2etest

import (
	"bytes"
	"fmt"
	"io"

	"github.com/Azure/azure-storage-azcopy/v10/cmd"
	"github.com/Azure/azure-storage-azcopy/v10/common"
)

/*
All resource managers implemented in this file are handed
to tests currently in dry-runs, hold no state, and
*/

// ensure interface compliance at compile time
func init() {
	void := func(_ ...any) {}

	void(
		// basic types
		AccountResourceManager(&MockAccountResourceManager{}),
		ServiceResourceManager(&MockServiceResourceManager{}),
		ContainerResourceManager(&MockContainerResourceManager{}),
		ObjectResourceManager(&MockObjectResourceManager{}),

		// ensure remote resource manager is available >= service
		RemoteResourceManager(&MockServiceResourceManager{}),
		RemoteResourceManager(&MockContainerResourceManager{}),
		RemoteResourceManager(&MockObjectResourceManager{}),

		// ensure all are mock resource managers
		mockResource(&MockAccountResourceManager{}),
		mockResource(&MockServiceResourceManager{}),
		mockResource(&MockContainerResourceManager{}),
		mockResource(&MockObjectResourceManager{}),
	)
}

var mockAccountServices = map[AccountType][]common.Location{
	EAccountType.Standard():                     {common.ELocation.Blob(), common.ELocation.File(), common.ELocation.BlobFS()},
	EAccountType.PremiumBlockBlobs():            {common.ELocation.Blob(), common.ELocation.BlobFS()},
	EAccountType.PremiumPageBlobs():             {common.ELocation.Blob()},
	EAccountType.PremiumFileShares():            {common.ELocation.File()},
	EAccountType.HierarchicalNamespaceEnabled(): {common.ELocation.Blob(), common.ELocation.File(), common.ELocation.BlobFS()},
	EAccountType.Classic():                      {},
}

type mockResource interface {
	mockSignature()
}

type MockAccountResourceManager struct {
	accountName string
	accountType AccountType
}

func (m *MockAccountResourceManager) mockSignature() {
	//TODO implement me
	panic("mockSignature should not be called")
}

func (m *MockAccountResourceManager) AccountName() string {
	return m.accountName
}

func (m *MockAccountResourceManager) AccountType() AccountType {
	return m.accountType
}

func (m *MockAccountResourceManager) AvailableServices() []common.Location {
	return mockAccountServices[m.accountType]
}

func (m *MockAccountResourceManager) GetService(a Asserter, location common.Location) ServiceResourceManager {
	if !ListContains(location, m.AvailableServices()) {
		a.Error(fmt.Sprintf("\"%s\" is not a valid service for account type %s. Valid services are: %v", location, m.accountType, m.AvailableServices()))
	}

	return &MockServiceResourceManager{parent: m, serviceType: location}
}

var mockServiceAuthTypes = map[common.Location]ExplicitCredentialTypes{
	common.ELocation.Blob():   (&BlobServiceResourceManager{}).ValidAuthTypes(),
	common.ELocation.File():   (&FileServiceResourceManager{}).ValidAuthTypes(),
	common.ELocation.BlobFS(): (&BlobFSServiceResourceManager{}).ValidAuthTypes(),
	// todo S3
	// todo GCP
}

var mockServiceDefaultAuthTypes = map[common.Location]ExplicitCredentialTypes{
	common.ELocation.Blob():   (&BlobServiceResourceManager{}).DefaultAuthType(),
	common.ELocation.File():   (&FileServiceResourceManager{}).DefaultAuthType(),
	common.ELocation.BlobFS(): (&BlobFSServiceResourceManager{}).DefaultAuthType(),
}

type MockServiceResourceManager struct {
	parent           *MockAccountResourceManager
	specificAuthType *ExplicitCredentialTypes
	serviceType      common.Location
}

func (m *MockServiceResourceManager) DefaultAuthType() ExplicitCredentialTypes {
	return mockServiceDefaultAuthTypes[m.serviceType]
}

func (m *MockServiceResourceManager) WithSpecificAuthType(cred ExplicitCredentialTypes, a Asserter, opts ...CreateAzCopyTargetOptions) AzCopyTarget {
	return CreateAzCopyTarget(m, cred, a, opts...)
}

func (m *MockServiceResourceManager) Canon() string {
	return fmt.Sprintf("%s/%s", m.parent.accountName, m.serviceType.String())
}

func (m *MockServiceResourceManager) URI(opts ...GetURIOptions) string {
	return ""
}

func (m *MockServiceResourceManager) Account() AccountResourceManager {
	return m.parent
}

func (m *MockServiceResourceManager) Parent() ResourceManager {
	return nil
}

func (m *MockServiceResourceManager) mockSignature() {
	panic("mockSignature should not be called")
}

func (m *MockServiceResourceManager) Location() common.Location {
	return m.serviceType
}

func (m *MockServiceResourceManager) Level() cmd.LocationLevel {
	return cmd.ELocationLevel.Service()
}

func (m *MockServiceResourceManager) ValidAuthTypes() ExplicitCredentialTypes {
	return mockServiceAuthTypes[m.serviceType]
}

func (m *MockServiceResourceManager) ResourceClient() any {
	panic("Test code should only perform \"real\" actions during wet runs. Does not create an emulated resource client.")
}

func (m *MockServiceResourceManager) GetResourceTarget(a Asserter) string {
	return ""
}

func (m *MockServiceResourceManager) ListContainers(a Asserter) []string {
	// No-op it
	return []string{}
}

func (m *MockServiceResourceManager) GetContainer(s string) ContainerResourceManager {
	return &MockContainerResourceManager{parent: m, account: m.parent, containerName: s}
}

func (m *MockServiceResourceManager) IsHierarchical() bool {
	return m.Location() == common.ELocation.File() || m.Location() == common.ELocation.BlobFS()
}

type MockContainerResourceManager struct {
	overrideLocation common.Location // Local doesn't expose service resource managers, so we have to work around it.
	account          *MockAccountResourceManager
	parent           *MockServiceResourceManager
	containerName    string
}

func (m *MockContainerResourceManager) ValidAuthTypes() ExplicitCredentialTypes {
	return mockServiceAuthTypes[m.Location()]
}

func (m *MockContainerResourceManager) DefaultAuthType() ExplicitCredentialTypes {
	return mockServiceDefaultAuthTypes[m.Location()]
}

func (m *MockContainerResourceManager) WithSpecificAuthType(cred ExplicitCredentialTypes, a Asserter, opts ...CreateAzCopyTargetOptions) AzCopyTarget {
	return CreateAzCopyTarget(m, cred, a, opts...)
}

func (m *MockContainerResourceManager) ResourceClient() any {
	panic("Test code should only perform \"real\" actions during wet runs. Does not create an emulated resource client.")
}

func (m *MockContainerResourceManager) Canon() string {
	return m.parent.Canon() + "/" + m.containerName
}

func (m *MockContainerResourceManager) Exists() bool {
	return true
}

func (m *MockContainerResourceManager) URI(opts ...GetURIOptions) string {
	return ""
}

func (m *MockContainerResourceManager) Parent() ResourceManager {
	return m.parent
}

func (m *MockContainerResourceManager) Account() AccountResourceManager {
	return m.account
}

func (m *MockContainerResourceManager) mockSignature() {
	panic("mockSignature should not be called")
}

func (m *MockContainerResourceManager) Location() common.Location {
	if m.overrideLocation != common.ELocation.Unknown() || m.parent == nil {
		return m.overrideLocation
	}

	return m.parent.Location()
}

func (m *MockContainerResourceManager) Level() cmd.LocationLevel {
	return cmd.ELocationLevel.Container()
}

func (m *MockContainerResourceManager) ContainerName() string {
	return m.containerName
}

func (m *MockContainerResourceManager) Create(a Asserter, props ContainerProperties) {
	// No-op it
}

func (m *MockContainerResourceManager) GetProperties(a Asserter) ContainerProperties {
	return ContainerProperties{}
}

func (m *MockContainerResourceManager) SetProperties(a Asserter, properties *ContainerResourceManager) {
}

func (m *MockContainerResourceManager) Delete(a Asserter) {
	// No-op it
}

func (m *MockContainerResourceManager) ListObjects(a Asserter, prefixOrDirectory string, recursive bool) map[string]ObjectProperties {
	// No-op it
	return map[string]ObjectProperties{}
}

func (m *MockContainerResourceManager) GetObject(a Asserter, path string, eType common.EntityType) ObjectResourceManager {
	return &MockObjectResourceManager{parent: m, account: m.account, entityType: eType, path: path}
}

func (m *MockContainerResourceManager) GetResourceTarget(a Asserter) string {
	return ""
}

type MockObjectResourceManager struct {
	overrideLocation   common.Location // If there is no parent, e.g. this is a lone file, it needs to be overridden
	parent             *MockContainerResourceManager
	account            *MockAccountResourceManager
	entityType         common.EntityType
	path               string
	hardlinkedFilePath string
}

func (m *MockObjectResourceManager) ValidAuthTypes() ExplicitCredentialTypes {
	return mockServiceAuthTypes[m.Location()]
}

func (m *MockObjectResourceManager) DefaultAuthType() ExplicitCredentialTypes {
	return mockServiceDefaultAuthTypes[m.Location()]
}

func (m *MockObjectResourceManager) WithSpecificAuthType(cred ExplicitCredentialTypes, a Asserter, opts ...CreateAzCopyTargetOptions) AzCopyTarget {
	return CreateAzCopyTarget(m, cred, a, opts...)
}

func (m *MockObjectResourceManager) ResourceClient() any {
	panic("Test code should only perform \"real\" actions during wet runs. Does not create an emulated resource client.")
}

func (m *MockObjectResourceManager) Canon() string {
	return m.parent.Canon() + "/" + m.path
}

func (m *MockObjectResourceManager) URI(opts ...GetURIOptions) string {
	return ""
}

func (m *MockObjectResourceManager) Parent() ResourceManager {
	return m.parent
}

func (m *MockObjectResourceManager) Account() AccountResourceManager {
	return m.account
}

func (m *MockObjectResourceManager) mockSignature() {
	panic("mockSignature should not be called")
}

func (m *MockObjectResourceManager) Location() common.Location {
	if m.overrideLocation != common.ELocation.Unknown() || m.parent == nil {
		return m.overrideLocation
	}

	return m.parent.Location()
}

func (m *MockObjectResourceManager) Level() cmd.LocationLevel {
	return cmd.ELocationLevel.Object()
}

func (m *MockObjectResourceManager) EntityType() common.EntityType {
	return m.entityType
}

func (m *MockObjectResourceManager) ContainerName() string {
	return m.parent.ContainerName()
}

func (m *MockObjectResourceManager) ObjectName() string {
	return m.path
}

func (l *MockObjectResourceManager) HardlinkedFileName() string {
	return l.hardlinkedFilePath
}

func (m *MockObjectResourceManager) Create(a Asserter, body ObjectContentContainer, properties ObjectProperties) {
	// no-op
}

func (m *MockObjectResourceManager) Delete(a Asserter) {
	// no-op
}

func (m *MockObjectResourceManager) ListChildren(a Asserter, recursive bool) map[string]ObjectProperties {
	a.Assert("Object must be a folder to list children", Equal{}, m.entityType, common.EEntityType.Folder())

	// no-op
	return map[string]ObjectProperties{}
}

func (m *MockObjectResourceManager) GetProperties(a Asserter) ObjectProperties {
	return ObjectProperties{}
}

func (m *MockObjectResourceManager) SetHTTPHeaders(a Asserter, h contentHeaders) {
	// no-op
}

func (m *MockObjectResourceManager) SetMetadata(a Asserter, metadata common.Metadata) {
	// no-op
}

func (m *MockObjectResourceManager) SetObjectProperties(a Asserter, props ObjectProperties) {
	// no-op
}

func (m *MockObjectResourceManager) Download(a Asserter) io.ReadSeeker {
	return bytes.NewReader([]byte{})
}

func (m *MockObjectResourceManager) Exists() bool {
	return true
}

func (b *MockObjectResourceManager) ReadLink(a Asserter) string {
	return ""
}
