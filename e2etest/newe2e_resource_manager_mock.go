package e2etest

import (
	"github.com/Azure/azure-storage-azcopy/v10/cmd"
	"github.com/Azure/azure-storage-azcopy/v10/common"
)

type MockResourceManager struct {
	location common.Location
	level    cmd.LocationLevel
}

func (m MockResourceManager) Location() common.Location {
	return m.location
}

func (m MockResourceManager) Level() cmd.LocationLevel {
	return m.level
}

type MockRemoteResourceManager struct {
	MockResourceManager
	validAuthTypes ExplicitCredentialTypes
}

func (m MockRemoteResourceManager) ValidAuthTypes() ExplicitCredentialTypes {
	return m.validAuthTypes
}

func (m MockRemoteResourceManager) ResourceClient() any {
	panic("mock resource manager is not intended for use")
}
