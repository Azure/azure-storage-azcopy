package e2etest

import "github.com/Azure/azure-storage-azcopy/v10/common"

// ResourceDefinition todo define interface
type ResourceDefinition interface{}

type ResourceDefinitionService struct {
	Location   common.Location
	Containers map[string]ResourceDefinitionContainer
}

type ResourceDefinitionContainer struct {
	ContainerName *string
	Objects       map[string]ResourceDefinitionObject
}

type ResourceDefinitionObject struct {
	EntityType common.EntityType
	Name       *string
	Properties ObjectProperties
	Body       ObjectContentContainer
}
