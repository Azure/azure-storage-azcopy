package e2etest

import "path"

// this should maybe be in newe2e_resource_definitions but it felt relevant to have on it's own

type ObjectResourceMapping interface {
	// Flatten returns a set of objects, flat mapped, as the root of container space.
	// "" is the root, or self. This is useful for setting folder properties, or creating single files under folders.
	Flatten() map[string]ResourceDefinitionObject
}

type ObjectResourceMappingFlat map[string]ResourceDefinitionObject // todo: convert to hierarchical?

func (o ObjectResourceMappingFlat) Flatten() map[string]ResourceDefinitionObject {
	return o // We're already flat!
}

type ObjectResourceMappingFolder struct {
	ResourceDefinitionObject
	Children map[string]ObjectResourceMapping
}

func (o ObjectResourceMappingFolder) Flatten() map[string]ResourceDefinitionObject {
	out := map[string]ResourceDefinitionObject{
		"": o.ResourceDefinitionObject,
	}

	for childName, child := range o.Children {
		grandchildren := child.Flatten()
		for grandChildName, grandChild := range grandchildren {
			out[path.Join(childName, grandChildName)] = grandChild
		}
	}

	return out
}

type ObjectResourceMappingObject ResourceDefinitionObject

func (o ObjectResourceMappingObject) Flatten() map[string]ResourceDefinitionObject {
	return map[string]ResourceDefinitionObject{
		"": ResourceDefinitionObject(o),
	}
}
