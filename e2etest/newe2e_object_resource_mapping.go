package e2etest

import (
	"path"
)

// this should maybe be in newe2e_resource_definitions but it felt relevant to have on it's own

var _ ObjectResourceMapping = ObjectResourceMappingFlat{}
var _ ObjectResourceMapping = ObjectResourceMappingObject{}
var _ ObjectResourceMapping = ObjectResourceMappingFolder{}
var _ ObjectResourceMapping = ObjectResourceMappingParentFolder{}

type ObjectResourceMapping interface {
	// Flatten returns a set of objects, flat mapped, as the root of container space.
	// "" is the root, or self. This is useful for setting folder properties, or creating single files under folders.
	Flatten() map[string]ResourceDefinitionObject
}

type ObjectResourceMappingFlat map[string]ResourceDefinitionObject // todo: convert to hierarchical?

func (o ObjectResourceMappingFlat) Flatten() map[string]ResourceDefinitionObject {
	return o // We're already flat!
}

// ObjectResourceMappingOverlay appends new objects at the same level without modifying the underlying ObjectResourceMapping.
// Objects in Overlay overwrite objects in Base, so, this can also be used to override an object with custom options.
type ObjectResourceMappingOverlay struct {
	Base    ObjectResourceMapping
	Overlay ObjectResourceMapping
}

func (o ObjectResourceMappingOverlay) Flatten() map[string]ResourceDefinitionObject {
	var out map[string]ResourceDefinitionObject
	if o.Base != nil {
		out = o.Base.Flatten()
	} else {
		out = make(map[string]ResourceDefinitionObject)
	}

	if o.Overlay != nil {
		for k, v := range o.Overlay.Flatten() {
			out[k] = v
		}
	}

	return out
}

// ObjectResourceMappingParentFolder appends a parent folder to all objects under Children.
type ObjectResourceMappingParentFolder struct {
	FolderName string
	Children   ObjectResourceMapping
}

func (o ObjectResourceMappingParentFolder) Flatten() map[string]ResourceDefinitionObject {
	base := o.Children.Flatten()
	out := make(map[string]ResourceDefinitionObject)

	for k, v := range base {
		out[path.Join(o.FolderName, k)] = v
	}

	return out
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
