package cmd

import (
	"github.com/Azure/azure-storage-azcopy/v10/azcopy"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/traverser"
)

type BucketToContainerNameResolver interface {
	ResolveName(string) (string, error)
}

func (cca *CookedCopyCmdArgs) validateSourceDir(source traverser.ResourceTraverser) error {
	var err error
	cca.IsSourceDir, err = azcopy.ValidateCopySourceDirectory(source, cca.Recursive, cca.StripTopDir)
	return err
}

func (cca *CookedCopyCmdArgs) InitModularFilters() []traverser.ObjectFilter {
	filters := traverser.BuildFilters(cca.FromTo, cca.Source, cca.Recursive, traverser.FilterOptions{
		IncludeBefore: cca.IncludeBefore, IncludeAfter: cca.IncludeAfter,
		IncludePatterns: cca.IncludePatterns, ExcludePatterns: cca.ExcludePatterns,
		ExcludePaths: cca.ExcludePathPatterns, IncludeRegex: cca.includeRegex, ExcludeRegex: cca.excludeRegex,
		ExcludeBlobTypes:  cca.excludeBlobType,
		IncludeAttributes: cca.IncludeFileAttributes, ExcludeAttributes: cca.ExcludeFileAttributes,
	})
	switch cca.permanentDeleteOption {
	case common.EPermanentDeleteOption.Snapshots():
		filters = append(filters, &traverser.PermDeleteFilter{DeleteSnapshots: true})
	case common.EPermanentDeleteOption.Versions():
		filters = append(filters, &traverser.PermDeleteFilter{DeleteVersions: true})
	case common.EPermanentDeleteOption.SnapshotsAndVersions():
		filters = append(filters, &traverser.PermDeleteFilter{DeleteSnapshots: true, DeleteVersions: true})
	}
	return filters
}

func (cca *CookedCopyCmdArgs) MakeEscapedRelativePath(source, dstIsDir bool, asSubDirOrObject any, objects ...traverser.StoredObject) string {
	asSubDir := cca.asSubdir
	object, ok := asSubDirOrObject.(traverser.StoredObject)
	if !ok {
		asSubDir = asSubDirOrObject.(bool)
		object = objects[0]
	}
	return (azcopy.CopyPathOptions{
		Source: cca.Source, Destination: cca.Destination, FromTo: cca.FromTo,
		StripTopDir: cca.StripTopDir, AsSubDir: asSubDir, DisableAutoDecoding: cca.disableAutoDecoding,
	}).MakeEscapedRelativePath(source, dstIsDir, object)
}

func NewFolderPropertyOption(fromTo common.FromTo, recursive, stripTopDir bool, filters []traverser.ObjectFilter, preserveInfo, preservePermissions, preservePOSIXProperties, isDstNull, includeDirectoryStubs bool) (common.FolderPropertyOption, string) {
	return azcopy.NewFolderPropertyOption(fromTo, recursive, stripTopDir, filters, preserveInfo, preservePermissions, preservePOSIXProperties, isDstNull, includeDirectoryStubs)
}
