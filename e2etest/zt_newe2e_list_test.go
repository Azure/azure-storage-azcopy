package e2etest

import (
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	blobsas "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/sas"
	"github.com/Azure/azure-storage-azcopy/v10/cmd"
	"github.com/Azure/azure-storage-azcopy/v10/common"
)

func init() {
	suiteManager.RegisterSuite(&ListSuite{})
}

type ListSuite struct{}

func (s *ListSuite) Scenario_ListBasic(svm *ScenarioVariationManager) {
	srcService := GetRootResource(svm, ResolveVariation(svm, []common.Location{common.ELocation.Blob(),
		common.ELocation.File()}))

	svm.InsertVariationSeparator(":")
	body := NewRandomObjectContentContainer(SizeFromString("1K"))
	var expectedObjects map[AzCopyOutputKey]cmd.AzCopyListObject
	if srcService.Location() == common.ELocation.Blob() {
		expectedObjects = map[AzCopyOutputKey]cmd.AzCopyListObject{}
	} else {
		expectedObjects = map[AzCopyOutputKey]cmd.AzCopyListObject{
			AzCopyOutputKey{Path: "/"}: {Path: "/", ContentLength: "0.00 B"},
		}
	}
	// Scale up from service to object
	srcObj := CreateResource[ObjectResourceManager](svm, srcService, ResourceDefinitionObject{
		ObjectName: pointerTo("test"),
		Body:       body,
	})
	expectedObjects[AzCopyOutputKey{Path: "test"}] = cmd.AzCopyListObject{Path: "test", ContentLength: "1.00 KiB"}

	stdout, _ := RunAzCopy(
		svm,
		AzCopyCommand{
			Verb: AzCopyVerbList,
			Targets: []ResourceManager{
				srcObj.Parent().(RemoteResourceManager).WithSpecificAuthType(ResolveVariation(svm,
					[]ExplicitCredentialTypes{EExplicitCredentialType.OAuth(),
						EExplicitCredentialType.SASToken()}), svm,
					CreateAzCopyTargetOptions{
						SASTokenOptions: GenericServiceSignatureValues{
							ContainerName: srcObj.ContainerName(),
							Permissions:   (&blobsas.ContainerPermissions{Read: true, List: true}).String(),
						},
					}),
			},
			Flags: ListFlags{},
		})

	ValidateListOutput(svm, stdout, expectedObjects, nil)
}

func (s *ListSuite) Scenario_ListHierarchy(svm *ScenarioVariationManager) {
	acctService := ResolveVariation(svm, []struct {
		acct string
		loc  common.Location
	}{
		{PrimaryStandardAcct, common.ELocation.Blob()},
		{PrimaryStandardAcct, common.ELocation.File()},
		{PrimaryHNSAcct, common.ELocation.BlobFS()},
	})

	acct := GetAccount(svm, acctService.acct)
	srcService := acct.GetService(svm, acctService.loc)

	svm.InsertVariationSeparator(":")
	srcContainer := CreateResource[ContainerResourceManager](svm, srcService, ResourceDefinitionContainer{})
	var expectedObjects map[AzCopyOutputKey]cmd.AzCopyListObject
	if srcService.Location() == common.ELocation.Blob() {
		expectedObjects = map[AzCopyOutputKey]cmd.AzCopyListObject{}
	} else {
		expectedObjects = map[AzCopyOutputKey]cmd.AzCopyListObject{
			AzCopyOutputKey{Path: "/"}: {Path: "/", ContentLength: "0.00 B"},
		}
	}
	objects := []ResourceDefinitionObject{
		{ObjectName: pointerTo("file_in_root.txt"), Body: NewRandomObjectContentContainer(SizeFromString("1K")), Size: "1.00 KiB"},
		{ObjectName: pointerTo("dir_in_root"), ObjectProperties: ObjectProperties{EntityType: common.EEntityType.Folder()}, Size: "0.00 B"},
		{ObjectName: pointerTo("dir_in_root/file.txt"), Body: NewRandomObjectContentContainer(SizeFromString("2K")), Size: "2.00 KiB"},
		{ObjectName: pointerTo("dir_in_root/subdir"), ObjectProperties: ObjectProperties{EntityType: common.EEntityType.Folder()}, Size: "0.00 B"},
	}
	// Scale up from service to object
	for _, o := range objects {
		obj := CreateResource[ObjectResourceManager](svm, srcContainer, o)
		name := obj.ObjectName()
		if o.EntityType == common.EEntityType.Folder() {
			name += "/"
			if obj.Location() == common.ELocation.Blob() {
				continue
			}
		}
		expectedObjects[AzCopyOutputKey{Path: name}] = cmd.AzCopyListObject{Path: name, ContentLength: o.Size}
	}

	stdout, _ := RunAzCopy(
		svm,
		AzCopyCommand{
			Verb: AzCopyVerbList,
			Targets: []ResourceManager{
				srcContainer.(RemoteResourceManager).WithSpecificAuthType(EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{
					SASTokenOptions: GenericServiceSignatureValues{
						ContainerName: srcContainer.ContainerName(),
						Permissions:   (&blobsas.ContainerPermissions{Read: true, List: true}).String(),
					},
				}),
			},
			Flags: ListFlags{},
		})

	ValidateListOutput(svm, stdout, expectedObjects, nil)
}

func (s *ListSuite) Scenario_ListProperties(svm *ScenarioVariationManager) {
	acct := GetAccount(svm, PrimaryStandardAcct)
	srcService := acct.GetService(svm, common.ELocation.Blob())

	svm.InsertVariationSeparator(":")
	srcContainer := CreateResource[ContainerResourceManager](svm, srcService, ResourceDefinitionContainer{})

	// Create expected objects
	expectedObjects := make(map[AzCopyOutputKey]cmd.AzCopyListObject)
	blobNames := []string{"AzURE2021.jpeg", "sub1/dir2/HELLO-4.txt", "sub1/test/testing.txt"}
	for _, blobName := range blobNames {
		obj := CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
			ObjectName: pointerTo(blobName),
			Body:       NewRandomObjectContentContainer(SizeFromString("1K")),
		})
		props := obj.GetProperties(svm)
		versionId := common.IffNotNil(props.BlobProperties.VersionId, "")
		expectedObjects[AzCopyOutputKey{Path: blobName, VersionId: versionId}] = cmd.AzCopyListObject{
			Path:             blobName,
			ContentLength:    "1.00 KiB",
			LastModifiedTime: props.LastModifiedTime,
			VersionId:        versionId,
			BlobType:         common.IffNotNil(props.BlobProperties.Type, ""),
			BlobAccessTier:   common.IffNotNil(props.BlobProperties.BlockBlobAccessTier, ""),
			ContentType:      common.IffNotNil(props.HTTPHeaders.contentType, ""),
			ContentEncoding:  common.IffNotNil(props.HTTPHeaders.contentEncoding, ""),
			ContentMD5:       props.HTTPHeaders.contentMD5,
			LeaseState:       common.IffNotNil(props.BlobProperties.LeaseState, ""),
			LeaseDuration:    common.IffNotNil(props.BlobProperties.LeaseDuration, ""),
			LeaseStatus:      common.IffNotNil(props.BlobProperties.LeaseStatus, ""),
			ArchiveStatus:    common.IffNotNil(props.BlobProperties.ArchiveStatus, ""),
		}
	}

	stdout, _ := RunAzCopy(
		svm,
		AzCopyCommand{
			Verb: AzCopyVerbList,
			Targets: []ResourceManager{
				srcContainer.(RemoteResourceManager).WithSpecificAuthType(EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{
					SASTokenOptions: GenericServiceSignatureValues{
						ContainerName: srcContainer.ContainerName(),
						Permissions:   (&blobsas.ContainerPermissions{Read: true, List: true}).String(),
					},
				}),
			},
			Flags: ListFlags{
				Properties: to.Ptr("LastModifiedTime;VersionId;BlobType;BlobAccessTier;ContentType;ContentEncoding;ContentMD5;LeaseState;LeaseStatus;LeaseDuration;ArchiveStatus"),
			},
		})

	ValidateListOutput(svm, stdout, expectedObjects, nil)
}

func (s *ListSuite) Scenario_ListProperties_TextOutput(svm *ScenarioVariationManager) {
	acct := GetAccount(svm, PrimaryStandardAcct)
	srcService := acct.GetService(svm, common.ELocation.Blob())

	svm.InsertVariationSeparator(":")
	srcContainer := CreateResource[ContainerResourceManager](svm, srcService, ResourceDefinitionContainer{})

	// Create expected objects
	expectedObjects := make(map[AzCopyOutputKey]cmd.AzCopyListObject)
	blobNames := []string{"AzURE2021.jpeg", "sub1/dir2/HELLO-4.txt", "sub1/test/testing.txt"}
	for _, blobName := range blobNames {
		obj := CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
			ObjectName: pointerTo(blobName),
			Body:       NewRandomObjectContentContainer(SizeFromString("1K")),
		})
		props := obj.GetProperties(svm)
		versionId := common.IffNotNil(props.BlobProperties.VersionId, "")
		expectedObjects[AzCopyOutputKey{Path: blobName, VersionId: versionId}] = cmd.AzCopyListObject{
			Path:             blobName,
			ContentLength:    "1.00 KiB",
			LastModifiedTime: props.LastModifiedTime,
			VersionId:        versionId,
			BlobType:         common.IffNotNil(props.BlobProperties.Type, ""),
			BlobAccessTier:   common.IffNotNil(props.BlobProperties.BlockBlobAccessTier, ""),
			ContentType:      common.IffNotNil(props.HTTPHeaders.contentType, ""),
			ContentEncoding:  common.IffNotNil(props.HTTPHeaders.contentEncoding, ""),
			ContentMD5:       props.HTTPHeaders.contentMD5,
			LeaseState:       common.IffNotNil(props.BlobProperties.LeaseState, ""),
			LeaseDuration:    common.IffNotNil(props.BlobProperties.LeaseDuration, ""),
			LeaseStatus:      common.IffNotNil(props.BlobProperties.LeaseStatus, ""),
			ArchiveStatus:    common.IffNotNil(props.BlobProperties.ArchiveStatus, ""),
		}
	}

	stdout, _ := RunAzCopy(
		svm,
		AzCopyCommand{
			Verb: AzCopyVerbList,
			Targets: []ResourceManager{
				srcContainer.(RemoteResourceManager).WithSpecificAuthType(EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{
					SASTokenOptions: GenericServiceSignatureValues{
						ContainerName: srcContainer.ContainerName(),
						Permissions:   (&blobsas.ContainerPermissions{Read: true, List: true}).String(),
					},
				}),
			},
			Flags: ListFlags{
				GlobalFlags: GlobalFlags{
					OutputType:       to.Ptr(common.EOutputFormat.Text()),
					SkipVersionCheck: to.Ptr(true),
				},
				Properties: to.Ptr("LastModifiedTime;VersionId;BlobType;BlobAccessTier;ContentType;ContentEncoding;ContentMD5;LeaseState;LeaseStatus;LeaseDuration;ArchiveStatus"),
			},
		})

	ValidateListTextOutput(svm, stdout, expectedObjects, nil)
}

func (s *ListSuite) Scenario_ListPropertiesInvalid(svm *ScenarioVariationManager) {
	acct := GetAccount(svm, PrimaryStandardAcct)
	srcService := acct.GetService(svm, common.ELocation.Blob())

	svm.InsertVariationSeparator(":")
	srcContainer := CreateResource[ContainerResourceManager](svm, srcService, ResourceDefinitionContainer{})

	// Create expected objects
	expectedObjects := make(map[AzCopyOutputKey]cmd.AzCopyListObject)
	blobNames := []string{"AzURE2021.jpeg", "sub1/dir2/HELLO-4.txt", "sub1/test/testing.txt"}
	for _, blobName := range blobNames {
		obj := CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
			ObjectName: pointerTo(blobName),
			Body:       NewRandomObjectContentContainer(SizeFromString("1K")),
		})
		props := obj.GetProperties(svm)
		expectedObjects[AzCopyOutputKey{Path: blobName}] = cmd.AzCopyListObject{
			Path:             blobName,
			ContentLength:    "1.00 KiB",
			LastModifiedTime: props.LastModifiedTime,
		}
	}

	stdout, _ := RunAzCopy(
		svm,
		AzCopyCommand{
			Verb: AzCopyVerbList,
			Targets: []ResourceManager{
				srcContainer.(RemoteResourceManager).WithSpecificAuthType(EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{
					SASTokenOptions: GenericServiceSignatureValues{
						ContainerName: srcContainer.ContainerName(),
						Permissions:   (&blobsas.ContainerPermissions{Read: true, List: true}).String(),
					},
				}),
			},
			Flags: ListFlags{
				Properties: to.Ptr("LastModifiedTime;UnsupportedProperty"),
			},
		})

	ValidateListOutput(svm, stdout, expectedObjects, nil)
}

func (s *ListSuite) Scenario_ListMachineReadable(svm *ScenarioVariationManager) {
	acct := GetAccount(svm, PrimaryStandardAcct)
	srcService := acct.GetService(svm, common.ELocation.Blob())

	svm.InsertVariationSeparator(":")
	srcContainer := CreateResource[ContainerResourceManager](svm, srcService, ResourceDefinitionContainer{})

	// Create expected objects
	expectedObjects := make(map[AzCopyOutputKey]cmd.AzCopyListObject)
	blobNames := []string{"AzURE2021.jpeg", "sub1/dir2/HELLO-4.txt", "sub1/test/testing.txt"}
	for _, blobName := range blobNames {
		CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
			ObjectName: pointerTo(blobName),
			Body:       NewRandomObjectContentContainer(SizeFromString("1K")),
		})
		expectedObjects[AzCopyOutputKey{Path: blobName}] = cmd.AzCopyListObject{
			Path:          blobName,
			ContentLength: "1024",
		}
	}

	stdout, _ := RunAzCopy(
		svm,
		AzCopyCommand{
			Verb: AzCopyVerbList,
			Targets: []ResourceManager{
				srcContainer.(RemoteResourceManager).WithSpecificAuthType(EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{
					SASTokenOptions: GenericServiceSignatureValues{
						ContainerName: srcContainer.ContainerName(),
						Permissions:   (&blobsas.ContainerPermissions{Read: true, List: true}).String(),
					},
				}),
			},
			Flags: ListFlags{
				MachineReadable: to.Ptr(true),
			},
		})

	ValidateListOutput(svm, stdout, expectedObjects, nil)
}

func (s *ListSuite) Scenario_ListMegaUnits(svm *ScenarioVariationManager) {
	acct := GetAccount(svm, PrimaryStandardAcct)
	srcService := acct.GetService(svm, common.ELocation.Blob())

	svm.InsertVariationSeparator(":")
	srcContainer := CreateResource[ContainerResourceManager](svm, srcService, ResourceDefinitionContainer{})

	// Create expected objects
	expectedObjects := make(map[AzCopyOutputKey]cmd.AzCopyListObject)
	blobNames := []string{"AzURE2021.jpeg", "sub1/dir2/HELLO-4.txt", "sub1/test/testing.txt"}
	for _, blobName := range blobNames {
		CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
			ObjectName: pointerTo(blobName),
			Body:       NewRandomObjectContentContainer(SizeFromString("1K")),
		})
		expectedObjects[AzCopyOutputKey{Path: blobName}] = cmd.AzCopyListObject{
			Path:          blobName,
			ContentLength: "1.02 KB",
		}
	}

	stdout, _ := RunAzCopy(
		svm,
		AzCopyCommand{
			Verb: AzCopyVerbList,
			Targets: []ResourceManager{
				srcContainer.(RemoteResourceManager).WithSpecificAuthType(EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{
					SASTokenOptions: GenericServiceSignatureValues{
						ContainerName: srcContainer.ContainerName(),
						Permissions:   (&blobsas.ContainerPermissions{Read: true, List: true}).String(),
					},
				}),
			},
			Flags: ListFlags{
				MegaUnits: to.Ptr(true),
			},
		})

	ValidateListOutput(svm, stdout, expectedObjects, nil)
}

func (s *ListSuite) Scenario_ListBasic_TextOutput(svm *ScenarioVariationManager) {
	acct := GetAccount(svm, PrimaryStandardAcct)
	srcService := acct.GetService(svm, common.ELocation.Blob())

	body := NewRandomObjectContentContainer(SizeFromString("1K"))
	// Scale up from service to object
	srcObj := CreateResource[ObjectResourceManager](svm, srcService, ResourceDefinitionObject{
		ObjectName: pointerTo("test"),
		Body:       body,
	})

	stdout, _ := RunAzCopy(
		svm,
		AzCopyCommand{
			Verb: AzCopyVerbList,
			Targets: []ResourceManager{
				srcObj.Parent().(RemoteResourceManager).WithSpecificAuthType(EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{
					SASTokenOptions: GenericServiceSignatureValues{
						ContainerName: srcObj.ContainerName(),
						Permissions:   (&blobsas.ContainerPermissions{Read: true, List: true}).String(),
					},
				}),
			},
			Flags: ListFlags{
				RunningTally: to.Ptr(true),
				GlobalFlags: GlobalFlags{
					OutputType:       to.Ptr(common.EOutputFormat.Text()),
					SkipVersionCheck: to.Ptr(true),
				},
			},
		})

	expectedObjects := map[AzCopyOutputKey]cmd.AzCopyListObject{
		AzCopyOutputKey{Path: "test"}: {Path: "test", ContentLength: "1.00 KiB"},
	}
	expectedSummary := &cmd.AzCopyListSummary{FileCount: "1", TotalFileSize: "1.00 KiB"}
	ValidateListTextOutput(svm, stdout, expectedObjects, expectedSummary)
}

func (s *ListSuite) Scenario_ListRunningTally(svm *ScenarioVariationManager) {
	acct := GetAccount(svm, PrimaryStandardAcct)
	srcService := acct.GetService(svm, common.ELocation.Blob())

	body := NewRandomObjectContentContainer(SizeFromString("1K"))
	// Scale up from service to object
	srcObj := CreateResource[ObjectResourceManager](svm, srcService, ResourceDefinitionObject{
		ObjectName: pointerTo("test"),
		Body:       body,
	}) // todo: generic CreateResource is something to pursue in another branch, but it's an interesting thought.

	stdout, _ := RunAzCopy(
		svm,
		AzCopyCommand{
			Verb: AzCopyVerbList,
			Targets: []ResourceManager{
				srcObj.Parent().(RemoteResourceManager).WithSpecificAuthType(EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{
					SASTokenOptions: GenericServiceSignatureValues{
						ContainerName: srcObj.ContainerName(),
						Permissions:   (&blobsas.ContainerPermissions{Read: true, List: true}).String(),
					},
				}),
			},
			Flags: ListFlags{
				RunningTally: to.Ptr(true),
			},
		})

	expectedObjects := map[AzCopyOutputKey]cmd.AzCopyListObject{
		AzCopyOutputKey{Path: "test"}: {Path: "test", ContentLength: "1.00 KiB"},
	}
	expectedSummary := &cmd.AzCopyListSummary{FileCount: "1", TotalFileSize: "1.00 KiB"}
	ValidateListOutput(svm, stdout, expectedObjects, expectedSummary)
}

func (s *ListSuite) Scenario_ListRunningTallyMegaUnits(svm *ScenarioVariationManager) {
	acct := GetAccount(svm, PrimaryStandardAcct)
	srcService := acct.GetService(svm, common.ELocation.Blob())

	svm.InsertVariationSeparator(":")
	srcContainer := CreateResource[ContainerResourceManager](svm, srcService, ResourceDefinitionContainer{})

	// Create expected objects
	expectedObjects := make(map[AzCopyOutputKey]cmd.AzCopyListObject)
	blobNames := []string{"AzURE2021.jpeg", "sub1/dir2/HELLO-4.txt", "sub1/test/testing.txt"}
	for _, blobName := range blobNames {
		CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
			ObjectName: pointerTo(blobName),
			Body:       NewRandomObjectContentContainer(SizeFromString("1K")),
		})
		expectedObjects[AzCopyOutputKey{Path: blobName}] = cmd.AzCopyListObject{
			Path:          blobName,
			ContentLength: "1.02 KB",
		}
	}

	stdout, _ := RunAzCopy(
		svm,
		AzCopyCommand{
			Verb: AzCopyVerbList,
			Targets: []ResourceManager{
				srcContainer.(RemoteResourceManager).WithSpecificAuthType(EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{
					SASTokenOptions: GenericServiceSignatureValues{
						ContainerName: srcContainer.ContainerName(),
						Permissions:   (&blobsas.ContainerPermissions{Read: true, List: true}).String(),
					},
				}),
			},
			Flags: ListFlags{
				MegaUnits:    to.Ptr(true),
				RunningTally: to.Ptr(true),
			},
		})

	expectedSummary := &cmd.AzCopyListSummary{FileCount: "3", TotalFileSize: "3.07 KB"}
	ValidateListOutput(svm, stdout, expectedObjects, expectedSummary)
}

func (s *ListSuite) Scenario_ListRunningTallyMachineReadable(svm *ScenarioVariationManager) {
	acct := GetAccount(svm, PrimaryStandardAcct)
	srcService := acct.GetService(svm, common.ELocation.Blob())

	svm.InsertVariationSeparator(":")
	srcContainer := CreateResource[ContainerResourceManager](svm, srcService, ResourceDefinitionContainer{})

	// Create expected objects
	expectedObjects := make(map[AzCopyOutputKey]cmd.AzCopyListObject)
	blobNames := []string{"AzURE2021.jpeg", "sub1/dir2/HELLO-4.txt", "sub1/test/testing.txt"}
	for _, blobName := range blobNames {
		CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
			ObjectName: pointerTo(blobName),
			Body:       NewRandomObjectContentContainer(SizeFromString("1K")),
		})
		expectedObjects[AzCopyOutputKey{Path: blobName}] = cmd.AzCopyListObject{
			Path:          blobName,
			ContentLength: "1024",
		}
	}

	stdout, _ := RunAzCopy(
		svm,
		AzCopyCommand{
			Verb: AzCopyVerbList,
			Targets: []ResourceManager{
				srcContainer.(RemoteResourceManager).WithSpecificAuthType(EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{
					SASTokenOptions: GenericServiceSignatureValues{
						ContainerName: srcContainer.ContainerName(),
						Permissions:   (&blobsas.ContainerPermissions{Read: true, List: true}).String(),
					},
				}),
			},
			Flags: ListFlags{
				MachineReadable: to.Ptr(true),
				RunningTally:    to.Ptr(true),
			},
		})

	expectedSummary := &cmd.AzCopyListSummary{FileCount: "3", TotalFileSize: "3072"}
	ValidateListOutput(svm, stdout, expectedObjects, expectedSummary)
}

func (s *ListSuite) Scenario_ListVersionIdNoAdditionalVersions(svm *ScenarioVariationManager) {
	acct := GetAccount(svm, PrimaryStandardAcct)
	srcService := acct.GetService(svm, common.ELocation.Blob())
	srcContainer := CreateResource[ContainerResourceManager](svm, srcService, ResourceDefinitionContainer{})

	// Create expected objects
	expectedObjects := make(map[AzCopyOutputKey]cmd.AzCopyListObject)
	blobNames := []string{"AzURE2021.jpeg", "sub1/dir2/HELLO-4.txt", "sub1/test/testing.txt"}
	for _, blobName := range blobNames {
		obj := CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
			ObjectName: pointerTo(blobName),
			Body:       NewRandomObjectContentContainer(SizeFromString("1K")),
		})
		props := obj.GetProperties(svm)
		versionId := common.IffNotNil(props.BlobProperties.VersionId, "")
		expectedObjects[AzCopyOutputKey{Path: blobName, VersionId: versionId}] = cmd.AzCopyListObject{Path: blobName, ContentLength: "1.00 KiB", VersionId: versionId}
	}

	stdout, _ := RunAzCopy(
		svm,
		AzCopyCommand{
			Verb: AzCopyVerbList,
			Targets: []ResourceManager{
				srcContainer.(RemoteResourceManager).WithSpecificAuthType(EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{
					SASTokenOptions: GenericServiceSignatureValues{
						ContainerName: srcContainer.ContainerName(),
						Permissions:   (&blobsas.ContainerPermissions{Read: true, List: true}).String(),
					},
				}),
			},
			Flags: ListFlags{
				RunningTally: to.Ptr(true),
				Properties:   to.Ptr("VersionId"),
			},
		})

	expectedSummary := &cmd.AzCopyListSummary{FileCount: "3", TotalFileSize: "3.00 KiB"}
	ValidateListOutput(svm, stdout, expectedObjects, expectedSummary)
}

func (s *ListSuite) Scenario_ListVersionIdNoAdditionalVersions_TextOutput(svm *ScenarioVariationManager) {
	acct := GetAccount(svm, PrimaryStandardAcct)
	srcService := acct.GetService(svm, common.ELocation.Blob())

	srcContainer := CreateResource[ContainerResourceManager](svm, srcService, ResourceDefinitionContainer{})

	// Create expected objects
	expectedObjects := make(map[AzCopyOutputKey]cmd.AzCopyListObject)
	blobNames := []string{"AzURE2021.jpeg", "sub1/dir2/HELLO-4.txt", "sub1/test/testing.txt"}
	for _, blobName := range blobNames {
		obj := CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
			ObjectName: pointerTo(blobName),
			Body:       NewRandomObjectContentContainer(SizeFromString("1K")),
		})
		props := obj.GetProperties(svm)
		versionId := common.IffNotNil(props.BlobProperties.VersionId, "")
		expectedObjects[AzCopyOutputKey{Path: blobName, VersionId: versionId}] = cmd.AzCopyListObject{Path: blobName, ContentLength: "1.00 KiB", VersionId: versionId}
	}

	stdout, _ := RunAzCopy(
		svm,
		AzCopyCommand{
			Verb: AzCopyVerbList,
			Targets: []ResourceManager{
				srcContainer.(RemoteResourceManager).WithSpecificAuthType(EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{
					SASTokenOptions: GenericServiceSignatureValues{
						ContainerName: srcContainer.ContainerName(),
						Permissions:   (&blobsas.ContainerPermissions{Read: true, List: true}).String(),
					},
				}),
			},
			Flags: ListFlags{
				GlobalFlags: GlobalFlags{
					OutputType:       to.Ptr(common.EOutputFormat.Text()),
					SkipVersionCheck: to.Ptr(true),
				},
				RunningTally: to.Ptr(true),
				Properties:   to.Ptr("VersionId"),
			},
		})

	expectedSummary := &cmd.AzCopyListSummary{FileCount: "3", TotalFileSize: "3.00 KiB"}
	ValidateListTextOutput(svm, stdout, expectedObjects, expectedSummary)
}

func (s *ListSuite) Scenario_ListVersionIdWithVersions(svm *ScenarioVariationManager) {
	acct := GetAccount(svm, PrimaryStandardAcct)
	srcService := acct.GetService(svm, common.ELocation.Blob())

	srcContainer := CreateResource[ContainerResourceManager](svm, srcService, ResourceDefinitionContainer{})

	// Create expected objects
	expectedObjects := make(map[AzCopyOutputKey]cmd.AzCopyListObject)
	blobNames := []string{"foo.txt", "foo/foo.txt", "test/foo.txt", "sub1/test/baz.txt"}
	for i, blobName := range blobNames {
		obj := CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
			ObjectName: pointerTo(blobName),
			Body:       NewRandomObjectContentContainer(SizeFromString("1K")),
		})
		props := obj.GetProperties(svm)
		versionId := common.IffNotNil(props.BlobProperties.VersionId, "")
		expectedObjects[AzCopyOutputKey{Path: blobName, VersionId: versionId}] = cmd.AzCopyListObject{Path: blobName, ContentLength: "1.00 KiB", VersionId: versionId}

		// Create a new version of the blob for the first two blobs
		if i < 2 {
			obj.Create(svm, NewRandomObjectContentContainer(SizeFromString("2K")), ObjectProperties{})
			props = obj.GetProperties(svm)
			versionId = common.IffNotNil(props.BlobProperties.VersionId, "")
			expectedObjects[AzCopyOutputKey{Path: blobName, VersionId: versionId}] = cmd.AzCopyListObject{Path: blobName, ContentLength: "2.00 KiB", VersionId: versionId}
		}
	}

	stdout, _ := RunAzCopy(
		svm,
		AzCopyCommand{
			Verb: AzCopyVerbList,
			Targets: []ResourceManager{
				srcContainer.(RemoteResourceManager).WithSpecificAuthType(EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{
					SASTokenOptions: GenericServiceSignatureValues{
						ContainerName: srcContainer.ContainerName(),
						Permissions:   (&blobsas.ContainerPermissions{Read: true, List: true}).String(),
					},
				}),
			},
			Flags: ListFlags{
				RunningTally: to.Ptr(true),
				Properties:   to.Ptr("VersionId"),
			},
		})

	expectedSummary := &cmd.AzCopyListSummary{FileCount: "4", TotalFileSize: "6.00 KiB"}
	ValidateListOutput(svm, stdout, expectedObjects, expectedSummary)
}

func (s *ListSuite) Scenario_ListWithVersions(svm *ScenarioVariationManager) {
	acct := GetAccount(svm, PrimaryStandardAcct)
	srcService := acct.GetService(svm, common.ELocation.Blob())
	srcContainer := CreateResource[ContainerResourceManager](svm, srcService, ResourceDefinitionContainer{})

	// Create expected objects
	expectedObjects := make(map[AzCopyOutputKey]cmd.AzCopyListObject)
	blobNames := []string{"foo.txt", "foo/foo.txt", "test/foo.txt", "sub1/test/baz.txt"}
	for i, blobName := range blobNames {
		obj := CreateResource[ObjectResourceManager](svm, srcContainer, ResourceDefinitionObject{
			ObjectName: pointerTo(blobName),
			Body:       NewRandomObjectContentContainer(SizeFromString("1K")),
		})

		// Create a new version of the blob for the first two blobs
		if i < 2 {
			obj.Create(svm, NewRandomObjectContentContainer(SizeFromString("2K")), ObjectProperties{})
			expectedObjects[AzCopyOutputKey{Path: blobName}] = cmd.AzCopyListObject{Path: blobName, ContentLength: "2.00 KiB"}
		} else {
			expectedObjects[AzCopyOutputKey{Path: blobName}] = cmd.AzCopyListObject{Path: blobName, ContentLength: "1.00 KiB"}
		}
	}

	stdout, _ := RunAzCopy(
		svm,
		AzCopyCommand{
			Verb: AzCopyVerbList,
			Targets: []ResourceManager{
				srcContainer.(RemoteResourceManager).WithSpecificAuthType(EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{
					SASTokenOptions: GenericServiceSignatureValues{
						ContainerName: srcContainer.ContainerName(),
						Permissions:   (&blobsas.ContainerPermissions{Read: true, List: true}).String(),
					},
				}),
			},
			Flags: ListFlags{
				RunningTally: to.Ptr(true),
			},
		})

	expectedSummary := &cmd.AzCopyListSummary{FileCount: "4", TotalFileSize: "6.00 KiB"}
	ValidateListOutput(svm, stdout, expectedObjects, expectedSummary)
}

func (s *ListSuite) Scenario_ListHierarchyTrailingDot(svm *ScenarioVariationManager) {
	acct := GetAccount(svm, PrimaryStandardAcct)
	srcService := acct.GetService(svm, common.ELocation.File())

	svm.InsertVariationSeparator(":")
	srcContainer := CreateResource[ContainerResourceManager](svm, srcService, ResourceDefinitionContainer{})
	var expectedObjects map[AzCopyOutputKey]cmd.AzCopyListObject
	if srcService.Location() == common.ELocation.Blob() {
		expectedObjects = map[AzCopyOutputKey]cmd.AzCopyListObject{}
	} else {
		expectedObjects = map[AzCopyOutputKey]cmd.AzCopyListObject{
			AzCopyOutputKey{Path: "/"}: {Path: "/", ContentLength: "0.00 B"},
		}
	}
	objects := []ResourceDefinitionObject{
		{ObjectName: pointerTo("file_in_root"), Body: NewRandomObjectContentContainer(SizeFromString("1K")), Size: "1.00 KiB"},
		{ObjectName: pointerTo("file_in_root."), Body: NewRandomObjectContentContainer(SizeFromString("1K")), Size: "1.00 KiB"},
		{ObjectName: pointerTo("dir_in_root."), ObjectProperties: ObjectProperties{EntityType: common.EEntityType.Folder()}, Size: "0.00 B"},
		{ObjectName: pointerTo("dir_in_root./file"), Body: NewRandomObjectContentContainer(SizeFromString("2K")), Size: "2.00 KiB"},
		{ObjectName: pointerTo("dir_in_root./file."), Body: NewRandomObjectContentContainer(SizeFromString("2K")), Size: "2.00 KiB"},
		{ObjectName: pointerTo("dir_in_root./subdir"), ObjectProperties: ObjectProperties{EntityType: common.EEntityType.Folder()}, Size: "0.00 B"},
		{ObjectName: pointerTo("dir_in_root./subdir."), ObjectProperties: ObjectProperties{EntityType: common.EEntityType.Folder()}, Size: "0.00 B"},
		{ObjectName: pointerTo("dir_in_root"), ObjectProperties: ObjectProperties{EntityType: common.EEntityType.Folder()}, Size: "0.00 B"},
		{ObjectName: pointerTo("dir_in_root/file"), Body: NewRandomObjectContentContainer(SizeFromString("2K")), Size: "2.00 KiB"},
		{ObjectName: pointerTo("dir_in_root/file."), Body: NewRandomObjectContentContainer(SizeFromString("2K")), Size: "2.00 KiB"},
		{ObjectName: pointerTo("dir_in_root/subdir"), ObjectProperties: ObjectProperties{EntityType: common.EEntityType.Folder()}, Size: "0.00 B"},
		{ObjectName: pointerTo("dir_in_root/subdir."), ObjectProperties: ObjectProperties{EntityType: common.EEntityType.Folder()}, Size: "0.00 B"},
	}
	// Scale up from service to object
	for _, o := range objects {
		obj := CreateResource[ObjectResourceManager](svm, srcContainer, o)
		name := obj.ObjectName()
		if o.EntityType == common.EEntityType.Folder() {
			name += "/"
		}
		expectedObjects[AzCopyOutputKey{Path: name}] = cmd.AzCopyListObject{Path: name, ContentLength: o.Size}
	}

	stdout, _ := RunAzCopy(
		svm,
		AzCopyCommand{
			Verb: AzCopyVerbList,
			Targets: []ResourceManager{
				srcContainer.(RemoteResourceManager).WithSpecificAuthType(EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{
					SASTokenOptions: GenericServiceSignatureValues{
						ContainerName: srcContainer.ContainerName(),
						Permissions:   (&blobsas.ContainerPermissions{Read: true, List: true}).String(),
					},
				}),
			},
			Flags: ListFlags{
				TrailingDot: to.Ptr(common.ETrailingDotOption.Enable()),
			},
		})

	ValidateListOutput(svm, stdout, expectedObjects, nil)
}

func (s *ListSuite) Scenario_ListHierarchyTrailingDotDisable(svm *ScenarioVariationManager) {
	acct := GetAccount(svm, PrimaryStandardAcct)
	srcService := acct.GetService(svm, common.ELocation.File())

	svm.InsertVariationSeparator(":")
	srcContainer := CreateResource[ContainerResourceManager](svm, srcService, ResourceDefinitionContainer{})
	var expectedObjects map[AzCopyOutputKey]cmd.AzCopyListObject
	if srcService.Location() == common.ELocation.Blob() {
		expectedObjects = map[AzCopyOutputKey]cmd.AzCopyListObject{}
	} else {
		expectedObjects = map[AzCopyOutputKey]cmd.AzCopyListObject{
			AzCopyOutputKey{Path: "/"}: {Path: "/", ContentLength: "0.00 B"},
		}
	}
	objects := []ResourceDefinitionObject{
		{ObjectName: pointerTo("file_in_root"), Body: NewRandomObjectContentContainer(SizeFromString("1K")), Size: "1.00 KiB"},
		{ObjectName: pointerTo("file_in_root."), Body: NewRandomObjectContentContainer(SizeFromString("1K")), Size: "1.00 KiB"},
		{ObjectName: pointerTo("dir_in_root."), ObjectProperties: ObjectProperties{EntityType: common.EEntityType.Folder()}, Size: "0.00 B"},
		{ObjectName: pointerTo("dir_in_root./file"), Body: NewRandomObjectContentContainer(SizeFromString("2K")), Size: "2.00 KiB"},
		{ObjectName: pointerTo("dir_in_root./file."), Body: NewRandomObjectContentContainer(SizeFromString("2K")), Size: "2.00 KiB"},
		{ObjectName: pointerTo("dir_in_root./subdir"), ObjectProperties: ObjectProperties{EntityType: common.EEntityType.Folder()}, Size: "0.00 B"},
		{ObjectName: pointerTo("dir_in_root./subdir."), ObjectProperties: ObjectProperties{EntityType: common.EEntityType.Folder()}, Size: "0.00 B"},
		{ObjectName: pointerTo("dir_in_root"), ObjectProperties: ObjectProperties{EntityType: common.EEntityType.Folder()}, Size: "0.00 B"},
		{ObjectName: pointerTo("dir_in_root/file"), Body: NewRandomObjectContentContainer(SizeFromString("2K")), Size: "2.00 KiB"},
		{ObjectName: pointerTo("dir_in_root/file."), Body: NewRandomObjectContentContainer(SizeFromString("2K")), Size: "2.00 KiB"},
		{ObjectName: pointerTo("dir_in_root/subdir"), ObjectProperties: ObjectProperties{EntityType: common.EEntityType.Folder()}, Size: "0.00 B"},
		{ObjectName: pointerTo("dir_in_root/subdir."), ObjectProperties: ObjectProperties{EntityType: common.EEntityType.Folder()}, Size: "0.00 B"},
	}
	// Scale up from service to object
	for _, o := range objects {
		obj := CreateResource[ObjectResourceManager](svm, srcContainer, o)
		name := obj.ObjectName()
		if o.EntityType == common.EEntityType.Folder() {
			name += "/"
		}
		expectedObjects[AzCopyOutputKey{Path: name}] = cmd.AzCopyListObject{Path: name, ContentLength: o.Size}
	}

	stdout, _ := RunAzCopy(
		svm,
		AzCopyCommand{
			Verb: AzCopyVerbList,
			Targets: []ResourceManager{
				srcContainer.(RemoteResourceManager).WithSpecificAuthType(EExplicitCredentialType.SASToken(), svm, CreateAzCopyTargetOptions{
					SASTokenOptions: GenericServiceSignatureValues{
						ContainerName: srcContainer.ContainerName(),
						Permissions:   (&blobsas.ContainerPermissions{Read: true, List: true}).String(),
					},
				}),
			},
			Flags: ListFlags{
				TrailingDot: to.Ptr(common.ETrailingDotOption.Disable()),
			},
		})

	ValidateListOutput(svm, stdout, expectedObjects, nil)
}

func (s *ListSuite) Scenario_EmptySASErrorCodes(svm *ScenarioVariationManager) {
	// Scale up from service to object
	// TODO: update this test once File OAuth PR is merged bc current output is "azure files requires a SAS token for authentication"
	srcObj := CreateResource[ObjectResourceManager](svm, GetRootResource(svm, ResolveVariation(svm, []common.Location{common.ELocation.Blob(), common.ELocation.BlobFS()})), ResourceDefinitionObject{})

	stdout, _ := RunAzCopy(
		svm,
		AzCopyCommand{
			Verb: AzCopyVerbList,
			Targets: []ResourceManager{
				AzCopyTarget{srcObj, EExplicitCredentialType.PublicAuth(), CreateAzCopyTargetOptions{}},
			},
			ShouldFail: true,
		})

	// Validate that the stdout contains these error URLs
	ValidateMessageOutput(svm, stdout, "https://aka.ms/AzCopyError/NoAuthenticationInformation", true)
}

func (s *ListSuite) Scenario_VirtualDirectoryHandling(svm *ScenarioVariationManager) {
	targetAcct := pointerTo(NamedResolveVariation(svm, map[string]string{
		"FNS": PrimaryStandardAcct,
		"HNS": PrimaryHNSAcct,
	}))

	// This should also fix copy/sync because the changed codepath overlaps, *but*, we'll have a separate test for that too.
	srcRoot := GetRootResource(svm, common.ELocation.Blob(), GetResourceOptions{
		PreferredAccount: targetAcct,
	})

	resourceMapping := NamedResolveVariation(svm, map[string]ObjectResourceMappingFlat{
		"DisallowOverlap": { // "foo" is  a folder, only a folder, there is no difference between "foo" and "foo/".
			"foo": ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{
					EntityType: common.EEntityType.Folder(),
				},
				Body: NewZeroObjectContentContainer(0),
			},
			"foo/bar": ResourceDefinitionObject{Body: NewZeroObjectContentContainer(1024)}, // File inside
			"baz":     ResourceDefinitionObject{Body: NewZeroObjectContentContainer(1024)}, // File on the side
		},
		"AllowOverlap": { // "foo" (the file), and "foo/" (the directory) can exist, but "foo/" is still a directory with metadata.
			"foo/": ResourceDefinitionObject{
				ObjectProperties: ObjectProperties{
					EntityType: common.EEntityType.Folder(),
				},
				Body: NewZeroObjectContentContainer(0),
			},
			"foo/bar": ResourceDefinitionObject{Body: NewZeroObjectContentContainer(1024)}, // File inside
			"foo":     ResourceDefinitionObject{Body: NewZeroObjectContentContainer(1024)}, // File on the side
		},
	})

	// HNS will automatically correct blob calls to "foo/" to "foo", which is correct behavior
	// But incompatible with the overlap scenario.
	if _, ok := resourceMapping["foo/"]; *targetAcct == PrimaryHNSAcct && ok {
		svm.InvalidateScenario()
		return
	}

	res := CreateResource[ContainerResourceManager](svm, srcRoot, ResourceDefinitionContainer{
		Objects: resourceMapping,
	})

	tgt := GetRootResource(svm, common.ELocation.BlobFS(), GetResourceOptions{
		PreferredAccount: targetAcct,
	}).(ServiceResourceManager).GetContainer(res.ContainerName())

	stdout, _ := RunAzCopy(
		svm,
		AzCopyCommand{
			Verb: AzCopyVerbList,
			Targets: []ResourceManager{
				tgt,
			},
			Flags: ListFlags{},
		},
	)

	expectedObjects := make(map[AzCopyOutputKey]cmd.AzCopyListObject)
	expectedObjects[AzCopyOutputKey{Path: "/"}] = cmd.AzCopyListObject{Path: "/", ContentLength: "0.00 B"}
	for k, v := range resourceMapping {
		// Correct for naming scheme if needed
		if v.EntityType == common.EEntityType.Folder() && !strings.HasSuffix(k, "/") {
			k += "/"
		}

		expectedObjects[AzCopyOutputKey{
			Path: k,
		}] = cmd.AzCopyListObject{
			Path:          k,
			ContentLength: SizeToString(v.Body.Size(), false),
		}
	}

	ValidateListOutput(svm, stdout, expectedObjects, nil) // No expected summary
}
