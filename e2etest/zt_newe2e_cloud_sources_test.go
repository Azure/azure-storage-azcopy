package e2etest

import (
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

func init() {
	suiteManager.RegisterSuite(&CloudSourceSuite{})
}

// CloudSourceSuite copies from Amazon S3 and Google Cloud Storage into Blob storage. Besides checking the copied data,
// each run registers what its telemetry must report about the source, which the suite checks at teardown when
// Application Insights validation is configured. Scenarios skip when the provider's credentials aren't set.
type CloudSourceSuite struct{}

// cloudSourceTelemetry is what telemetry reports for each third-party source: protocol, mount type and the credential
// AzCopy used.
var cloudSourceTelemetry = map[common.Location][3]string{
	common.ELocation.S3():  {"s3", "cloud-s3", common.ECredentialType.S3AccessKey().String()},
	common.ELocation.GCP(): {"gcs", "cloud-gcs", common.ECredentialType.GoogleAppCredentials().String()},
}

func (*CloudSourceSuite) Scenario_CopyToBlob(svm *ScenarioVariationManager) {
	source := ResolveVariation(svm, []common.Location{common.ELocation.S3(), common.ELocation.GCP()})
	scope := ResolveVariation(svm, []string{"bucket", "object"})
	if svm.Dryrun() {
		return
	}
	objects := map[string]ObjectContentContainer{
		"payload.bin":       NewRandomObjectContentContainer(SizeFromString("1K")),
		"folder/nested.bin": NewRandomObjectContentContainer(SizeFromString("2K")),
	}
	bucket := CreateCloudBucket(svm, source, objects)
	container := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Blob()), ResourceDefinitionContainer{})

	// A bucket copies its keys into the container as they are; an object copies to a blob of the same name.
	var from, to ResourceManager = bucket, container
	expected := ObjectResourceMappingFlat{}
	for key, body := range objects {
		expected[key] = ResourceDefinitionObject{Body: body}
	}
	sourceScope, destScope := "bucket", "container"
	if scope == "object" {
		from = bucket.Object("payload.bin")
		to = container.GetObject(svm, "payload.bin", common.EEntityType.File())
		expected = ObjectResourceMappingFlat{"payload.bin": expected["payload.bin"]}
		sourceScope, destScope = "object-or-prefix", "object-or-prefix"
	}

	reported := cloudSourceTelemetry[source]
	RunAzCopy(svm, AzCopyCommand{
		Verb:    AzCopyVerbCopy,
		Targets: []ResourceManager{from, to},
		Flags:   CopyFlags{CopySyncCommonFlags: CopySyncCommonFlags{Recursive: pointerTo(scope == "bucket")}},
		Timeout: 5 * time.Minute,
		Telemetry: &telemetryExpectation{
			// Bucket names and object keys must never reach telemetry.
			ForbiddenValues: []string{bucket.BucketName(), "payload.bin", "nested.bin"},
			// Storage account, endpoint kind and cloud type describe Azure endpoints only, so they are empty here.
			Properties: map[string]string{
				"SourceProtocol": reported[0], "SourceMountType": reported[1], "SourceAuthMechanism": reported[2],
				"SourceScope": sourceScope, "SourceStorageAccount": "", "SourceEndpointKind": "", "SourceCloudType": "",
				"DestProtocol": "https", "DestScope": destScope, "DestAuthMechanism": "SAS",
			},
		},
	})

	ValidateResource[ContainerResourceManager](svm, container, ResourceDefinitionContainer{Objects: expected}, ValidateResourceOptions{
		validateObjectContent: true,
	})
}
