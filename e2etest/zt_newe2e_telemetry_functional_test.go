package e2etest

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/google/uuid"
)

func init() {
	suiteManager.RegisterSuite(&TelemetryFunctionalSuite{})
}

type TelemetryFunctionalSuite struct{}

func telemetryUnreachableIngestionCommand(source, destination ResourceManager) AzCopyCommand {
	return AzCopyCommand{
		Verb: AzCopyVerbCopy, Targets: []ResourceManager{source, destination}, Timeout: 3 * time.Minute,
		Environment: &AzCopyEnvironment{
			DisableTelemetry: pointerTo(false), NoProxy: pointerTo("192.0.2.1"),
			TelemetryConnectionString: pointerTo("InstrumentationKey=11111111-2222-3333-4444-555555555555;IngestionEndpoint=http://192.0.2.1"),
		},
		Flags:     CopyFlags{CopySyncCommonFlags: CopySyncCommonFlags{GlobalFlags: GlobalFlags{CapMbps: pointerTo(1.0)}, BlockSizeMB: pointerTo(0.0625)}},
		Telemetry: &telemetryExpectation{NoEvents: true, DeliveryFailureExpected: true},
	}
}

func (*TelemetryFunctionalSuite) Scenario_UnreachableIngestion(svm *ScenarioVariationManager) {
	if svm.Dryrun() {
		return
	}
	requireFunctionalTelemetry(svm)
	body := NewRandomObjectContentContainer(SizeFromString("2M"))
	source := CreateResource[ObjectResourceManager](svm, GetRootResource(svm, common.ELocation.Blob()), ResourceDefinitionObject{ObjectName: pointerTo("payload.bin"), Body: body})
	destination := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Local()), ResourceDefinitionContainer{}).GetObject(svm, "download.bin", common.EEntityType.File())
	command := telemetryUnreachableIngestionCommand(source, destination)
	capture := newAzCopyJobIDCapture(&AzCopyRawStdout{})
	command.Stdout = capture
	RunAzCopy(svm, command)
	requireFunctionalSummary(svm, capture, common.EJobStatus.Completed(), 1, uint64(SizeFromString("2M")))
	ValidateResource[ObjectResourceManager](svm, destination, ResourceDefinitionObject{Body: body}, ValidateResourceOptions{validateObjectContent: true})
}

func requireFunctionalTelemetry(svm *ScenarioVariationManager) {
	svm.AssertNow("functional E2E requires the existing Application Insights configuration", Equal{}, AppInsightsTelemetryValidationEnabled(), true)
}

func requireFunctionalSummary(svm *ScenarioVariationManager, capture *azCopyJobIDCapture, status common.JobStatus, objects uint32, size uint64) common.ListJobSummaryResponse {
	summary := capture.FinalSummary()
	svm.AssertNow("CLI must produce a final job summary", Not{IsNil{}}, summary)
	svm.Assert("job status", Equal{}, summary.JobStatus, status)
	svm.Assert("completed files", Equal{}, summary.TransfersCompleted-summary.FoldersCompleted, objects)
	svm.Assert("failed transfers", Equal{}, summary.TransfersFailed, uint32(0))
	svm.Assert("transferred bytes", Equal{}, summary.TotalBytesTransferred, size)
	return *summary
}

func (*TelemetryFunctionalSuite) Scenario_CommandExclusions(svm *ScenarioVariationManager) {
	name := ResolveVariation(svm, []string{"help", "copy-help", "env", "bash", "zsh", "fish", "powershell", "doc"})
	if svm.Dryrun() {
		return
	}
	requireFunctionalTelemetry(svm)
	root := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Local()), ResourceDefinitionContainer{}).URI(GetURIOptions{})
	environment := &AzCopyEnvironment{Home: &root, UserProfile: &root}
	flags := struct {
		GlobalFlags
		Help *bool `flag:"help"`
	}{}
	verb := AzCopyVerbJobsList
	expectation := &telemetryExpectation{NoEvents: true}
	switch name {
	case "help":
		verb = "help"
	case "copy-help":
		verb = AzCopyVerbCopy
		flags.Help = pointerTo(true)
	case "env":
		verb = "env"
	case "doc":
		verb = "doc"
	case "bash", "zsh", "fish", "powershell":
		verb = AzCopyVerb("completion " + name)
	}
	RunAzCopy(svm, AzCopyCommand{Verb: verb, Environment: environment, Flags: flags, Stdout: &AzCopyRawStdout{}, Timeout: time.Minute, WorkingDirectory: root, Telemetry: expectation})
}

func (*TelemetryFunctionalSuite) Scenario_TransferOptOut(svm *ScenarioVariationManager) {
	verb := ResolveVariation(svm, []AzCopyVerb{AzCopyVerbCopy, AzCopyVerbSync})
	mode := ResolveVariation(svm, []string{"cli-optout", "env-optout"})
	if svm.Dryrun() {
		return
	}
	requireFunctionalTelemetry(svm)
	body := NewRandomObjectContentContainer(SizeFromString("1K"))
	source := CreateResource[ObjectResourceManager](svm, GetRootResource(svm, common.ELocation.Blob()), ResourceDefinitionObject{ObjectName: pointerTo("payload.bin"), Body: body})
	destination := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Local()), ResourceDefinitionContainer{}).GetObject(svm, "download.bin", common.EEntityType.File())
	flags := CopyFlags{}
	environment := &AzCopyEnvironment{}
	if mode == "cli-optout" {
		flags.DisableTelemetry = pointerTo(true)
	}
	if mode == "env-optout" {
		environment.DisableTelemetry = pointerTo(true)
	}
	RunAzCopy(svm, AzCopyCommand{Verb: verb, Targets: []ResourceManager{source, destination}, Flags: flags, Environment: environment, Timeout: 3 * time.Minute, Telemetry: &telemetryExpectation{NoEvents: true}})
	ValidateResource[ObjectResourceManager](svm, destination, ResourceDefinitionObject{Body: body}, ValidateResourceOptions{validateObjectContent: true})
}

type telemetryBenchmarkFlags struct {
	GlobalFlags
	Mode      *string `flag:"mode"`
	FileCount *uint32 `flag:"file-count"`
	FileSize  *string `flag:"size-per-file"`
	Folders   *uint32 `flag:"number-of-folders"`
	Cleanup   *bool   `flag:"delete-test-data"`
}

func (*TelemetryFunctionalSuite) Scenario_Benchmark(svm *ScenarioVariationManager) {
	mode := ResolveVariation(svm, []string{"download", "upload"})
	if svm.Dryrun() {
		return
	}
	requireFunctionalTelemetry(svm)
	storage := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Blob()), ResourceDefinitionContainer{})
	var target ResourceManager = storage
	if mode == "download" {
		target = CreateResource[ObjectResourceManager](svm, storage, ResourceDefinitionObject{ObjectName: pointerTo("payload.bin"), Body: NewRandomObjectContentContainer(SizeFromString("32K"))})
	}
	stdout := newAzCopyJobIDCapture(&AzCopyRawStdout{})
	stdout.firstJobOnly = true
	RunAzCopy(svm, AzCopyCommand{
		Verb: AzCopyVerbBenchmark, Targets: []ResourceManager{target}, Stdout: stdout, Timeout: 3 * time.Minute,
		Flags:     telemetryBenchmarkFlags{Mode: &mode, FileCount: pointerTo(uint32(2)), FileSize: pointerTo("1K"), Folders: pointerTo(uint32(1)), Cleanup: pointerTo(true)},
		Telemetry: &telemetryExpectation{Properties: map[string]string{"BenchmarkMode": mode, "BenchmarkFileCount": "2", "BenchmarkFileSizeBytes": "1024", "BenchmarkFolderCount": "1", "BenchmarkIsCleanup": "false", "BenchmarkCleanupRequested": map[bool]string{true: "true", false: "false"}[mode == "upload"]}},
	})
	summaries := stdout.Summaries()
	for _, summary := range summaries {
		svm.Assert("benchmark job completed", Equal{}, summary.JobStatus, common.EJobStatus.Completed())
		svm.Assert("benchmark no failures", Equal{}, summary.TransfersFailed, uint32(0))
	}
	if mode == "upload" {
		svm.AssertNow("benchmark and cleanup summaries", Equal{}, len(summaries), 2)
		svm.Assert("uploaded benchmark bytes", Equal{}, summaries[0].TotalBytesTransferred, uint64(2048))
		svm.Assert("uploaded two benchmark files", Equal{}, summaries[0].TransfersCompleted, uint32(2))
		svm.Assert("cleanup deleted two files", Equal{}, summaries[1].TransfersCompleted, uint32(2))
		svm.Assert("no blobs remain after cleanup", Equal{}, len(storage.ListObjects(svm, "", true)), 0)
	} else {
		svm.AssertNow("one download summary", Equal{}, len(summaries), 1)
		svm.Assert("downloaded benchmark bytes", Equal{}, summaries[0].TotalBytesTransferred, uint64(SizeFromString("32K")))
	}
}

func (*TelemetryFunctionalSuite) Scenario_ConcurrentIdentity(svm *ScenarioVariationManager) {
	if svm.Dryrun() {
		return
	}
	requireFunctionalTelemetry(svm)
	body := NewRandomObjectContentContainer(SizeFromString("32K"))
	source := CreateResource[ObjectResourceManager](svm, GetRootResource(svm, common.ELocation.Blob()), ResourceDefinitionObject{ObjectName: pointerTo("payload.bin"), Body: body})
	home := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Local()), ResourceDefinitionContainer{}).URI(GetURIOptions{})
	expectedIdentity := strings.ReplaceAll(uuid.NewString(), "-", "")
	identityDirectory := filepath.Join(home, ".azcopy")
	svm.NoError("create shared identity directory", os.MkdirAll(identityDirectory, 0700), true)
	svm.NoError("seed shared installation identity", os.WriteFile(filepath.Join(identityDirectory, "installation_id"), []byte(expectedIdentity), 0600), true)
	environmentContext := FetchAzCopyEnvironmentContext(svm)
	environmentContext.SetupCleanup(svm)
	group := uuid.NewString()
	var commands []AzCopyCommand
	var destinations []ObjectResourceManager
	var captures []*azCopyJobIDCapture
	for range 4 {
		destination := CreateResource[ContainerResourceManager](svm, GetRootResource(svm, common.ELocation.Local()), ResourceDefinitionContainer{}).GetObject(svm, "download.bin", common.EEntityType.File())
		capture := newAzCopyJobIDCapture(&AzCopyRawStdout{})
		destinations = append(destinations, destination)
		captures = append(captures, capture)
		commands = append(commands, AzCopyCommand{
			Verb: AzCopyVerbCopy, Targets: []ResourceManager{source, destination}, Stdout: capture, Timeout: 3 * time.Minute,
			Environment: &AzCopyEnvironment{Home: &home, UserProfile: &home},
			Telemetry:   &telemetryExpectation{InstallationGroup: group, Properties: map[string]string{"InstallationID": expectedIdentity}},
		})
	}
	var workers sync.WaitGroup
	for _, command := range commands {
		workers.Add(1)
		go func() { defer workers.Done(); RunAzCopy(svm, command) }()
	}
	workers.Wait()
	jobs := map[string]bool{}
	for index, destination := range destinations {
		summary := requireFunctionalSummary(svm, captures[index], common.EJobStatus.Completed(), 1, uint64(SizeFromString("32K")))
		jobs[summary.JobID.String()] = true
		ValidateResource[ObjectResourceManager](svm, destination, ResourceDefinitionObject{Body: body}, ValidateResourceOptions{validateObjectContent: true})
	}
	svm.Assert("unique jobs across four processes", Equal{}, len(jobs), 4)
	identity, err := os.ReadFile(filepath.Join(home, ".azcopy", "installation_id"))
	svm.NoError("persisted installation identity", err, true)
	svm.Assert("persisted installation ID unchanged", Equal{}, strings.TrimSpace(string(identity)), expectedIdentity)
}
