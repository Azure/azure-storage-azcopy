package e2etest

import (
	"bytes"
	"fmt"
	"github.com/Azure/azure-sdk-for-go/sdk/data/aztables"
	cmd2 "github.com/Azure/azure-storage-azcopy/v10/cmd"
	"os"
	"os/exec"
	"strconv"
	"strings"
)

const (
	MemoryProfileTable     = "memprofstats"
	MemoryProfileContainer = "memprofdata"
)

func UploadMemoryProfile(a Asserter, profilePath string, runCount uint) {
	// We don't need telemetry configured to dump details on peak memory usage, and we need to grab these anyway.
	cmd := exec.Command("go", "tool", "pprof", "-top", "-unit=bytes", profilePath)

	stdout := &bytes.Buffer{}
	cmd.Stdout = stdout
	cmd.Stderr = &bytes.Buffer{}

	err := cmd.Run()
	if err != nil {
		a.NoError("pprof run failed: "+cmd.Stderr.(*bytes.Buffer).String(), err)
		return
	}

	// lazy, will inevitably snap like a twig if golang coughs. Unfortunately, you can't get json output. so that's not useful.
	outStr := stdout.String()
	outStr = outStr[strings.Index(outStr, "Showing nodes accounting for "):] // Locate the line we're really interested in
	outStr = outStr[:strings.IndexRune(outStr, '\n')]                        // Trim down to the line
	outStr = outStr[strings.Index(outStr, " of ")+4:]                        // grab the last total
	outStr = outStr[:strings.Index(outStr, "B total")]                       // trim the excess, parse.
	totalBytes, err := strconv.ParseInt(outStr, 10, 64)
	if err != nil {
		a.NoError("failed to parse "+outStr+" as an integer, original: "+stdout.String(), err)
		return
	}

	// Output the data so that if the upload fails or the acct isn't configured, we're good to go.
	a.Log("Test run sampled a heap size of %s", cmd2.ByteSizeToString(totalBytes))
	if !GlobalConfig.TelemetryConfigured() {
		return
	}
	a.Log("Uploading memory profile!")

	// Fetch our run count for table key
	tableService, err := GlobalConfig.GetTelemetryTableService()
	if err != nil {
		a.NoError("failed to get telemetry table service", err)
		return
	}
	blobService, err := GlobalConfig.GetTelemetryBlobService()
	if err != nil {
		a.NoError("failed to get telemetry blob service", err)
		return
	}

	table := tableService.NewClient(MemoryProfileTable)
	container := blobService.NewContainerClient(MemoryProfileContainer)
	partitionKey := a.GetTestName() + "/" + fmt.Sprintf("%05d", runCount) // We'll index the table by test, then select rows by the data key, doing the same on the blob front.

	blobProfilePath := fmt.Sprintf(
		"%s/%s",
		partitionKey,
		GlobalConfig.TelemetryConfig.DataKey,
	)

	buf, err := aztables.EDMEntity{
		Entity: aztables.Entity{
			PartitionKey: strings.ReplaceAll(partitionKey, "/", "-"),
			RowKey:       GlobalConfig.TelemetryConfig.DataKey,
		},
		Properties: map[string]any{
			"runDate":         "",
			"bytesSampled":    totalBytes,
			"blobProfilePath": blobProfilePath,
		},
	}.MarshalJSON()
	a.NoError("serialize table entity", err)
	if err == nil {
		_, err = table.UpsertEntity(ctx, buf, nil)
		a.NoError("upload table entity", err)
	}

	f, err := os.Open(profilePath)
	if err != nil {
		a.NoError("failed to open "+profilePath+" for reading", err)
		return
	}
	defer func() {
		_ = f.Close()
	}()

	bbClient := container.NewBlockBlobClient(blobProfilePath)
	_, err = bbClient.UploadFile(ctx, f, nil)
	a.NoError("uploading memory profile", err)
}
