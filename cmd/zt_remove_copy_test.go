package cmd

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCopyBlobsWithDirectoryStubsS2S(t *testing.T) {
	a := assert.New(t)
	t.Skip("Enable after setting Account to non-HNS")
	bsc := getBlobServiceClient()
	vdirName := "vdir1/"

	// create container and dest container
	srcContainerClient, srcContainerName := createNewContainer(a, bsc)
	dstContainerClient, dstContainerName := createNewContainer(a, bsc)
	dstBlobName := "testcopyblobswithdirectorystubs" + generateBlobName()
	defer deleteContainer(a, srcContainerClient)
	defer deleteContainer(a, dstContainerClient)

	blobAndDirStubsList := scenarioHelper{}.generateCommonRemoteScenarioForWASB(a, srcContainerClient, vdirName)
	a.NotNil(srcContainerClient)
	a.NotZero(len(blobAndDirStubsList))

	// set up interceptor
	mockedRPC := interceptor{}
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawSrcBlobWithSAS := scenarioHelper{}.getRawBlobURLWithSAS(a, srcContainerName, vdirName)
	rawDstBlobWithSAS := scenarioHelper{}.getRawBlobURLWithSAS(a, dstContainerName, dstBlobName)
	raw := getDefaultCopyRawInput(rawSrcBlobWithSAS.String(), rawDstBlobWithSAS.String())
	raw.recursive = true
	raw.includeDirectoryStubs = true

	runCopyAndVerify(a, raw, mockedRPC.intercept, func(err error) {
		a.Nil(err)

		// validate that the right number of transfers were scheduled
		a.Equal(len(blobAndDirStubsList), len(mockedRPC.transfers))

		// validate that the right transfers were sent
		expectedTransfers := scenarioHelper{}.shaveOffPrefix(blobAndDirStubsList, strings.TrimSuffix(vdirName, "/"))
		validateCopyTransfersAreScheduled(a, true, true, vdirName, "/vdir1", expectedTransfers, mockedRPC)
	})
}
