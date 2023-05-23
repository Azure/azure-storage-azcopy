package cmd

import (
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
)

func TestCopyBlobsWithDirectoryStubsS2S(t *testing.T) {
	a := assert.New(t)
	t.Skip("Enable after setting Account to non-HNS")
	bsu := getBSU()
	vdirName := "vdir1/"

	// create container and dest container
	srcContainerURL, srcContainerName := createNewContainer(a, bsu)
	dstContainerURL, dstContainerName := createNewContainer(a, bsu)
	dstBlobName := "testcopyblobswithdirectorystubs" + generateBlobName()
	defer deleteContainer(a, srcContainerURL)
	defer deleteContainer(a, dstContainerURL)

	blobAndDirStubsList := scenarioHelper{}.generateCommonRemoteScenarioForWASB(a, srcContainerURL, vdirName)
	a.NotNil(srcContainerURL)
	a.NotZero(len(blobAndDirStubsList))

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawSrcBlobWithSAS := scenarioHelper{}.getRawBlobURLWithSAS(a, srcContainerName, vdirName)
	rawDstBlobWithSAS := scenarioHelper{}.getRawBlobURLWithSAS(a, dstContainerName, dstBlobName)
	raw := getDefaultCopyRawInput(rawSrcBlobWithSAS.String(), rawDstBlobWithSAS.String())
	raw.recursive = true
	raw.includeDirectoryStubs = true

	runCopyAndVerify(a, raw, func(err error) {
		a.Nil(err)

		// validate that the right number of transfers were scheduled
		a.Equal(len(blobAndDirStubsList), len(mockedRPC.transfers))

		// validate that the right transfers were sent
		expectedTransfers := scenarioHelper{}.shaveOffPrefix(blobAndDirStubsList, strings.TrimSuffix(vdirName, "/"))
		validateCopyTransfersAreScheduled(a, true, true, vdirName, "/vdir1", expectedTransfers, mockedRPC)
	})
}
