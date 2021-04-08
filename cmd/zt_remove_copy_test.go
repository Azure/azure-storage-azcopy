package cmd

import (
	chk "gopkg.in/check.v1"
	"strings"
)

func (s *cmdIntegrationSuite) TestCopyBlobsWithDirectoryStubsS2S(c *chk.C) {
	c.Skip("Enable after setting Account to non-HNS")
	bsu := getBSU()
	vdirName := "vdir1/"

	// create container and dest container
	srcContainerURL, srcContainerName := createNewContainer(c, bsu)
	dstContainerURL, dstContainerName := createNewContainer(c, bsu)
	dstBlobName := "testcopyblobswithdirectorystubs" + generateBlobName()
	defer deleteContainer(c, srcContainerURL)
	defer deleteContainer(c, dstContainerURL)

	blobAndDirStubsList := scenarioHelper{}.generateCommonRemoteScenarioForWASB(c, srcContainerURL, vdirName)
	c.Assert(srcContainerURL, chk.NotNil)
	c.Assert(len(blobAndDirStubsList), chk.Not(chk.Equals), 0)

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	// construct the raw input to simulate user input
	rawSrcBlobWithSAS := scenarioHelper{}.getRawBlobURLWithSAS(c, srcContainerName, vdirName)
	rawDstBlobWithSAS := scenarioHelper{}.getRawBlobURLWithSAS(c, dstContainerName, dstBlobName)
	raw := getDefaultCopyRawInput(rawSrcBlobWithSAS.String(), rawDstBlobWithSAS.String())
	raw.recursive = true
	raw.includeDirectoryStubs = true

	runCopyAndVerify(c, raw, func(err error) {
		c.Assert(err, chk.IsNil)

		// validate that the right number of transfers were scheduled
		c.Assert(len(mockedRPC.transfers), chk.Equals, len(blobAndDirStubsList))

		// validate that the right transfers were sent
		expectedTransfers := scenarioHelper{}.shaveOffPrefix(blobAndDirStubsList, strings.TrimSuffix(vdirName, "/"))
		validateCopyTransfersAreScheduled(c, true, true, vdirName, "/vdir1",expectedTransfers, mockedRPC)
	})
}
