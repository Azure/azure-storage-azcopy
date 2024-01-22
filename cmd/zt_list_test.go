package cmd

import (
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
)

func TestListVersions(t *testing.T) {
	a := assert.New(t)
	bsc := getSecondaryBlobServiceClient()
	// set up the container with single blob with 2 versions
	containerClient, containerName := createNewContainer(a, bsc)
	defer deleteContainer(a, containerClient)

	blobsToInclude := []string{"AzURE2021.jpeg", "sub1/dir2/HELLO-4.txt", "sub1/test/testing.txt"}
	scenarioHelper{}.generateBlobsFromList(a, containerClient, blobsToInclude, blockBlobDefaultData)
	a.NotNil(containerClient)

	// get dictionary/map of blob: version id
	versions := make(map[string]string)
	for _, blob := range blobsToInclude {
		props, err := containerClient.NewBlockBlobClient(blob).GetProperties(ctx, nil)
		a.NoError(err)

		versions[blob] = *props.VersionID
	}

	// confirm that container has 3 blobs
	pager := containerClient.NewListBlobsFlatPager(nil)
	list, err := pager.NextPage(ctx)
	a.NoError(err)
	a.NotNil(list.Segment.BlobItems)
	a.Equal(3, len(list.Segment.BlobItems))

	// set up interceptor
	mockedRPC := interceptor{}
	Rpc = mockedRPC.intercept
	mockedRPC.init()

	mockedLcm := mockedLifecycleManager{infoLog: make(chan string, 50)}
	mockedLcm.SetOutputFormat(common.EOutputFormat.Text()) // text format
	glcm = &mockedLcm

	// construct the raw input to simulate user input
	rawContainerURLWithSAS := scenarioHelper{}.getSecondaryRawContainerURLWithSAS(a, containerName)
	raw := getDefaultListRawInput(rawContainerURLWithSAS.String())
	raw.Properties = "VersionId"

	runListAndVerify(a, raw, func(err error) {
		a.Nil(err)

		// validate that the no transfers were scheduled
		a.Nil(mockedRPC.transfers)

		// check if info logs contain the correct version id for each blob
		msg := mockedLcm.GatherAllLogs(mockedLcm.infoLog)
		for i, m := range msg {
			a.True(strings.Contains(m, blobsToInclude[i]))
			a.True(strings.Contains(m, "VersionId: "+versions[blobsToInclude[i]]))
		}
	})

}
