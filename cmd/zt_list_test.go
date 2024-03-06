package cmd

import (
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/streaming"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
)

func TestListVersionIdWithNoAdditionalVersions(t *testing.T) {
	a := assert.New(t)
	bsc := getSecondaryBlobServiceClient()

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
	raw.RunningTally = true

	runListAndVerify(a, raw, func(err error) {
		a.Nil(err)

		// validate that the no transfers were scheduled
		a.Nil(mockedRPC.transfers)

		// check if info logs contain the correct version id for each blob
		msg := mockedLcm.GatherAllLogs(mockedLcm.infoLog)
		for i, m := range msg {
			if i < 3 { // 0-2 will be blob names + version id
				a.True(strings.Contains(m, blobsToInclude[i]))
				a.True(strings.Contains(m, "VersionId: "+versions[blobsToInclude[i]]))
			}
			if i == 4 { // 4 will be file count
				a.True(strings.Contains(m, "File count: 3"))
			}
			if i == 5 { // 5 will be file size
				a.True(strings.Contains(m, "Total file size: 69.00 B"))
			}
		}
	})

	// test json output
	mockedLcm = mockedLifecycleManager{infoLog: make(chan string, 50)}
	mockedLcm.SetOutputFormat(common.EOutputFormat.Json()) // json format
	glcm = &mockedLcm

	runListAndVerify(a, raw, func(err error) {
		a.Nil(err)

		// validate that the no transfers were scheduled
		a.Nil(mockedRPC.transfers)

		// check if info logs contain the correct version id for each blob
		msg := mockedLcm.GatherAllLogs(mockedLcm.infoLog)
		for i, m := range msg {
			if i < 3 { // 0-2 will be blob names + version id
				a.True(strings.Contains(m, blobsToInclude[i]))
				a.True(strings.Contains(m, "VersionId: "+versions[blobsToInclude[i]]))
			}
			if i == 4 { // 4 will be file count
				a.True(strings.Contains(m, "File count: 3"))
			}
			if i == 5 { // 5 will be file size
				a.True(strings.Contains(m, "Total file size: 69.00 B"))
			}
		}
	})

}

func TestListVersionsMultiVersions(t *testing.T) {
	a := assert.New(t)
	bsc := getSecondaryBlobServiceClient()

	containerClient, containerName := createNewContainer(a, bsc)
	defer deleteContainer(a, containerClient)

	// testing how running tally will handle foo.txt vs foo/foo.txt, test/foo.txt
	blobsToInclude := []string{"foo.txt", "foo/foo.txt", "test/foo.txt", "sub1/test/baz.txt"}
	scenarioHelper{}.generateBlobsFromList(a, containerClient, blobsToInclude, blockBlobDefaultData)
	a.NotNil(containerClient)

	// make first two blobs have 1 additional version
	blobsToVersion := []string{blobsToInclude[0], blobsToInclude[1]}
	randomStrings := []string{"random-1", "random-two"}
	scenarioHelper{}.generateVersionsForBlobsFromList(a, containerClient, blobsToVersion, randomStrings)
	a.NotNil(containerClient)

	// make first blob have 2 versions in total
	blobClient := containerClient.NewBlockBlobClient(blobsToInclude[0])
	uploadResp, err := blobClient.Upload(ctx, streaming.NopCloser(strings.NewReader("random-three-3")), nil)
	a.NoError(err)
	a.NotNil(uploadResp.VersionID)
	a.NotNil(containerClient)

	// confirm that container has 7 blobs (4 blobs, 3 versions)
	// foo.txt has two versions
	// foo/foo.txt has one version
	// test/foo.txt and sub1/test/baz.txt don't have any versions
	pager := containerClient.NewListBlobsFlatPager(&container.ListBlobsFlatOptions{
		Include: container.ListBlobsInclude{Versions: true},
	})
	list, err := pager.NextPage(ctx)
	a.NoError(err)
	a.NotNil(list.Segment.BlobItems)
	a.Equal(7, len(list.Segment.BlobItems))

	var blobs []string
	var versions []string
	for _, item := range list.Segment.BlobItems {
		blobs = append(blobs, *item.Name)
		versions = append(versions, *item.VersionID)
	}

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
	raw.RunningTally = true

	runListAndVerify(a, raw, func(err error) {
		a.Nil(err)

		// validate that the no transfers were scheduled
		a.Nil(mockedRPC.transfers)

		// check if info logs contain the correct version id for each blob
		msg := mockedLcm.GatherAllLogs(mockedLcm.infoLog)
		for i, m := range msg {
			if i < 7 { // 0-6 will be blob names + version id
				a.True(contains(blobs, m, true))
				a.True(contains(versions, m, false))
			}
			if i == 8 { // 8 will be file count
				a.True(strings.Contains(m, "File count: 4"))
			}
			if i == 9 { // 9 will be file size of latest versions (should be 70.00 B)
				a.True(strings.Contains(m, "Total file size: 70.00 B"))
			}
		}
	})

	// test json output
	mockedLcm = mockedLifecycleManager{infoLog: make(chan string, 50)}
	mockedLcm.SetOutputFormat(common.EOutputFormat.Json()) // json format
	glcm = &mockedLcm

	runListAndVerify(a, raw, func(err error) {
		a.Nil(err)

		// validate that the no transfers were scheduled
		a.Nil(mockedRPC.transfers)

		// check if info logs contain the correct version id for each blob
		msg := mockedLcm.GatherAllLogs(mockedLcm.infoLog)
		for i, m := range msg {
			if i < 7 { // 0-6 will be blob names + version id
				a.True(contains(blobs, m, true))
				a.True(contains(versions, m, false))
			}
			if i == 8 { // 8 will be file count
				a.True(strings.Contains(m, "File count: 4"))
			}
			if i == 9 { // 9 will be file size of latest versions (should be 70.00 B)
				a.True(strings.Contains(m, "Total file size: 70.00 B"))
			}
		}
	})

}

func TestListVersionsMultiVersionsNoPropFlag(t *testing.T) {
	a := assert.New(t)
	bsc := getSecondaryBlobServiceClient()

	containerClient, containerName := createNewContainer(a, bsc)
	defer deleteContainer(a, containerClient)

	// testing how running tally will handle foo.txt vs foo/foo.txt, test/foo.txt
	blobsToInclude := []string{"foo.txt", "foo/foo.txt", "test/foo.txt", "sub1/test/baz.txt"}
	scenarioHelper{}.generateBlobsFromList(a, containerClient, blobsToInclude, blockBlobDefaultData)
	a.NotNil(containerClient)

	// make first two blobs have 1 additional version
	blobsToVersion := []string{blobsToInclude[0], blobsToInclude[1]}
	randomStrings := []string{"random-1", "random-two"}
	scenarioHelper{}.generateVersionsForBlobsFromList(a, containerClient, blobsToVersion, randomStrings)
	a.NotNil(containerClient)

	// make first blob have 2 versions in total
	blobClient := containerClient.NewBlockBlobClient(blobsToInclude[0])
	uploadResp, err := blobClient.Upload(ctx, streaming.NopCloser(strings.NewReader("random-three-3")), nil)
	a.NoError(err)
	a.NotNil(uploadResp.VersionID)
	a.NotNil(containerClient)

	// confirm that container has 7 blobs (4 blobs, 3 versions)
	// foo.txt has two versions
	// foo/foo.txt has one version
	// test/foo.txt and sub1/test/baz.txt don't have any versions
	pager := containerClient.NewListBlobsFlatPager(&container.ListBlobsFlatOptions{
		Include: container.ListBlobsInclude{Versions: true},
	})
	list, err := pager.NextPage(ctx)
	a.NoError(err)
	a.NotNil(list.Segment.BlobItems)
	a.Equal(7, len(list.Segment.BlobItems))

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
	raw.RunningTally = true

	runListAndVerify(a, raw, func(err error) {
		a.Nil(err)

		// validate that the no transfers were scheduled
		a.Nil(mockedRPC.transfers)

		// check if info logs contain the correct version id for each blob
		msg := mockedLcm.GatherAllLogs(mockedLcm.infoLog)
		for i, m := range msg {
			if i < 4 { // 0-3 is blob
				a.True(contains(blobsToInclude, m, true))
			}
			if i == 5 { // 5 will be file count
				a.True(strings.Contains(m, "File count: 4"))
			}
			if i == 6 { // 6 will be file size (should be 70 B)
				a.True(strings.Contains(m, "Total file size: 70.00 B"))
			}
		}
	})

	// test json output
	mockedLcm = mockedLifecycleManager{infoLog: make(chan string, 50)}
	mockedLcm.SetOutputFormat(common.EOutputFormat.Json()) // json format
	glcm = &mockedLcm

	runListAndVerify(a, raw, func(err error) {
		a.Nil(err)

		// validate that the no transfers were scheduled
		a.Nil(mockedRPC.transfers)

		// check if info logs contain the correct version id for each blob
		msg := mockedLcm.GatherAllLogs(mockedLcm.infoLog)
		for i, m := range msg {
			if i < 4 { // 0-3 is blob
				a.True(contains(blobsToInclude, m, true))
			}
			if i == 5 { // 5 will be file count
				a.True(strings.Contains(m, "File count: 4"))
			}
			if i == 6 { // 6 will be file size (should be 70 B)
				a.True(strings.Contains(m, "Total file size: 70.00 B"))
			}
		}
	})
}

func contains(arr []string, msg string, isBlob bool) bool {
	for _, a := range arr {
		if isBlob {
			if strings.HasPrefix(msg, a) {
				return true
			}
		} else {
			if strings.Contains(msg, a) {
				return true
			}
		}
	}
	return false
}
