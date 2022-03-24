package main

import (
	"context"
	"fmt"
	"math"
	"net/url"
	"strings"

	"github.com/Azure/azure-storage-azcopy/v10/cmd"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/ste"

	"github.com/Azure/azure-storage-blob-go/azblob"
)

func GetBlobProps(blobPath string) *azblob.BlobGetPropertiesResponse {
	rawURL, _ := url.Parse(blobPath)
	blobUrlParts := azblob.NewBlobURLParts(*rawURL)
	blobUrlParts.BlobName = strings.TrimSuffix(blobUrlParts.BlobName, "/")

	// perform the check
	blobURL := azblob.NewBlobURL(blobUrlParts.URL(), azblob.NewPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{}))
	props, _ := blobURL.GetProperties(context.TODO(), azblob.BlobAccessConditions{}, azblob.ClientProvidedKeyOptions{})
	return props
}

func NewOrder(jobID common.JobID) common.CopyJobPartOrderRequest {
	src := "https://storenakulkar.blob.core.windows.net/container1/ChangeLog.md?sp=racwdyt&st=2021-08-31T06:22:55Z&se=2021-08-31T14:22:55Z&spr=https&sv=2020-08-04&sr=b&sig=LqlmqmxGqGGe5K7mOj9IQCACd%2F%2BlT6Mun3Bx8HSydYA%3D"
	dst := "C:\\Users\\nakulkar\\ste1\\ste_test\\"

	srcResource, _ := cmd.SplitResourceString(src, common.ELocation.Blob())
	dstResource, _ := cmd.SplitResourceString(dst, common.ELocation.Local())

	props := GetBlobProps(src)
	t := common.CopyTransfer{
		Source:             "",
		Destination:        "/Changelog.md",
		EntityType:         common.EEntityType.File(),
		LastModifiedTime:   props.LastModified(),
		SourceSize:         props.ContentLength(),
		ContentType:        props.ContentType(),
		ContentEncoding:    props.ContentEncoding(),
		ContentDisposition: props.ContentDisposition(),
		ContentLanguage:    props.ContentLanguage(),
		CacheControl:       props.CacheControl(),
		ContentMD5:         props.ContentMD5(),
		Metadata:           nil,
		BlobType:           props.BlobType(),
		BlobTags:           nil,
	}
	jpo := common.CopyJobPartOrderRequest {
		JobID:           jobID,
		PartNum:         0,
		FromTo:          common.EFromTo.BlobLocal(),
		ForceWrite:      common.EOverwriteOption.True(),
		ForceIfReadOnly: false,
		AutoDecompress:  false,
		Priority:        common.EJobPriority.Normal(),
		LogLevel:        common.ELogLevel.Debug(),
		ExcludeBlobType: nil,
		BlobAttributes: common.BlobTransferAttributes{
			BlobType:                 common.EBlobType.BlockBlob(),
			BlockSizeInBytes:         4 * 1024 * 1024,
		},
		CommandString:  "NONE",
		DestinationRoot: dstResource,
		SourceRoot: srcResource,
		Fpo: common.EFolderPropertiesOption.NoFolders(),
		IsFinalPart: true,
		//Transfers: {t},
	}
	jpo.Transfers.List = append(jpo.Transfers.List, t)

	return jpo
}

func main1() {
	jobID := common.NewJobID()
	appCtx := context.TODO()
	cpuMon := common.NewNullCpuMonitor()
	level := common.ELogLevel.Debug()
	
	pacer := ste.NewNullAutoPacer()
	concurrencySettings := ste.NewConcurrencySettings(math.MaxInt32, false)
	tuner := ste.NullConcurrencyTuner{FixedValue: 128}
	slicePool := common.NewMultiSizeSlicePool(4 * 1024 * 1024 * 1024) //4GB
	cacheLimiter := common.NewCacheLimiter(4 * 1024 * 1024)
	fileCountLimiter := common.NewCacheLimiter(int64(64))
	logger := common.NewJobLogger(jobID, common.ELogLevel.Debug(), "C:\\Users\\nakulkar\\ste1" , "")
	logger.OpenLog()
	common.AzcopyJobPlanFolder = "C:\\Users\\nakulkar\\ste1"

	//======================================================================================================
	 jm := ste.NewJobMgr(concurrencySettings, jobID, appCtx, cpuMon, level, "NOCMD", "C:\\Users\\nakulkar\\ste1\\", &tuner, pacer, slicePool, cacheLimiter, fileCountLimiter, logger, false)
	//go statusMgr()

	 order := NewOrder(jobID) 
	 jppfn := ste.JobPartPlanFileName(fmt.Sprintf(ste.JobPartPlanFileNameFormat, jobID.String(), 0, ste.DataSchemaVersion))	 
	 jppfn.Create(order)                                                                  // Convert the order to a plan file

	waitToComplete := make(chan struct{})
	jm.AddJobPart(order.PartNum, jppfn, nil, order.SourceRoot.SAS, order.DestinationRoot.SAS, true, waitToComplete) 

	// Update jobPart Status with the status Manager
	jm.SendJobPartCreatedMsg(ste.JobPartCreatedMsg{TotalTransfers: uint32(len(order.Transfers.List)),
		IsFinalPart:          true,
		TotalBytesEnumerated: order.Transfers.TotalSizeInBytes,
		FileTransfers:        order.Transfers.FileTransferCount,
		FolderTransfer:       order.Transfers.FolderTransferCount})

	<-waitToComplete
}