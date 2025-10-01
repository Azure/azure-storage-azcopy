package azcopy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	blobservice "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/ste"
)

const (
	PipingUploadParallelismDefault = 5
	PipingBlockSizeDefault         = 8 * 1024 * 1024
)

func (e *transferExecutor) redirectionTransfer(ctx context.Context) error {
	if e.opts.fromTo == common.EFromTo.PipeBlob() {
		return e.redirectionBlobUpload(ctx)
	} else if e.opts.fromTo == common.EFromTo.BlobPipe() {
		return e.redirectionBlobDownload(ctx)
	}
	return fmt.Errorf("unsupported redirection type: %s", e.opts.fromTo.String())
}

// redirectionBlobUpload uploads data from os.stdin to a blob destination using piping (redirection).
func (e *transferExecutor) redirectionBlobUpload(ctx context.Context) (err error) {
	// Use the concurrency environment value
	concurrencyEnvVar := common.GetEnvironmentVariable(common.EEnvironmentVariable.ConcurrencyValue())

	pipingUploadParallelism := PipingUploadParallelismDefault
	if concurrencyEnvVar != "" {
		// handle when the concurrency value is AUTO
		if concurrencyEnvVar == "AUTO" {
			return errors.New("concurrency auto-tuning is not possible when using redirection transfers (AZCOPY_CONCURRENCY_VALUE = AUTO)")
		}

		// convert the concurrency value to
		var concurrencyValue int64
		concurrencyValue, err = strconv.ParseInt(concurrencyEnvVar, 10, 32)

		//handle the error if the conversion fails
		if err != nil {
			return fmt.Errorf("AZCOPY_CONCURRENCY_VALUE is not set to a valid value, an integer is expected (current value: %s): %w", concurrencyEnvVar, err)
		}

		pipingUploadParallelism = int(concurrencyValue) // Cast to Integer
	}

	// if no block size is set, then use default value
	blockSize := e.opts.blockSize
	if blockSize == 0 {
		blockSize = PipingBlockSizeDefault
	}

	var resourceURL string
	resourceURL, err = e.opts.destination.String()
	if err != nil {
		return fmt.Errorf("failed to get resource string: %w", err)
	}

	var blobURLParts blob.URLParts
	blobURLParts, err = blob.ParseURL(resourceURL)
	if err != nil {
		return fmt.Errorf("fatal: cannot parse destination URL due to error: %s", err.Error())
	}

	// step 2: leverage high-level call in Blob SDK to upload stdin in parallel
	var serviceClient *blobservice.Client
	serviceClient, err = e.trp.dstServiceClient.BlobServiceClient()
	if err != nil {
		return err
	}
	blockBlobClient := serviceClient.NewContainerClient(blobURLParts.ContainerName).NewBlockBlobClient(blobURLParts.BlobName)

	metadataString := e.opts.metadata
	metadataMap := common.Metadata{}
	if len(metadataString) > 0 {
		for _, keyAndValue := range strings.Split(metadataString, ";") { // key/value pairs are separated by ';'
			kv := strings.Split(keyAndValue, "=") // key/value are separated by '='
			metadataMap[kv[0]] = &kv[1]
		}
	}
	cpkInfo, err := e.opts.cpkOptions.GetCPKInfo()
	if err != nil {
		return err
	}
	_, err = blockBlobClient.UploadStream(ctx, os.Stdin, &blockblob.UploadStreamOptions{
		BlockSize:   blockSize,
		Concurrency: pipingUploadParallelism,
		Metadata:    metadataMap,
		Tags:        e.opts.blobTags,
		HTTPHeaders: &blob.HTTPHeaders{
			BlobContentType:        common.IffNotEmpty(e.opts.contentType),
			BlobContentLanguage:    common.IffNotEmpty(e.opts.contentLanguage),
			BlobContentEncoding:    common.IffNotEmpty(e.opts.contentEncoding),
			BlobContentDisposition: common.IffNotEmpty(e.opts.contentDisposition),
			BlobCacheControl:       common.IffNotEmpty(e.opts.cacheControl),
		},
		AccessTier:   common.Iff(e.opts.blockBlobTier == common.EBlockBlobTier.None(), nil, to.Ptr(blob.AccessTier(e.opts.blockBlobTier.String()))),
		CPKInfo:      cpkInfo,
		CPKScopeInfo: e.opts.cpkOptions.GetCPKScopeInfo(),
	})

	return err
}

// redirectionBlobDownload downloads a blob from a source URL to os.Stdout using piping (redirection).
func (e *transferExecutor) redirectionBlobDownload(ctx context.Context) error {
	// step 0: check the Stdout before uploading
	_, err := os.Stdout.Stat()
	if err != nil {
		return fmt.Errorf("fatal: cannot write to Stdout due to error: %s", err.Error())
	}

	var resourceURL string
	resourceURL, err = e.opts.source.String()
	if err != nil {
		return fmt.Errorf("failed to get resource string: %w", err)
	}

	var blobURLParts blob.URLParts
	blobURLParts, err = blob.ParseURL(resourceURL)
	if err != nil {
		return fmt.Errorf("fatal: cannot parse destination URL due to error: %s", err.Error())
	}

	// step 2: leverage high-level call in Blob SDK to upload stdin in parallel
	var serviceClient *blobservice.Client
	serviceClient, err = e.trp.dstServiceClient.BlobServiceClient()
	if err != nil {
		return err
	}
	blobClient := serviceClient.NewContainerClient(blobURLParts.ContainerName).NewBlobClient(blobURLParts.BlobName)

	// step 3: start download

	cpkInfo, err := e.opts.cpkOptions.GetCPKInfo()
	if err != nil {
		return err
	}
	blobStream, err := blobClient.DownloadStream(ctx, &blob.DownloadStreamOptions{
		CPKInfo:      cpkInfo,
		CPKScopeInfo: e.opts.cpkOptions.GetCPKScopeInfo(),
	})
	if err != nil {
		return fmt.Errorf("fatal: cannot download blob due to error: %s", err.Error())
	}

	blobBody := blobStream.NewRetryReader(ctx, &blob.RetryReaderOptions{MaxRetries: ste.MaxRetryPerDownloadBody})
	defer blobBody.Close()

	// step 4: pipe everything into Stdout
	_, err = io.Copy(os.Stdout, blobBody)
	if err != nil {
		return fmt.Errorf("fatal: cannot download blob to Stdout due to error: %s", err.Error())
	}

	return nil
}
