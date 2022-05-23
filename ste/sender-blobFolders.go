package ste

import (
	"fmt"
	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"net/url"
	"strings"
	"time"
)

type blobFolderSender struct {
	destination     azblob.BlockBlobURL // We'll treat all folders as block blobs
	jptm            IJobPartTransferMgr
	sip             ISourceInfoProvider
	metadataToApply azblob.Metadata
	headersToAppply azblob.BlobHTTPHeaders
	blobTagsToApply azblob.BlobTagsMap
	cpkToApply      azblob.ClientProvidedKeyOptions
}

func newBlobFolderSender(jptm IJobPartTransferMgr, destination string, p pipeline.Pipeline, pacer pacer, sip ISourceInfoProvider) (sender, error) {
	destURL, err := url.Parse(destination)
	if err != nil {
		return nil, err
	}

	destBlockBlobURL := azblob.NewBlockBlobURL(*destURL, p)

	props, err := sip.Properties()
	if err != nil {
		return nil, err
	}

	var out sender
	fsend := blobFolderSender{
		jptm:            jptm,
		sip:             sip,
		destination:     destBlockBlobURL,
		metadataToApply: props.SrcMetadata.Clone().ToAzBlobMetadata(), // We're going to modify it, so we should clone it.
		headersToAppply: props.SrcHTTPHeaders.ToAzBlobHTTPHeaders(),
		blobTagsToApply: props.SrcBlobTags.ToAzBlobTagsMap(),
		cpkToApply:      common.ToClientProvidedKeyOptions(jptm.CpkInfo(), jptm.CpkScopeInfo()),
	}
	fromTo := jptm.FromTo()
	if fromTo.IsUpload() {
		out = &dummyUploader{fsend}
	} else {
		out = &dummys2sCopier{fsend}
	}

	return out, nil
}

func (b *blobFolderSender) EnsureFolderExists() error {
	t := b.jptm.GetFolderCreationTracker()

	_, err := b.destination.GetProperties(b.jptm.Context(), azblob.BlobAccessConditions{}, b.cpkToApply)
	if err != nil {
		if stgErr, ok := err.(azblob.StorageError); !(ok && stgErr.ServiceCode() == azblob.ServiceCodeBlobNotFound) {
			return fmt.Errorf("when checking if blob exists: %w", err)
		}
	} else {
		/*
			There's a low likelihood of a blob ending in a / being anything but a folder, but customers can do questionable
			things with their own time and money. So, we should safeguard against that. Rather than simply writing to the
			destination blob a set of properties, we should be responsible and check if overwriting is intended.

			If so, we should delete the old blob, and create a new one in it's place with all of our fancy new properties.
		*/
		if t.ShouldSetProperties(b.DirUrlToString(), b.jptm.GetOverwriteOption(), b.jptm.GetOverwritePrompter()) {
			_, err := b.destination.Delete(b.jptm.Context(), azblob.DeleteSnapshotsOptionNone, azblob.BlobAccessConditions{})
			if err != nil {
				return fmt.Errorf("when deleting existing blob: %w", err)
			}
		} else {
			/*
				We don't want to prompt the user again, and we're not going to write properties. So, we should kill the
				transfer where it stands and prevent the process from going further.
				This will be caught by ShouldSetProperties in the folder property tracker.
			*/
			return folderPropertiesNotOverwroteInCreation{}
		}
	}

	b.metadataToApply["hdi_isfolder"] = "true" // Set folder metadata flag
	err = b.getExtraProperties()
	if err != nil {
		return fmt.Errorf("when getting additional folder properties: %w", err)
	}

	_, err = b.destination.Upload(b.jptm.Context(),
		strings.NewReader(""),
		b.headersToAppply,
		b.metadataToApply,
		azblob.BlobAccessConditions{},
		azblob.DefaultAccessTier, // It doesn't make sense to use a special access tier, the blob will be 0 bytes.
		b.blobTagsToApply,
		b.cpkToApply)
	if err != nil {
		return fmt.Errorf("when creating folder: %w", err)
	}

	t.RecordCreation(b.DirUrlToString())

	return folderPropertiesSetInCreation{}
}

func (b *blobFolderSender) SetFolderProperties() error {
	return nil // unnecessary, all properties were set on creation.
}

func (b *blobFolderSender) DirUrlToString() string {
	url := b.destination.URL()
	url.RawQuery = ""
	return url.String()
}

// ===== Implement sender so that it can be returned in newBlobUploader. =====
/*
	It's OK to just panic all of these out, as they will never get called in a folder transfer.
*/

func (b *blobFolderSender) ChunkSize() int64 {
	panic("this sender only sends folders.")
}

func (b *blobFolderSender) NumChunks() uint32 {
	panic("this sender only sends folders.")
}

func (b *blobFolderSender) RemoteFileExists() (bool, time.Time, error) {
	panic("this sender only sends folders.")
}

func (b *blobFolderSender) Prologue(state common.PrologueState) (destinationModified bool) {
	panic("this sender only sends folders.")
}

func (b *blobFolderSender) Epilogue() {
	panic("this sender only sends folders.")
}

func (b *blobFolderSender) Cleanup() {
	panic("this sender only sends folders.")
}

func (b *blobFolderSender) GetDestinationLength() (int64, error) {
	panic("this sender only sends folders.")
}

// implement uploader to handle commonSenderCompletion

type dummyUploader struct {
	blobFolderSender
}

func (d dummyUploader) GenerateUploadFunc(chunkID common.ChunkID, blockIndex int32, reader common.SingleChunkReader, chunkIsWholeFile bool) chunkFunc {
	panic("this sender only sends folders.")
}

func (d dummyUploader) Md5Channel() chan<- []byte {
	panic("this sender only sends folders.")
}

// ditto for s2sCopier

type dummys2sCopier struct {
	blobFolderSender
}

func (d dummys2sCopier) GenerateCopyFunc(chunkID common.ChunkID, blockIndex int32, adjustedChunkSize int64, chunkIsWholeFile bool) chunkFunc {
	// TODO implement me
	panic("implement me")
}
