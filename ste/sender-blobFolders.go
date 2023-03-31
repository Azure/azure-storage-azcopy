package ste

import (
	"fmt"
	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-storage-azcopy/v10/azbfs"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"net/url"
	"strings"
	"time"
)

type blobFolderSender struct {
	destClient      *blockblob.Client // We'll treat all folders as block blobs
	destination     azblob.BlockBlobURL
	jptm            IJobPartTransferMgr
	sip             ISourceInfoProvider
	metadataToApply azblob.Metadata
	headersToAppply blob.HTTPHeaders
	blobTagsToApply azblob.BlobTagsMap
	cpkToApply      azblob.ClientProvidedKeyOptions
}

func newBlobFolderSender(jptm IJobPartTransferMgr, destination string, p pipeline.Pipeline, pacer pacer, sip ISourceInfoProvider) (sender, error) {
	destURL, err := url.Parse(destination)
	if err != nil {
		return nil, err
	}

	destBlockBlobURL := azblob.NewBlockBlobURL(*destURL, p)
	destClient, err := common.CreateBlockBlobClient(destination, jptm.CredentialInfo(), jptm.CredentialOpOptions(), jptm.ClientOptions())
	if err != nil {
		return nil, err
	}

	props, err := sip.Properties()
	if err != nil {
		return nil, err
	}

	var out sender
	fsend := blobFolderSender{
		jptm:            jptm,
		sip:             sip,
		destClient:      destClient,
		destination:     destBlockBlobURL,
		metadataToApply: props.SrcMetadata.Clone().ToAzBlobMetadata(), // We're going to modify it, so we should clone it.
		headersToAppply: props.SrcHTTPHeaders.ToBlobHTTPHeaders(),
		blobTagsToApply: props.SrcBlobTags.ToAzBlobTagsMap(),
		cpkToApply:      common.ToClientProvidedKeyOptions(jptm.CpkInfo(), jptm.CpkScopeInfo()),
	}
	fromTo := jptm.FromTo()
	if fromTo.IsUpload() {
		out = &dummyFolderUploader{fsend}
	} else {
		out = &dummyFolderS2SCopier{fsend}
	}

	return out, nil
}

func (b *blobFolderSender) setDatalakeACLs() {
	bURLParts := azblob.NewBlobURLParts(b.destination.URL())
	bURLParts.BlobName = strings.TrimSuffix(bURLParts.BlobName, "/") // BlobFS does not like when we target a folder with the /
	bURLParts.Host = strings.ReplaceAll(bURLParts.Host, ".blob", ".dfs")
	// todo: jank, and violates the principle of interfaces
	fileURL := azbfs.NewFileURL(bURLParts.URL(), b.jptm.(*jobPartTransferMgr).jobPartMgr.(*jobPartMgr).secondaryPipeline)

	// We know for a fact our source is a "blob".
	acl, err := b.sip.(*blobSourceInfoProvider).AccessControl()
	if err != nil {
		b.jptm.FailActiveSend("Grabbing source ACLs", err)
	}
	acl.Permissions = "" // Since we're sending the full ACL, Permissions is irrelevant.
	_, err = fileURL.SetAccessControl(b.jptm.Context(), acl)
	if err != nil {
		b.jptm.FailActiveSend("Putting ACLs", err)
	}
}

func (b *blobFolderSender) overwriteDFSProperties() (string, error) {
	b.jptm.Log(pipeline.LogWarning, "It is impossible to completely overwrite a folder with existing content under it on a hierarchical namespace storage account. A best-effort attempt will be made, but if CPK does not match the transfer will fail.")

	err := b.getExtraProperties()
	if err != nil {
		return "Get Extra Properties", fmt.Errorf("when getting additional folder properties: %w", err)
	}

	// do not set folder flag as it's invalid to modify a folder with
	delete(b.metadataToApply, "hdi_isfolder")

	// SetMetadata can set CPK if it wasn't specified prior. This is not a "full" overwrite, but a best-effort overwrite.
	_, err = b.destination.SetMetadata(b.jptm.Context(), b.metadataToApply, azblob.BlobAccessConditions{}, b.cpkToApply)
	if err != nil {
		return "Set Metadata", fmt.Errorf("A best-effort overwrite was attempted; CPK errors cannot be handled when the blob cannot be deleted.\n%w", err)
	}
	_, err = b.destination.SetTags(b.jptm.Context(), nil, nil, nil, b.blobTagsToApply)
	if err != nil {
		return "Set Blob Tags", err
	}
	_, err = b.destination.SetHTTPHeaders(b.jptm.Context(), common.ToAzBlobHTTPHeaders(b.headersToAppply), azblob.BlobAccessConditions{})
	if err != nil {
		return "Set HTTP Headers", err
	}

	// Upload ADLS Gen 2 ACLs
	if b.jptm.FromTo() == common.EFromTo.BlobBlob() && b.jptm.Info().PreserveSMBPermissions.IsTruthy() {
		b.setDatalakeACLs()
	}

	return "", nil
}

func (b *blobFolderSender) SetContainerACL() error {
	bURLParts := azblob.NewBlobURLParts(b.destination.URL())
	bURLParts.BlobName = "/" // Container-level ACLs NEED a /
	bURLParts.Host = strings.ReplaceAll(bURLParts.Host, ".blob", ".dfs")
	// todo: jank, and violates the principle of interfaces
	fileURL := azbfs.NewFileSystemURL(bURLParts.URL(), b.jptm.(*jobPartTransferMgr).jobPartMgr.(*jobPartMgr).secondaryPipeline)

	// We know for a fact our source is a "blob".
	acl, err := b.sip.(*blobSourceInfoProvider).AccessControl()
	if err != nil {
		b.jptm.FailActiveSend("Grabbing source ACLs", err)
		return folderPropertiesSetInCreation{} // standard completion will detect failure
	}
	acl.Permissions = "" // Since we're sending the full ACL, Permissions is irrelevant.
	_, err = fileURL.SetAccessControl(b.jptm.Context(), acl)
	if err != nil {
		b.jptm.FailActiveSend("Putting ACLs", err)
		return folderPropertiesSetInCreation{} // standard completion will detect failure
	}

	return folderPropertiesSetInCreation{} // standard completion will handle the rest
}

func (b *blobFolderSender) EnsureFolderExists() error {
	t := b.jptm.GetFolderCreationTracker()

	parsedURL, err := blob.ParseURL(b.destination.String())
	if err != nil {
		return err
	}
	if parsedURL.BlobName == "" {
		return b.SetContainerACL() // Can't do much with a container, but it is here.
	}

	_, err = b.destClient.GetProperties(b.jptm.Context(), &blob.GetPropertiesOptions{CPKInfo: b.jptm.CpkInfo()})
	if err != nil {
		if !bloberror.HasCode(err, bloberror.BlobNotFound) {
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
				if stgErr, ok := err.(azblob.StorageError); ok {
					if stgErr.ServiceCode() == "DirectoryIsNotEmpty" { // this is DFS, and we cannot do a standard replacement on it. Opt to simply overwrite the properties.
						where, err := b.overwriteDFSProperties()
						if err != nil {
							return fmt.Errorf("%w. When %s", err, where)
						}

						return nil
					}
				}

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

	err = t.CreateFolder(b.DirUrlToString(), func() error {
		_, err := b.destination.Upload(b.jptm.Context(),
			strings.NewReader(""),
			common.ToAzBlobHTTPHeaders(b.headersToAppply),
			b.metadataToApply,
			azblob.BlobAccessConditions{},
			azblob.DefaultAccessTier, // It doesn't make sense to use a special access tier, the blob will be 0 bytes.
			b.blobTagsToApply,
			b.cpkToApply,
			azblob.ImmutabilityPolicyOptions{})

		return err
	})

	if err != nil {
		return fmt.Errorf("when creating folder: %w", err)
	}

	// Upload ADLS Gen 2 ACLs
	if b.jptm.FromTo() == common.EFromTo.BlobBlob() && b.jptm.Info().PreserveSMBPermissions.IsTruthy() {
		b.setDatalakeACLs()
	}

	return nil
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

type dummyFolderUploader struct {
	blobFolderSender
}

func (d dummyFolderUploader) GenerateUploadFunc(chunkID common.ChunkID, blockIndex int32, reader common.SingleChunkReader, chunkIsWholeFile bool) chunkFunc {
	panic("this sender only sends folders.")
}

func (d dummyFolderUploader) Md5Channel() chan<- []byte {
	panic("this sender only sends folders.")
}

// ditto for s2sCopier

type dummyFolderS2SCopier struct {
	blobFolderSender
}

func (d dummyFolderS2SCopier) GenerateCopyFunc(chunkID common.ChunkID, blockIndex int32, adjustedChunkSize int64, chunkIsWholeFile bool) chunkFunc {
	// TODO implement me
	panic("implement me")
}
