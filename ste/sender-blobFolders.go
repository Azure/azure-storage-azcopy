package ste

import (
	"bytes"
	"fmt"
	"net/url"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/streaming"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake/file"
	"github.com/Azure/azure-storage-azcopy/v10/common"
)

type blobFolderSender struct {
	destinationClient *blockblob.Client // We'll treat all folders as block blobs
	jptm              IJobPartTransferMgr
	sip               ISourceInfoProvider
	metadataToApply   common.SafeMetadata
	headersToApply    blob.HTTPHeaders
	blobTagsToApply   common.BlobTags
}

func newBlobFolderSender(jptm IJobPartTransferMgr, destination string, sip ISourceInfoProvider) (sender, error) {
	s, err := jptm.DstServiceClient().BlobServiceClient()
	if err != nil {
		return nil, err
	}
	destinationClient := s.NewContainerClient(jptm.Info().DstContainer).NewBlockBlobClient(jptm.Info().DstFilePath)

	props, err := sip.Properties()
	if err != nil {
		return nil, err
	}

	var out sender
	fsend := blobFolderSender{
		jptm:              jptm,
		sip:               sip,
		destinationClient: destinationClient,
		metadataToApply: common.SafeMetadata{
			Metadata: props.SrcMetadata.Clone(),
		}, // We're going to modify it, so we should clone it.
		headersToApply:  props.SrcHTTPHeaders.ToBlobHTTPHeaders(),
		blobTagsToApply: props.SrcBlobTags,
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
	// We know for a fact our source is a "blob".
	acl, err := b.sip.(*blobSourceInfoProvider).AccessControl()
	if err != nil {
		b.jptm.FailActiveSend("Grabbing source ACLs", err)
		return
	}

	dsc, err := b.jptm.DstServiceClient().DatalakeServiceClient()
	if err != nil {
		b.jptm.FailActiveSend("Getting source client", err)
		return
	}
	dstDatalakeClient := dsc.NewFileSystemClient(b.jptm.Info().DstContainer).NewFileClient(b.jptm.Info().DstFilePath)
	_, err = dstDatalakeClient.SetAccessControl(b.jptm.Context(), &file.SetAccessControlOptions{ACL: acl})
	if err != nil {
		b.jptm.FailActiveSend("Putting ACLs", err)
		return
	}
}

func (b *blobFolderSender) overwriteDFSProperties() (string, error) {
	b.jptm.Log(common.LogWarning, "It is impossible to completely overwrite a folder with existing content under it on a hierarchical namespace storage account. A best-effort attempt will be made, but if CPK does not match the transfer will fail.")

	err := b.getExtraProperties()
	if err != nil {
		return "Get Extra Properties", fmt.Errorf("when getting additional folder properties: %w", err)
	}

	// do not set folder flag as it's invalid to modify a folder with
	delete(b.metadataToApply.Metadata, "hdi_isfolder")
	delete(b.metadataToApply.Metadata, "Hdi_isfolder")
	// TODO : Here should we undo delete "Hdi_isfolder" too?

	// SetMetadata can set CPK if it wasn't specified prior. This is not a "full" overwrite, but a best-effort overwrite.
	_, err = b.destinationClient.SetMetadata(b.jptm.Context(), b.metadataToApply.Metadata,
		&blob.SetMetadataOptions{
			CPKInfo:      b.jptm.CpkInfo(),
			CPKScopeInfo: b.jptm.CpkScopeInfo(),
		})
	if err != nil {
		return "Set Metadata", fmt.Errorf("A best-effort overwrite was attempted; CPK errors cannot be handled when the blob cannot be deleted.\n%w", err)
	}
	//// blob API not yet supported for HNS account error; re-enable later.
	//_, err = b.destinationClient.SetTags(b.jptm.Context(), b.blobTagsToApply, nil)
	//if err != nil {
	//	return "Set Blob Tags", err
	//}
	_, err = b.destinationClient.SetHTTPHeaders(b.jptm.Context(), b.headersToApply, nil)
	if err != nil {
		return "Set HTTP Headers", err
	}

	// Upload ADLS Gen 2 ACLs
	fromTo := b.jptm.FromTo()
	if fromTo.From().SupportsHnsACLs() && fromTo.To().SupportsHnsACLs() && b.jptm.Info().PreservePermissions.IsTruthy() {
		b.setDatalakeACLs()
	}

	return "", nil
}

func (b *blobFolderSender) SetContainerACL() error {
	// We know for a fact our source is a "blob".
	acl, err := b.sip.(*blobSourceInfoProvider).AccessControl()
	if err != nil {
		b.jptm.FailActiveSend("Grabbing source ACLs", err)
		return folderPropertiesSetInCreation{} // standard completion will detect failure
	}

	dsc, err := b.jptm.DstServiceClient().DatalakeServiceClient()
	if err != nil {
		b.jptm.FailActiveSend("Getting source client", err)
		return folderPropertiesSetInCreation{} // standard completion will detect failure
	}
	dstDatalakeClient := dsc.NewFileSystemClient(b.jptm.Info().DstContainer).NewFileClient(b.jptm.Info().DstFilePath)

	_, err = dstDatalakeClient.SetAccessControl(b.jptm.Context(), &file.SetAccessControlOptions{ACL: acl})
	if err != nil {
		b.jptm.FailActiveSend("Putting ACLs", err)
		return folderPropertiesSetInCreation{} // standard completion will detect failure
	}

	return folderPropertiesSetInCreation{} // standard completion will handle the rest
}

func (b *blobFolderSender) EnsureFolderExists() error {
	t := b.jptm.GetFolderCreationTracker()

	parsedURL, err := blob.ParseURL(b.destinationClient.URL())
	if err != nil {
		return err
	}
	if parsedURL.BlobName == "" {
		return b.SetContainerACL() // Can't do much with a container, but it is here.
	}

	_, err = b.destinationClient.GetProperties(b.jptm.Context(), &blob.GetPropertiesOptions{CPKInfo: b.jptm.CpkInfo()})
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
			_, err := b.destinationClient.Delete(b.jptm.Context(), nil)
			if err != nil {
				if bloberror.HasCode(err, "DirectoryIsNotEmpty") { // this is DFS, and we cannot do a standard replacement on it. Opt to simply overwrite the properties.
					where, err := b.overwriteDFSProperties()
					if err != nil {
						return fmt.Errorf("%w. When %s", err, where)
					}
					return folderPropertiesSetInCreation{}
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

	// Always set this metadata in lower case while creating directories
	if b.metadataToApply.Metadata["Hdi_isfolder"] != nil {
		delete(b.metadataToApply.Metadata, "Hdi_isfolder")
	}
	b.metadataToApply.Metadata["hdi_isfolder"] = to.Ptr("true") // Set folder metadata flag

	err = b.getExtraProperties()
	if err != nil {
		return fmt.Errorf("when getting additional folder properties: %w", err)
	}

	err = t.CreateFolder(b.DirUrlToString(), func() error {
		blobTags := b.blobTagsToApply
		setTags := separateSetTagsRequired(blobTags)
		if setTags || len(blobTags) == 0 {
			blobTags = nil
		}

		// It doesn't make sense to use a special access tier for a blob folder, the blob will be 0 bytes.
		_, err := b.destinationClient.Upload(b.jptm.Context(), streaming.NopCloser(bytes.NewReader(nil)),
			&blockblob.UploadOptions{
				HTTPHeaders:  &b.headersToApply,
				Metadata:     b.metadataToApply.Metadata,
				Tags:         blobTags,
				CPKInfo:      b.jptm.CpkInfo(),
				CPKScopeInfo: b.jptm.CpkScopeInfo(),
			})
		if err != nil {
			b.jptm.FailActiveSend(common.Iff(len(blobTags) > 0, "Upload folder (with tags)", "Upload folder"), err)
		}

		if setTags {
			if _, err := b.destinationClient.SetTags(b.jptm.Context(), b.blobTagsToApply, nil); err != nil {
				b.jptm.FailActiveSend("Set tags", err)
				return nil
			}
		}

		return nil
	})

	if err != nil {
		return fmt.Errorf("when creating folder: %w", err)
	}

	// Upload ADLS Gen 2 ACLs
	fromTo := b.jptm.FromTo()
	if fromTo.From().SupportsHnsACLs() && fromTo.To().SupportsHnsACLs() && b.jptm.Info().PreservePermissions.IsTruthy() {
		b.setDatalakeACLs()
	}

	return nil
}

func (b *blobFolderSender) SetFolderProperties() error {
	return nil // unnecessary, all properties were set on creation.
}

func (b *blobFolderSender) DirUrlToString() string {
	rawURL := b.jptm.Info().Destination
	parsedURL, err := url.Parse(rawURL)
	if err != nil {
		return ""
	}
	parsedURL.RawPath = ""
	parsedURL.RawQuery = ""
	return parsedURL.String()
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

func (d *dummyFolderUploader) GenerateUploadFunc(chunkID common.ChunkID, blockIndex int32, reader common.SingleChunkReader, chunkIsWholeFile bool) chunkFunc {
	panic("this sender only sends folders.")
}

func (d *dummyFolderUploader) Md5Channel() chan<- []byte {
	panic("this sender only sends folders.")
}

// ditto for s2sCopier

type dummyFolderS2SCopier struct {
	blobFolderSender
}

func (d *dummyFolderS2SCopier) GenerateCopyFunc(chunkID common.ChunkID, blockIndex int32, adjustedChunkSize int64, chunkIsWholeFile bool) chunkFunc {
	// TODO implement me
	panic("implement me")
}
