// Copyright © Microsoft <wastore@microsoft.com>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package ste

import (
	"context"
	"fmt"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/file"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/fileerror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/share"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/pacer"
)

type azureFileUploader struct {
	azureFileSenderBase
	md5Channel chan []byte
}

func newAzureFilesUploader(jptm IJobPartTransferMgr, destination string, pacer pacer.Interface, sip ISourceInfoProvider) (sender, error) {
	senderBase, err := newAzureFileSenderBase(jptm, destination, pacer, sip)
	if err != nil {
		return nil, err
	}

	return &azureFileUploader{azureFileSenderBase: *senderBase, md5Channel: newMd5Channel()}, nil
}

func (u *azureFileUploader) Md5Channel() chan<- []byte {
	return u.md5Channel
}

func (u *azureFileUploader) GenerateUploadFunc(id common.ChunkID, blockIndex int32, reader common.SingleChunkReader, chunkIsWholeFile bool) chunkFunc {

	return createSendToRemoteChunkFunc(u.jptm, id, func() {
		jptm := u.jptm

		defer reader.Close() // In case of memory leak in sparse file case.

		if jptm.Info().SourceSize == 0 {
			// nothing to do, since this is a dummy chunk in a zero-size file, and the prologue will have done all the real work
			return
		}

		if reader.HasPrefetchedEntirelyZeros() {
			// for this destination type, there is no need to upload ranges than consist entirely of zeros
			jptm.Log(common.LogDebug,
				fmt.Sprintf("Not uploading range from %d to %d,  all bytes are zero",
					id.OffsetInFile(), id.OffsetInFile()+reader.Length()))
			return
		}

		// upload the byte range represented by this chunk
		jptm.LogChunkStatus(id, common.EWaitReason.Body())
		// inject our pacer so our policy picks it up
		pacerCtx, err := u.pacer.InjectPacer(reader.Length(), u.jptm.FromTo(), u.ctx)
		if err != nil {
			u.jptm.FailActiveDownload("Injecting pacer into context", err)
			return
		}

		_, err = u.getFileClient().UploadRange(pacerCtx, id.OffsetInFile(), reader, nil)
		if err != nil {
			jptm.FailActiveUpload("Uploading range", err)
			return
		}
	})
}

func (u *azureFileUploader) Epilogue() {
	u.azureFileSenderBase.Epilogue()

	jptm := u.jptm

	// set content MD5 (only way to do this is to re-PUT all the headers, this time with the MD5 included)
	if jptm.IsLive() {
		tryPutMd5Hash(jptm, u.md5Channel, func(md5Hash []byte) error {
			if len(md5Hash) == 0 {
				return nil
			}

			u.headersToApply.ContentMD5 = md5Hash
			_, err := u.getFileClient().SetHTTPHeaders(u.ctx, &file.SetHTTPHeadersOptions{
				HTTPHeaders:   &u.headersToApply,
				Permissions:   &u.permissionsToApply,
				SMBProperties: &u.smbPropertiesToApply,
			})
			return err
		})
	}
}

// SendSymlink creates a symbolic link on Azure Files NFS with the given link data.
func (u *azureFileUploader) SendSymlink(linkData string) error {

	jptm := u.jptm
	info := jptm.Info()
	if !jptm.FromTo().IsNFS() {
		return nil
	}

	createSymlinkOptions := &file.CreateSymbolicLinkOptions{
		Metadata: u.metadataToApply,
	}

	stage, err := u.addNFSPropertiesToHeaders(info)
	if err != nil {
		jptm.FailActiveSend(stage, err)
		return err
	}

	stage, err = u.addNFSPermissionsToHeaders(info, u.getFileClient().URL())
	if err != nil {
		jptm.FailActiveSend(stage, err)
		return err
	}
	createSymlinkOptions.FileNFSProperties = &file.NFSProperties{
		CreationTime:  u.nfsPropertiesToApply.CreationTime,
		LastWriteTime: u.nfsPropertiesToApply.LastWriteTime,
		Owner:         u.nfsPropertiesToApply.Owner,
		Group:         u.nfsPropertiesToApply.Group,
		FileMode:      u.nfsPropertiesToApply.FileMode,
	}

	err = DoWithCreateSymlinkOnAzureFilesNFS(u.ctx,
		func() error {
			_, err := u.getFileClient().CreateSymbolicLink(u.ctx, linkData, createSymlinkOptions)
			return err
		},
		u.getFileClient(),
		u.shareClient,
		u.pacer,
		u.jptm)

	// if still failing, give up
	if err != nil {
		jptm.FailActiveUpload("Creating symlink", err)
		return fmt.Errorf("failed to create symlink: %w", err)
	}

	u.jptm.Log(common.LogDebug, fmt.Sprintf("Created symlink with data: %s", linkData))
	return nil
}

// DoWithCreateSymlinkOnAzureFilesNFS tries to create a symlink, with retry logic
// for parent not found and resource already exists.
func DoWithCreateSymlinkOnAzureFilesNFS(
	ctx context.Context,
	action func() error,
	client *file.Client,
	shareClient *share.Client,
	pacer pacer.Interface,
	jptm IJobPartTransferMgr,
) error {
	// try the action
	err := action()

	// did fail because parent is missing?
	if fileerror.HasCode(err, fileerror.ParentNotFound) {
		jptm.Log(common.LogInfo,
			fmt.Sprintf("%s: %s \nAzCopy will create parent directories for the symlink.",
				fileerror.ParentNotFound, err.Error()))

		err = AzureFileParentDirCreator{}.CreateParentDirToRoot(ctx,
			client, shareClient, jptm.GetFolderCreationTracker())
		if err != nil {
			jptm.FailActiveUpload("Creating parent directory", err)
		}

		// retry the action
		err = action()
	}

	// did fail because item already exists on the destination?
	// The destination object can be a symlink, file or directory.
	// If it's a symlink or a file, we will delete it try creating symlink.
	// If it's a directory, we will fail.
	if fileerror.HasCode(err, fileerror.ResourceAlreadyExists) {
		jptm.Log(common.LogWarning,
			fmt.Sprintf("%s: %s \nAzCopy will delete and recreate the symlink.",
				fileerror.ResourceAlreadyExists, err.Error()))

		// destination symlink already exists we try to delete the destination symlink
		_, delErr := client.Delete(ctx, nil)
		if delErr != nil {
			jptm.FailActiveUpload("Deleting existing symlink", delErr)
		}

		// retry the action
		err = action()
	}

	// did fail because resource type mismatch?
	// This can happen if the destination is a file or a directory.
	// We will delete the destination and try creating the symlink.
	// If the destination is a directory, the delete will fail and we will fail the transfer.
	if fileerror.HasCode(err, fileerror.ResourceTypeMismatch) {
		jptm.Log(common.LogWarning,
			fmt.Sprintf("%s: %s \nAzCopy will delete the destination resource.",
				fileerror.ResourceTypeMismatch, err.Error()))

		// destination can be a file
		if _, delErr := client.Delete(ctx, nil); delErr != nil {
			// if this fails it means the destination is a directory
			// we don't support deleting a directory here because it can be recursive and dangerous
			// so we fail the transfer
			// customer can manually delete the destination directory and rerun the transfer
			jptm.FailActiveUpload("Deleting existing resource", delErr)
		}

		// retry the action
		err = action()
	}

	return err
}
