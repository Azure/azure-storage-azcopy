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
	"strings"
	"time"

	datalakesas "github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake/sas"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/streaming"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake/datalakeerror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake/directory"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake/file"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

type DatalakeClientStub interface {
	DFSURL() string
	BlobURL() string
}

type blobFSSenderBase struct {
	jptm                IJobPartTransferMgr
	sip                 ISourceInfoProvider
	blobClient          blockblob.Client
	fileOrDirClient     DatalakeClientStub
	parentDirClient     *directory.Client
	chunkSize           int64
	numChunks           uint32
	pacer               pacer
	creationTimeHeaders *file.HTTPHeaders
	flushThreshold      int64
	metadataToSet       *common.SafeMetadata
}

func newBlobFSSenderBase(jptm IJobPartTransferMgr, destination string, pacer pacer, sip ISourceInfoProvider) (*blobFSSenderBase, error) {
	info := jptm.Info()

	// compute chunk size and number of chunks
	chunkSize := info.BlockSize
	numChunks := getNumChunks(info.SourceSize, chunkSize, chunkSize)

	props, err := sip.Properties()
	if err != nil {
		return nil, err
	}
	headers := props.SrcHTTPHeaders.ToBlobFSHTTPHeaders()

	s, err := jptm.DstServiceClient().DatalakeServiceClient()
	if err != nil {
		return nil, err
	}

	datalakeURLParts, err := azdatalake.ParseURL(destination)
	if err != nil {
		return nil, err
	}
	fsc := s.NewFileSystemClient(datalakeURLParts.FileSystemName)
	directoryOrFilePath := datalakeURLParts.PathName
	parentPath := ""
	if strings.LastIndex(directoryOrFilePath, "/") != -1 {
		parentPath = directoryOrFilePath[:strings.LastIndex(directoryOrFilePath, "/")]
	}

	var destClient DatalakeClientStub
	if info.IsFolderPropertiesTransfer() {
		destClient = fsc.NewDirectoryClient(directoryOrFilePath)
	} else {
		destClient = fsc.NewFileClient(directoryOrFilePath)
	}

	bsc, _ := jptm.DstServiceClient().BlobServiceClient()

	return &blobFSSenderBase{
		jptm:                jptm,
		sip:                 sip,
		blobClient:          *bsc.NewContainerClient(info.DstContainer).NewBlockBlobClient(info.DstFilePath),
		fileOrDirClient:     destClient,
		parentDirClient:     fsc.NewDirectoryClient(parentPath),
		chunkSize:           chunkSize,
		numChunks:           numChunks,
		pacer:               pacer,
		creationTimeHeaders: &headers,
		flushThreshold:      chunkSize * int64(ADLSFlushThreshold),
		metadataToSet:       &common.SafeMetadata{Metadata: props.SrcMetadata.Clone()},
	}, nil
}

func (u *blobFSSenderBase) getFileClient() *file.Client {
	return u.fileOrDirClient.(*file.Client)
}

func (u *blobFSSenderBase) getDirectoryClient() *directory.Client {
	return u.fileOrDirClient.(*directory.Client)
}

func (u *blobFSSenderBase) SendableEntityType() common.EntityType {
	if _, ok := u.fileOrDirClient.(*directory.Client); ok {
		return common.EEntityType.Folder()
	} else {
		return common.EEntityType.File()
	}
}

func (u *blobFSSenderBase) ChunkSize() int64 {
	return u.chunkSize
}

func (u *blobFSSenderBase) NumChunks() uint32 {
	return u.numChunks
}

func (u *blobFSSenderBase) RemoteFileExists() (bool, time.Time, error) {
	props, err := u.getFileClient().GetProperties(u.jptm.Context(), nil)
	return remoteObjectExists(datalakePropertiesResponseAdapter{props}, err)
}

func (u *blobFSSenderBase) Prologue(state common.PrologueState) (destinationModified bool) {

	destinationModified = true

	// create the directory separately
	// This "burns" an extra IO operation, unfortunately, but its the only way we can make our
	// folderCreationTracker work, and we need that for our overwrite logic for folders.
	// (Even tho there's not much in the way of properties to set in ADLS Gen 2 on folders, at least, not
	// that we support right now, we still run the same folder logic here to be consistent with our other
	// folder-aware sources).
	err := u.doEnsureDirExists(u.parentDirClient)
	if err != nil {
		u.jptm.FailActiveUpload("Ensuring parent directory exists", err)
		return
	}

	// Create file with the source size
	_, err = u.getFileClient().Create(u.jptm.Context(), &file.CreateOptions{HTTPHeaders: u.creationTimeHeaders}) // "create" actually calls "create path", so if we didn't need to track folder creation, we could just let this call create the folder as needed
	if err != nil {
		u.jptm.FailActiveUpload("Creating file", err)
		return
	}
	return
}

func (u *blobFSSenderBase) Cleanup() {
	jptm := u.jptm

	// Cleanup if status is now failed
	if jptm.IsDeadInflight() {
		// transfer was either failed or cancelled
		// the file created in share needs to be deleted, since it's
		// contents will be at an unknown stage of partial completeness
		deletionContext, cancelFn := context.WithTimeout(context.WithValue(context.Background(), ServiceAPIVersionOverride, DefaultServiceApiVersion), 2*time.Minute)
		defer cancelFn()
		_, err := u.getFileClient().Delete(deletionContext, nil)
		if err != nil {
			jptm.Log(common.LogError, fmt.Sprintf("error deleting the (incomplete) file %s. Failed with error %s", u.getFileClient().DFSURL(), err.Error()))
		}
	}
}

func (u *blobFSSenderBase) GetDestinationLength() (int64, error) {
	prop, err := u.getFileClient().GetProperties(u.jptm.Context(), nil)

	if err != nil {
		return -1, err
	}

	if prop.ContentLength == nil {
		return -1, fmt.Errorf("destination content length not returned")
	}
	return *prop.ContentLength, nil
}

func (u *blobFSSenderBase) EnsureFolderExists() error {
	return u.doEnsureDirExists(u.getDirectoryClient())
}

func isFilesystemRoot(directoryClient *directory.Client) (bool, error) {
	datalakeURLParts, err := azdatalake.ParseURL(directoryClient.DFSURL())
	if err != nil {
		return false, err
	}
	return datalakeURLParts.PathName == "", nil
}

func (u *blobFSSenderBase) doEnsureDirExists(directoryClient *directory.Client) error {
	isFSRoot, err := isFilesystemRoot(directoryClient)
	if err != nil {
		return err
	}
	if isFSRoot {
		return nil // nothing to do, there's no directory component to create
	}
	// must always do this, regardless of whether we are called in a file-centric code path
	// or a folder-centric one, since with the parallelism we use, we don't actually
	// know which will happen first
	err = u.jptm.GetFolderCreationTracker().CreateFolder(directoryClient.DFSURL(), func() error {
		_, err := directoryClient.Create(u.jptm.Context(), &directory.CreateOptions{AccessConditions: &directory.AccessConditions{ModifiedAccessConditions: &directory.ModifiedAccessConditions{IfNoneMatch: to.Ptr(azcore.ETagAny)}}})

		if datalakeerror.HasCode(err, datalakeerror.PathAlreadyExists) {
			return common.FolderCreationErrorAlreadyExists{}
		}

		return err
	})
	return err
}

func (u *blobFSSenderBase) GetSourcePOSIXProperties() (common.UnixStatAdapter, error) {
	if unixSIP, ok := u.sip.(IUNIXPropertyBearingSourceInfoProvider); ok {
		statAdapter, err := unixSIP.GetUNIXProperties()
		if err != nil {
			return nil, err
		}

		return statAdapter, nil
	} else {
		return nil, nil // no properties present!
	}
}

func (u *blobFSSenderBase) SetPOSIXProperties() error {
	adapter, err := u.GetSourcePOSIXProperties()
	if err != nil {
		return fmt.Errorf("failed to get POSIX properties: %w", err)
	} else if adapter == nil {
		return nil
	}

	meta := u.metadataToSet
	common.AddStatToBlobMetadata(adapter, meta, u.jptm.Info().PosixPropertiesStyle)
	delete(meta.Metadata, common.POSIXFolderMeta) // Can't be set on HNS accounts.

	_, err = u.blobClient.SetMetadata(u.jptm.Context(), meta.Metadata, nil)
	return err
}

func (u *blobFSSenderBase) SetFolderProperties() error {
	if u.jptm.Info().PreservePOSIXProperties {
		return u.SetPOSIXProperties()
	} else if len(u.metadataToSet.Metadata) > 0 {
		_, err := u.blobClient.SetMetadata(u.jptm.Context(), u.metadataToSet.Metadata, nil)
		if err != nil {
			return fmt.Errorf("failed to set metadata: %w", err)
		}
	}

	return nil
}

func (u *blobFSSenderBase) DirUrlToString() string {
	directoryURL := u.getDirectoryClient().DFSURL()

	parts, err := datalakesas.ParseURL(directoryURL)
	common.PanicIfErr(err)

	parts.SAS = datalakesas.QueryParameters{}
	parts.UnparsedParams = ""

	if parts.PathName == "/" {
		parts.PathName = ""
	}

	return parts.String()
}

func (u *blobFSSenderBase) SendSymlink(linkData string) error {
	meta := &common.SafeMetadata{Metadata: make(common.Metadata)} // meta isn't traditionally supported for dfs, but still exists
	adapter, err := u.GetSourcePOSIXProperties()
	if err != nil {
		return fmt.Errorf("when polling for POSIX properties: %w", err)
	} else if adapter != nil { // We don't need POSIX data to send a symlink.
		common.AddStatToBlobMetadata(adapter, meta, u.jptm.Info().PosixPropertiesStyle)
	}

	meta.Metadata[common.POSIXSymlinkMeta] = to.Ptr("true") // just in case there isn't any metadata
	blobHeaders := blob.HTTPHeaders{                        // translate headers, since those still apply
		BlobContentType:        u.creationTimeHeaders.ContentType,
		BlobContentEncoding:    u.creationTimeHeaders.ContentEncoding,
		BlobContentLanguage:    u.creationTimeHeaders.ContentLanguage,
		BlobContentDisposition: u.creationTimeHeaders.ContentDisposition,
		BlobCacheControl:       u.creationTimeHeaders.CacheControl,
		BlobContentMD5:         u.creationTimeHeaders.ContentMD5,
	}
	_, err = u.blobClient.Upload(
		u.jptm.Context(),
		streaming.NopCloser(strings.NewReader(linkData)),
		&blockblob.UploadOptions{
			HTTPHeaders: &blobHeaders,
			Metadata:    meta.Metadata,
		})

	return err
}
