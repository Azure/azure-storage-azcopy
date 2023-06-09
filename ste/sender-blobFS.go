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
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/streaming"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"net/url"
	"strings"
	"time"

	"github.com/Azure/azure-pipeline-go/pipeline"

	"github.com/Azure/azure-storage-azcopy/v10/azbfs"
	"github.com/Azure/azure-storage-azcopy/v10/common"
)

type blobFSSenderBase struct {
	jptm                IJobPartTransferMgr
	sip          ISourceInfoProvider
	fileOrDirURL URLHolderV1
	chunkSize    int64
	numChunks           uint32
	pipeline            pipeline.Pipeline
	pacer               pacer
	creationTimeHeaders *azbfs.BlobFSHTTPHeaders
	flushThreshold      int64
}

func newBlobFSSenderBase(jptm IJobPartTransferMgr, destination string, p pipeline.Pipeline, pacer pacer, sip ISourceInfoProvider) (*blobFSSenderBase, error) {

	info := jptm.Info()

	// compute chunk size and number of chunks
	chunkSize := info.BlockSize
	numChunks := getNumChunks(info.SourceSize, chunkSize)

	// make sure URL is parsable
	destURL, err := url.Parse(destination)
	if err != nil {
		return nil, err
	}

	props, err := sip.Properties()
	if err != nil {
		return nil, err
	}
	headers := props.SrcHTTPHeaders.ToBlobFSHTTPHeaders()

	var h URLHolderV1
	if info.IsFolderPropertiesTransfer() {
		h = azbfs.NewDirectoryURL(*destURL, p)
	} else {
		h = azbfs.NewFileURL(*destURL, p)
	}
	return &blobFSSenderBase{
		jptm:                jptm,
		sip: 				 sip,
		fileOrDirURL:        h,
		chunkSize:           chunkSize,
		numChunks:           numChunks,
		pipeline:            p,
		pacer:               pacer,
		creationTimeHeaders: &headers,
		flushThreshold:      chunkSize * int64(ADLSFlushThreshold),
	}, nil
}

func (u *blobFSSenderBase) fileURL() azbfs.FileURL {
	return u.fileOrDirURL.(azbfs.FileURL)
}

func (u *blobFSSenderBase) dirURL() azbfs.DirectoryURL {
	return u.fileOrDirURL.(azbfs.DirectoryURL)
}

func (u *blobFSSenderBase) SendableEntityType() common.EntityType {
	if _, ok := u.fileOrDirURL.(azbfs.DirectoryURL); ok {
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

// simply provides the parse lmt from the path properties
// TODO it's not the best solution as usually the SDK should provide the time in parsed format already
type blobFSLastModifiedTimeProvider struct {
	lmt time.Time
}

func (b blobFSLastModifiedTimeProvider) LastModified() time.Time {
	return b.lmt
}

func newBlobFSLastModifiedTimeProvider(props *azbfs.PathGetPropertiesResponse) blobFSLastModifiedTimeProvider {
	var lmt time.Time
	// parse the lmt if the props is not empty
	if props != nil {
		parsedLmt, err := time.Parse(time.RFC1123, props.LastModified())
		if err == nil {
			lmt = parsedLmt
		}
	}

	return blobFSLastModifiedTimeProvider{lmt: lmt}
}

func (u *blobFSSenderBase) RemoteFileExists() (bool, time.Time, error) {
	props, err := u.fileURL().GetProperties(u.jptm.Context())
	return remoteObjectExists(newBlobFSLastModifiedTimeProvider(props), err)
}

func (u *blobFSSenderBase) Prologue(state common.PrologueState) (destinationModified bool) {

	destinationModified = true

	// create the directory separately
	// This "burns" an extra IO operation, unfortunately, but its the only way we can make our
	// folderCreationTracker work, and we need that for our overwrite logic for folders.
	// (Even tho there's not much in the way of properties to set in ADLS Gen 2 on folders, at least, not
	// that we support right now, we still run the same folder logic here to be consistent with our other
	// folder-aware sources).
	parentDir, err := u.fileURL().GetParentDir()
	if err != nil {
		u.jptm.FailActiveUpload("Getting parent directory URL", err)
		return
	}
	err = u.doEnsureDirExists(parentDir)
	if err != nil {
		u.jptm.FailActiveUpload("Ensuring parent directory exists", err)
		return
	}

	// Create file with the source size
	_, err = u.fileURL().Create(u.jptm.Context(), *u.creationTimeHeaders, azbfs.BlobFSAccessControl{}) // "create" actually calls "create path", so if we didn't need to track folder creation, we could just let this call create the folder as needed
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
		_, err := u.fileURL().Delete(deletionContext)
		if err != nil {
			jptm.Log(pipeline.LogError, fmt.Sprintf("error deleting the (incomplete) file %s. Failed with error %s", u.fileURL().String(), err.Error()))
		}
	}
}

func (u *blobFSSenderBase) GetDestinationLength() (int64, error) {
	prop, err := u.fileURL().GetProperties(u.jptm.Context())

	if err != nil {
		return -1, err
	}

	return prop.ContentLength(), nil
}

func (u *blobFSSenderBase) EnsureFolderExists() error {
	return u.doEnsureDirExists(u.dirURL())
}

func (u *blobFSSenderBase) doEnsureDirExists(d azbfs.DirectoryURL) error {
	if d.IsFileSystemRoot() {
		return nil // nothing to do, there's no directory component to create
	}
	// must always do this, regardless of whether we are called in a file-centric code path
	// or a folder-centric one, since with the parallelism we use, we don't actually
	// know which will happen first
	dirUrl := d.URL()
	err := u.jptm.GetFolderCreationTracker().CreateFolder(dirUrl.String(), func() error {
		_, err := d.Create(u.jptm.Context(), false)
		return err
	})
	if stgErr, ok := err.(azbfs.StorageError); ok && stgErr.ServiceCode() == azbfs.ServiceCodePathAlreadyExists {
		return nil // not a error as far as we are concerned. It just already exists
	}
	return err
}

func (u *blobFSSenderBase) GetBlobURL() (*blockblob.Client, error) {
	blobURLParts, err := blob.ParseURL(u.fileOrDirURL.String())
	if err != nil {
		return nil, err
	}
	blobURLParts.Host = strings.ReplaceAll(blobURLParts.Host, ".dfs", ".blob") // switch back to blob

	client := common.CreateBlockBlobClient(blobURLParts.String(), u.jptm.CredentialInfo(), u.jptm.CredentialOpOptions(), u.jptm.ClientOptions())
	return client, nil
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
		return fmt.Errorf("failed to get POSIX properties")
	} else if adapter == nil {
		return nil
	}

	meta := common.Metadata{}
	common.AddStatToBlobMetadata(adapter, meta)
	delete(meta, common.POSIXFolderMeta) // Can't be set on HNS accounts.

	client, err := u.GetBlobURL()
	if err != nil {
		return err
	}
	_, err = client.SetMetadata(u.jptm.Context(), meta, nil)
	return err
}

func (u *blobFSSenderBase) SetFolderProperties() error {
	return u.SetPOSIXProperties()
}

func (u *blobFSSenderBase) DirUrlToString() string {
	dirUrl := u.dirURL().URL()
	// To avoid encoding/decoding
	dirUrl.RawPath = ""
	// To avoid SAS token
	dirUrl.RawQuery = ""
	return dirUrl.String()
}

func (u *blobFSSenderBase) SendSymlink(linkData string) error {
	meta := common.Metadata{} // meta isn't traditionally supported for dfs, but still exists
	adapter, err := u.GetSourcePOSIXProperties()
	if err != nil {
		return fmt.Errorf("when polling for POSIX properties: %w", err)
	} else if adapter == nil {
		return nil // No-op
	}

	common.AddStatToBlobMetadata(adapter, meta)
	meta[common.POSIXSymlinkMeta] = to.Ptr("true") // just in case there isn't any metadata
	blobHeaders := blob.HTTPHeaders{ // translate headers, since those still apply
		BlobContentType: &u.creationTimeHeaders.ContentType,
		BlobContentEncoding: &u.creationTimeHeaders.ContentEncoding,
		BlobContentLanguage: &u.creationTimeHeaders.ContentLanguage,
		BlobContentDisposition: &u.creationTimeHeaders.ContentDisposition,
		BlobCacheControl: &u.creationTimeHeaders.CacheControl,
	}
	client, err := u.GetBlobURL()
	if err != nil {
		return err
	}
	_, err = client.Upload(
		u.jptm.Context(),
		streaming.NopCloser(strings.NewReader(linkData)),
		&blockblob.UploadOptions{
			HTTPHeaders: &blobHeaders,
			Metadata: meta,
		})

	return err
}
