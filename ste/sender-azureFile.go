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
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-file-go/azfile"

	"github.com/Azure/azure-storage-azcopy/common"
)

type URLHolder interface {
	URL() url.URL
	String() string
}

// TODO: review the decision to have this implement both interfaces
// azureFileSenderBase implements both IFolderSender and (most of) IFileSender.
// Why implement both interfaces in the one type, even though they are largely unrelated? Because it
// makes functions like newAzureFilesUploader easier to reason about, since they always return the same type.
// It may also make it easier to describe what's needed when supporting an new backend - e.g. "to send to a new back end
// you need a sender that implements IFileSender and, if the back end is folder aware, it should also implement IFolderSender"
// (The alternative would be to have the likes of newAzureFilesUploader call sip.EntityType and return a different type
// if the entity type is folder).
type azureFileSenderBase struct {
	jptm         IJobPartTransferMgr
	fileOrDirURL URLHolder
	chunkSize    uint32
	numChunks    uint32
	pipeline     pipeline.Pipeline
	pacer        pacer
	ctx          context.Context
	// Headers and other info that we will apply to the destination
	// object. For S2S, these come from the source service.
	// When sending local data, they are computed based on
	// the properties of the local file
	headersToApply  azfile.FileHTTPHeaders
	metadataToApply azfile.Metadata
}

func newAzureFileSenderBase(jptm IJobPartTransferMgr, destination string, p pipeline.Pipeline, pacer pacer, sip ISourceInfoProvider) (*azureFileSenderBase, error) {

	info := jptm.Info()

	// compute chunk size (irrelevant but harmless for folders)
	// If the given chunk Size for the Job is greater than maximum file chunk size i.e 4 MB
	// then chunk size will be 4 MB.
	chunkSize := info.BlockSize
	if chunkSize > common.DefaultAzureFileChunkSize {
		chunkSize = common.DefaultAzureFileChunkSize
		if jptm.ShouldLog(pipeline.LogWarning) {
			jptm.Log(pipeline.LogWarning,
				fmt.Sprintf("Block size %d larger than maximum file chunk size, 4 MB chunk size used", info.BlockSize))
		}
	}

	// compute num chunks (irrelevant but harmless for folders)
	numChunks := getNumChunks(info.SourceSize, chunkSize)

	// make sure URL is parsable
	destURL, err := url.Parse(destination)
	if err != nil {
		return nil, err
	}

	// due to the REST parity feature added in 2019-02-02, the File APIs are no longer backward compatible
	// so we must use the latest SDK version to stay safe
	ctx := context.WithValue(jptm.Context(), ServiceAPIVersionOverride, azfile.ServiceVersion)

	props, err := sip.Properties()
	if err != nil {
		return nil, err
	}

	var h URLHolder
	if info.IsFolderPropertiesTransfer() {
		h = azfile.NewDirectoryURL(*destURL, p)
	} else {
		h = azfile.NewFileURL(*destURL, p)
	}

	return &azureFileSenderBase{
		jptm:            jptm,
		fileOrDirURL:    h,
		chunkSize:       chunkSize,
		numChunks:       numChunks,
		pipeline:        p,
		pacer:           pacer,
		ctx:             ctx,
		headersToApply:  props.SrcHTTPHeaders.ToAzFileHTTPHeaders(),
		metadataToApply: props.SrcMetadata.ToAzFileMetadata(),
	}, nil
}

func (u *azureFileSenderBase) fileURL() azfile.FileURL {
	return u.fileOrDirURL.(azfile.FileURL)
}

func (u *azureFileSenderBase) dirURL() azfile.DirectoryURL {
	return u.fileOrDirURL.(azfile.DirectoryURL)
}

func (u *azureFileSenderBase) SendableEntityType() common.EntityType {
	if _, ok := u.fileOrDirURL.(azfile.DirectoryURL); ok {
		return common.EEntityType.Folder()
	} else {
		return common.EEntityType.File()
	}
}

func (u *azureFileSenderBase) ChunkSize() uint32 {
	return u.chunkSize
}

func (u *azureFileSenderBase) NumChunks() uint32 {
	return u.numChunks
}

func (u *azureFileSenderBase) RemoteFileExists() (bool, error) {
	return remoteObjectExists(u.fileURL().GetProperties(u.ctx))
}

func (u *azureFileSenderBase) Prologue(state common.PrologueState) (destinationModified bool) {
	jptm := u.jptm
	info := jptm.Info()

	destinationModified = true

	// Create the parent directories of the file. Note share must be existed, as the files are listed from share or directory.
	err := AzureFileParentDirCreator{}.CreateParentDirToRoot(u.ctx, u.fileURL(), u.pipeline, u.jptm.GetFolderCreationTracker())
	if err != nil {
		jptm.FailActiveUpload("Creating parent directory", err)
		return
	}

	if state.CanInferContentType() {
		// sometimes, specifically when reading local files, we have more info
		// about the file type at this time than what we had before
		u.headersToApply.ContentType = state.GetInferredContentType(u.jptm)
	}

	// Create Azure file with the source size
	_, err = u.fileURL().Create(u.ctx, info.SourceSize, u.headersToApply, u.metadataToApply)
	if err != nil {
		jptm.FailActiveUpload("Creating file", err)
		return
	}

	return
}

func (u *azureFileSenderBase) Cleanup() {
	jptm := u.jptm

	// Cleanup
	if jptm.IsDeadInflight() {
		// transfer was either failed or cancelled
		// the file created in share needs to be deleted, since it's
		// contents will be at an unknown stage of partial completeness
		deletionContext, cancelFn := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancelFn()
		_, err := u.fileURL().Delete(deletionContext)
		if err != nil {
			jptm.Log(pipeline.LogError, fmt.Sprintf("error deleting the (incomplete) file %s. Failed with error %s", u.fileOrDirURL.String(), err.Error()))
		}
	}
}

func (u *azureFileSenderBase) GetDestinationLength() (int64, error) {
	prop, err := u.fileURL().GetProperties(u.ctx)

	if err != nil {
		return -1, err
	}

	return prop.ContentLength(), nil
}

func (u *azureFileSenderBase) EnsureFolderExists() error {
	return AzureFileParentDirCreator{}.CreateDirToRoot(u.ctx, u.dirURL(), u.pipeline, u.jptm.GetFolderCreationTracker())
}

func (u *azureFileSenderBase) SetFolderProperties() error {
	// TODO
	return nil
}

// namespace for functions related to creating parent directories in Azure File
// to avoid free floating global funcs
type AzureFileParentDirCreator struct{}

// getParentDirectoryURL gets parent directory URL of an Azure FileURL.
func (AzureFileParentDirCreator) getParentDirectoryURL(uh URLHolder, p pipeline.Pipeline) azfile.DirectoryURL {
	u := uh.URL()
	u.Path = u.Path[:strings.LastIndex(u.Path, "/")]
	return azfile.NewDirectoryURL(u, p)
}

// verifyAndHandleCreateErrors handles create errors, StatusConflict is ignored, as specific level directory could be existing.
// Report http.StatusForbidden, as user should at least have read and write permission of the destination,
// and there is no permission on directory level, i.e. create directory is a general permission for each level diretories for Azure file.
func (AzureFileParentDirCreator) verifyAndHandleCreateErrors(err error) error {
	if err != nil {
		sErr, sErrOk := err.(azfile.StorageError)
		if sErrOk && sErr.Response() != nil &&
			(sErr.Response().StatusCode == http.StatusConflict) { // Note the ServiceCode actually be AuthenticationFailure when share failed to be created, if want to create share as well.
			return nil
		}
		return err
	}

	return nil
}

// splitWithoutToken splits string with a given token, and returns splitted results without token.
func (AzureFileParentDirCreator) splitWithoutToken(str string, token rune) []string {
	return strings.FieldsFunc(str, func(c rune) bool {
		return c == token
	})
}

// CreateParentDirToRoot creates parent directories of the Azure file if file's parent directory doesn't exist.
func (d AzureFileParentDirCreator) CreateParentDirToRoot(ctx context.Context, fileURL azfile.FileURL, p pipeline.Pipeline, t common.FolderCreationTracker) error {
	dirURL := d.getParentDirectoryURL(fileURL, p)
	return d.CreateDirToRoot(ctx, dirURL, p, t)
}

// CreateDirToRoot Creates the dir (and parents as necessary) if it does not exist
func (d AzureFileParentDirCreator) CreateDirToRoot(ctx context.Context, dirURL azfile.DirectoryURL, p pipeline.Pipeline, t common.FolderCreationTracker) error {
	dirURLExtension := common.FileURLPartsExtension{FileURLParts: azfile.NewFileURLParts(dirURL.URL())}
	if _, err := dirURL.GetProperties(ctx); err != nil {
		if stgErr, stgErrOk := err.(azfile.StorageError); stgErrOk && stgErr.Response() != nil &&
			stgErr.Response().StatusCode == http.StatusNotFound { // At least need read and write permisson for destination
			// File's parent directory doesn't exist, try to create the parent directories.
			// Split directories as segments.
			segments := d.splitWithoutToken(dirURLExtension.DirectoryOrFilePath, '/')

			shareURL := azfile.NewShareURL(dirURLExtension.GetShareURL(), p)
			curDirURL := shareURL.NewRootDirectoryURL() // Share directory should already exist, doesn't support creating share
			// Try to create the directories
			for i := 0; i < len(segments); i++ {
				curDirURL = curDirURL.NewDirectoryURL(segments[i])
				_, err := curDirURL.Create(ctx, azfile.Metadata{})
				if err == nil {
					// We did create it, so record that fact. I.e. THIS job created the folder.
					// Must do it here, in the routine that is shared by both the folder and the file code,
					// because due to the parallelism of AzCopy, we don't know which will get here first, file code, or folder code.
					dirUrl := curDirURL.URL()
					t.RecordCreation(dirUrl.String())
				}
				if verifiedErr := d.verifyAndHandleCreateErrors(err); verifiedErr != nil {
					return verifiedErr
				}
			}
		} else {
			return err
		}
	}

	// Directly return if parent directory exists.
	return nil
}
