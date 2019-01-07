// Copyright © 2017 Microsoft <wastore@microsoft.com>
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
	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-file-go/2017-07-29/azfile"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

type azureFilesUploader struct {
	jptm         IJobPartTransferMgr
	fileURL      azfile.FileURL
	chunkSize    uint32
	numChunks    uint32
	pipeline     pipeline.Pipeline
	pacer        *pacer
	leadingBytes []byte
	prologueOnce *sync.Once
}

func newAzureFilesUploader(jptm IJobPartTransferMgr, destination string, p pipeline.Pipeline, pacer *pacer) (uploader, error) {

	info := jptm.Info()

	// compute chunk size
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

	// compute num chunks
	numChunks := getNumUploadChunks(info.SourceSize, chunkSize)

	// make sure URL is parsable
	destURL, err := url.Parse(destination)
	if err != nil {
		return nil, err
	}

	return &azureFilesUploader{
		jptm:         jptm,
		fileURL:      azfile.NewFileURL(*destURL, p),
		chunkSize:    chunkSize,
		numChunks:    numChunks,
		pipeline:     p,
		pacer:        pacer,
		prologueOnce: &sync.Once{},
	}, nil
}

func (au *azureFilesUploader) ChunkSize() uint32 {
	return au.chunkSize
}

func (au *azureFilesUploader) NumChunks() uint32 {
	return au.numChunks
}

func (au *azureFilesUploader) SetLeadingBytes(leadingBytes []byte) {
	au.leadingBytes = leadingBytes
}

func (au *azureFilesUploader) RemoteFileExists() (bool, error) {
	_, err := au.fileURL.GetProperties(au.jptm.Context())
	return err != nil, nil // TODO: is there a better, more robust way to do this check, rather than just taking ANY error as evidence of non-existence?
}

// For AzureFiles, it's necessary to create the file before sending any data to it
// We use a sync.Once to help us do this exactly once.
// Why do we do this with a sync.Once, and call it from the chunkfunc?  Instead, couldn't we call it just once at the start, from localToRemote?
// No... or at least, not easily. The reason is that this needs the first bytes of the file, for MIME-type detection. And we don't really get to those
// bytes in localToRemote until we are in the chunkfunc scheduling loop.   Getting those bytes earlier, without the perf cost of reading that part of the
// file twice, would be a messy refactoring.  So we do this here instead.
func (au *azureFilesUploader) runPrologueOnce() {
	au.prologueOnce.Do(func() {

		jptm := au.jptm
		info := jptm.Info()

		// Create the parent directories of the file. Note share must be existed, as the files are listed from share or directory.
		err := createParentDirToRoot(jptm.Context(), au.fileURL, au.pipeline)
		if err != nil {
			jptm.LogUploadError(info.Source, info.Destination, "Parent Directory Create Error "+err.Error(), 0)
			jptm.FailActiveUpload(err)
			return
		}

		// Create Azure file with the source size
		fileHTTPHeaders, metaData := jptm.FileDstData(au.leadingBytes)
		_, err = au.fileURL.Create(jptm.Context(), info.SourceSize, fileHTTPHeaders, metaData)
		if err != nil {
			status, msg := ErrorEx{err}.ErrorCodeAndString()
			jptm.LogUploadError(info.Source, info.Destination, "File Create Error "+msg, status)
			jptm.FailActiveUpload(err)
			return
		}
	})
}

func (au *azureFilesUploader) GenerateUploadFunc(id common.ChunkID, blockIndex int32, reader common.SingleChunkReader, chunkIsWholeFile bool) chunkFunc {

	return func(workerId int) {

		jptm := au.jptm

		defer jptm.ReportChunkDone() // whether successful or failed, it's always "done" and we must always tell the jptm

		jptm.OccupyAConnection() // TODO: added the two operations for debugging purpose. remove later
		defer jptm.ReleaseAConnection()

		// Ensure prologue has been run exactly once, before we do anything else
		au.runPrologueOnce()

		if au.jptm.Info().SourceSize == 0 {
			jptm.LogChunkStatus(id, common.EWaitReason.ChunkDone())
			// nothing more to do, since this is a dummy chunk in a zero-size file, and the prologue will have done all the real work
			return
		}

		if jptm.WasCanceled() {
			jptm.LogChunkStatus(id, common.EWaitReason.Cancelled())
			return
		}

		// upload the byte range represented by this chunk
		jptm.LogChunkStatus(id, common.EWaitReason.Body())
		body := newLiteRequestBodyPacer(reader, au.pacer)
		_, err := au.fileURL.UploadRange(jptm.Context(), id.OffsetInFile, body)
		if err != nil {
			jptm.FailActiveUploadWithDetails(err, "Upload range error", common.ETransferStatus.Failed())
			return
		}

		jptm.LogChunkStatus(id, common.EWaitReason.ChunkDone())
	}
}

func (au *azureFilesUploader) Epilogue() {
	jptm := au.jptm

	// Cleanup
	if jptm.TransferStatus() <= 0 {
		// If the transfer status is less than or equal to 0
		// then transfer was either failed or cancelled
		// the file created in share needs to be deleted, since it's
		// contents will be at an unknown stage of partial completeness
		deletionContext, _ := context.WithTimeout(context.Background(), 2*time.Minute)
		_, err := au.fileURL.Delete(deletionContext)
		if err != nil {
			if jptm.ShouldLog(pipeline.LogError) {
				jptm.Log(pipeline.LogInfo, fmt.Sprintf("error deleting the file %s. Failed with error %s", au.fileURL.String(), err.Error()))
			}
		}
	}

}

// getParentDirectoryURL gets parent directory URL of an Azure FileURL.
func getParentDirectoryURL(fileURL azfile.FileURL, p pipeline.Pipeline) azfile.DirectoryURL {
	u := fileURL.URL()
	u.Path = u.Path[:strings.LastIndex(u.Path, "/")]
	return azfile.NewDirectoryURL(u, p)
}

// verifyAndHandleCreateErrors handles create errors, StatusConflict is ignored, as specific level directory could be existing.
// Report http.StatusForbidden, as user should at least have read and write permission of the destination,
// and there is no permission on directory level, i.e. create directory is a general permission for each level diretories for Azure file.
func verifyAndHandleCreateErrors(err error) error {
	if err != nil {
		sErr := err.(azfile.StorageError)
		if sErr != nil && sErr.Response() != nil &&
			(sErr.Response().StatusCode == http.StatusConflict) { // Note the ServiceCode actually be AuthenticationFailure when share failed to be created, if want to create share as well.
			return nil
		}
		return err
	}

	return nil
}

// splitWithoutToken splits string with a given token, and returns splitted results without token.
func splitWithoutToken(str string, token rune) []string {
	return strings.FieldsFunc(str, func(c rune) bool {
		return c == token
	})
}

// createParentDirToRoot creates parent directories of the Azure file if file's parent directory doesn't exist.
func createParentDirToRoot(ctx context.Context, fileURL azfile.FileURL, p pipeline.Pipeline) error {
	dirURL := getParentDirectoryURL(fileURL, p)
	dirURLExtension := common.FileURLPartsExtension{FileURLParts: azfile.NewFileURLParts(dirURL.URL())}
	// Check whether parent dir of the file exists.
	if _, err := dirURL.GetProperties(ctx); err != nil {
		if err.(azfile.StorageError) != nil && (err.(azfile.StorageError)).Response() != nil &&
			(err.(azfile.StorageError).Response().StatusCode == http.StatusNotFound) { // At least need read and write permisson for destination
			// File's parent directory doesn't exist, try to create the parent directories.
			// Split directories as segments.
			segments := splitWithoutToken(dirURLExtension.DirectoryOrFilePath, '/')

			shareURL := azfile.NewShareURL(dirURLExtension.GetShareURL(), p)
			curDirURL := shareURL.NewRootDirectoryURL() // Share directory should already exist, doesn't support creating share
			// Try to create the directories
			for i := 0; i < len(segments); i++ {
				curDirURL = curDirURL.NewDirectoryURL(segments[i])
				_, err := curDirURL.Create(ctx, azfile.Metadata{})
				if verifiedErr := verifyAndHandleCreateErrors(err); verifiedErr != nil {
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
