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
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

const azcopyTempDownloadPrefix string = ".azDownload-%s-"

// xfer.go requires just a single xfer function for the whole job.
// This routine serves that role for downloads and redirects for each transfer to a file or folder implementation
func remoteToLocal(jptm IJobPartTransferMgr, pacer pacer, df downloaderFactory) {
	info := jptm.Info()
	if info.IsFolderPropertiesTransfer() {
		remoteToLocal_folder(jptm, pacer, df)
	} else if info.EntityType == common.EEntityType.Symlink() {
		remoteToLocal_symlink(jptm, pacer, df)
	} else {
		remoteToLocal_file(jptm, pacer, df)
	}
}

// general-purpose "any remote persistence location" to local, for files
func remoteToLocal_file(jptm IJobPartTransferMgr, pacer pacer, df downloaderFactory) {

	info := jptm.Info()

	// step 1: create downloader instance for this transfer
	// We are using a separate instance per transfer, in case some implementations need to hold per-transfer state
	dl, err := df(jptm)
	if err != nil {
		jptm.SetStatus(common.ETransferStatus.Failed())
		jptm.ReportTransferDone()
		return
	}

	// step 2: get the source, destination info for the transfer.
	fileSize := int64(info.SourceSize)
	downloadChunkSize := info.BlockSize

	// step 3: Perform initial checks
	// If the transfer was cancelled, then report transfer as done
	// TODO Question: the above comment had this following text too: "and increasing the bytestransferred by the size of the source." what does it mean?
	if jptm.WasCanceled() {
		/* This is the earliest we detect that jptm has been cancelled before we schedule chunks */
		jptm.SetStatus(common.ETransferStatus.Cancelled())
		jptm.ReportTransferDone()
		return
	}
	// if the force Write flags is set to false or prompt
	// then check the file exists at the remote location
	// if it does, react accordingly
	if jptm.GetOverwriteOption() != common.EOverwriteOption.True() {
		dstProps, err := common.OSStat(info.Destination)
		if err == nil {
			// if the error is nil, then file exists locally
			shouldOverwrite := false

			// if necessary, prompt to confirm user's intent
			if jptm.GetOverwriteOption() == common.EOverwriteOption.Prompt() {
				shouldOverwrite = jptm.GetOverwritePrompter().ShouldOverwrite(info.Destination, common.EEntityType.File())
			} else if jptm.GetOverwriteOption() == common.EOverwriteOption.IfSourceNewer() {
				// only overwrite if source lmt is newer (after) the destination
				if jptm.LastModifiedTime().After(dstProps.ModTime()) {
					shouldOverwrite = true
				}
			}

			if !shouldOverwrite {
				// logging as Warning so that it turns up even in compact logs, and because previously we use Error here
				jptm.LogAtLevelForCurrentTransfer(common.LogWarning, "File already exists, so will be skipped")
				jptm.SetStatus(common.ETransferStatus.SkippedEntityAlreadyExists())
				jptm.ReportTransferDone()
				return
			}
		}
	}

	if jptm.MD5ValidationOption() == common.EHashValidationOption.FailIfDifferentOrMissing() {
		// We can make a check early on MD5 existence and fail the transfer if it's not present.
		// This will save hours in the event a user has say, a several hundred gigabyte file.
		if len(info.SrcHTTPHeaders.ContentMD5) == 0 {
			jptm.LogDownloadError(info.Source, info.Destination, errExpectedMd5Missing.Error(), 0)
			jptm.SetStatus(common.ETransferStatus.Failed())
			jptm.ReportTransferDone()
			return
		}
	}

	// step 4a: mark destination as modified before we take our first action there (which is to create the destination file)
	jptm.SetDestinationIsModified()

	writeThrough := false
	// TODO: consider cases where we might set it to true. It might give more predictable and understandable disk throughput.
	//    But can't be used in the cases shown in the if statement below (one of which is only pseudocode, at this stage)
	//      if fileSize <= 1*1024*1024 || jptm.JobHasLowFileCount() || <is a short-running job> {
	//        // but, for very small files, testing indicates that we can need it in at least some cases. (Presumably just can't get enough queue depth to physical disk without it.)
	//        // And also, for very low file counts, we also need it. Presumably for same reasons of queue depth (given our sequential write strategy as at March 2019)
	//        // And for very short-running jobs, it looks and feels faster for the user to just let the OS cache flush out after the job appears to have finished.
	//        writeThrough = false
	//    }

	var dstFile io.WriteCloser
	if ctdl, ok := dl.(creationTimeDownloader); info.Destination != os.DevNull && ok { // ctdl never needs to handle devnull
		failFileCreation := func(err error) {
			jptm.LogDownloadError(info.Source, info.Destination, "File Creation Error "+err.Error(), 0)
			jptm.SetStatus(common.ETransferStatus.Failed())
			// use standard epilogue for consistency, but force release of file count (without an actual file) if necessary
			epilogueWithCleanupDownload(jptm, dl, nil, nil)
		}
		// block until we can safely use a file handle
		err := jptm.WaitUntilLockDestination(jptm.Context())
		if err != nil {
			failFileCreation(err)
			return
		}

		size := fileSize
		ct := common.ECompressionType.None()
		if jptm.ShouldDecompress() {
			size = 0                                  // we don't know what the final size will be, so we can't pre-size it
			ct, err = jptm.GetSourceCompressionType() // calls same decompression getter routine as the front-end does
			if err != nil {                           // check this, and return error, before we create any disk file, since if we return err, then no cleanup of file will be required
				failFileCreation(err)
				return
			}
			// Why get the decompression type again here, when we already looked at it at enumeration time?
			// Because we have better ability to report unsupported compression types here, with clear "transfer failed" handling,
			// and we still need to set size to zero here, so relying on enumeration more wouldn't simply this code much, if at all.
		}

		// Normal scenario, create the destination file as expected
		// Use pseudo chunk id to allow our usual state tracking mechanism to keep count of how many
		// file creations are running at any given instant, for perf diagnostics
		//
		// We create the file to a temporary location with name .azcopy-<jobID>-<actualName> and then move it
		// to correct name.
		pseudoId := common.NewPseudoChunkIDForWholeFile(info.Source)
		jptm.LogChunkStatus(pseudoId, common.EWaitReason.CreateLocalFile())
		var needChunks bool
		dstFile, needChunks, err = ctdl.CreateFile(jptm, info.getDownloadPath(), size, writeThrough, jptm.GetFolderCreationTracker())
		jptm.LogChunkStatus(pseudoId, common.EWaitReason.ChunkDone()) // normal setting to done doesn't apply to these pseudo ids
		if err != nil {
			failFileCreation(err)
			return
		}

		if !needChunks { // If no chunks need to be transferred (e.g. this is 0-bytes long, a symlink, etc.), treat it as a 0-byte transfer
			dl.Prologue(jptm)
			epilogueWithCleanupDownload(jptm, dl, nil, nil)
			return
		}

		if jptm.ShouldDecompress() { // Wrap the file in the decompressor if necessary
			jptm.LogAtLevelForCurrentTransfer(common.LogInfo, "will be decompressed from "+ct.String())

			// wrap for automatic decompression
			dstFile = common.NewDecompressingWriter(dstFile, ct)
			// why don't we just let Go's network stack automatically decompress for us? Because
			// 1. Then we can't check the MD5 hash (since logically, any stored hash should be the hash of the file that exists in Storage, i.e. the compressed one)
			// 2. Then we can't pre-plan a certain number of fixed-size chunks (which is required by the way our architecture currently works).
		}
	} else {
		// step 4b: special handling for empty files
		if fileSize == 0 {
			if strings.EqualFold(info.Destination, common.Dev_Null) {
				// do nothing
			} else {
				err := jptm.WaitUntilLockDestination(jptm.Context())
				if err == nil {
					err = createEmptyFile(jptm, info.Destination)
				}
				if err != nil {
					jptm.LogDownloadError(info.Source, info.Destination, "Empty File Creation error "+err.Error(), 0)
					jptm.SetStatus(common.ETransferStatus.Failed())
				}
			}
			// Run the prologue anyway, as some downloaders (files) require this.
			// Note that this doesn't actually have adverse effects (at the moment).
			// For files, it just sets a few properties.
			// For blobs, it sets up a page blob pacer if it's a page blob.
			// For blobFS, it's a noop.
			dl.Prologue(jptm)
			epilogueWithCleanupDownload(jptm, dl, nil, nil) // need standard epilogue, rather than a quick exit, so we can preserve modification dates
			return
		}

		// step 4c: normal file creation when source has content

		failFileCreation := func(err error) {
			jptm.LogDownloadError(info.Source, info.Destination, "File Creation Error "+err.Error(), 0)
			jptm.SetStatus(common.ETransferStatus.Failed())
			// use standard epilogue for consistency, but force release of file count (without an actual file) if necessary
			epilogueWithCleanupDownload(jptm, dl, nil, nil)
		}
		// block until we can safely use a file handle
		err := jptm.WaitUntilLockDestination(jptm.Context())
		if err != nil {
			failFileCreation(err)
			return
		}

		if strings.EqualFold(info.Destination, common.Dev_Null) {
			// the user wants to discard the downloaded data
			dstFile = devNullWriter{}
		} else {
			// Normal scenario, create the destination file as expected
			// Use pseudo chunk id to allow our usual state tracking mechanism to keep count of how many
			// file creations are running at any given instant, for perf diagnostics
			//
			// We create the file to a temporary location with name .azcopy-<jobID>-<actualName> and then move it
			// to correct name.
			pseudoId := common.NewPseudoChunkIDForWholeFile(info.Source)
			jptm.LogChunkStatus(pseudoId, common.EWaitReason.CreateLocalFile())
			dstFile, err = createDestinationFile(jptm, info.getDownloadPath(), fileSize, writeThrough)
			jptm.LogChunkStatus(pseudoId, common.EWaitReason.ChunkDone()) // normal setting to done doesn't apply to these pseudo ids
			if err != nil {
				failFileCreation(err)
				return
			}
		}
	}

	// TODO: Question: do we need to Stat the file, to check its size, after explicitly making it with the desired size?
	// That was what the old xfer-blobToLocal code used to do
	// I've commented it out to be more concise, but we'll put it back if someone knows why it needs to be here
	/*
		dstFileInfo, err := dstFile.Stat()
		if err != nil || (dstFileInfo.Size() != blobSize) {
			jptm.LogDownloadError(info.Source, info.Destination, "File Creation Error "+err.Error(), 0)
			jptm.SetStatus(common.ETransferStatus.Failed())
			// Since the transfer failed, the file created above should be deleted
			// If there was an error while opening / creating the file, delete will fail.
			// But delete is required when error occurred while truncating the file and
			// in this case file should be deleted.
			tryDeleteFile(info, jptm)
			jptm.ReportTransferDone()
			return
		}*/

	// step 5a: compute num chunks
	numChunks := uint32(0)
	if rem := fileSize % downloadChunkSize; rem == 0 {
		numChunks = uint32(fileSize / downloadChunkSize)
	} else {
		numChunks = uint32(fileSize/downloadChunkSize + 1)
	}

	// step 5b: create destination writer
	chunkLogger := jptm.ChunkStatusLogger()
	sourceMd5Exists := len(info.SrcHTTPHeaders.ContentMD5) > 0
	dstWriter := common.NewChunkedFileWriter(
		jptm.Context(),
		jptm.SlicePool(),
		jptm.CacheLimiter(),
		chunkLogger,
		dstFile,
		numChunks,
		MaxRetryPerDownloadBody,
		jptm.MD5ValidationOption(),
		sourceMd5Exists)

	// step 5c: run prologue in downloader (here it can, for example, create things that will require cleanup in the epilogue)
	common.GetLifecycleMgr().E2EAwaitAllowOpenFiles()
	dl.Prologue(jptm)

	// step 5d: tell jptm what to expect, and how to clean up at the end
	jptm.SetNumberOfChunks(numChunks)
	jptm.SetActionAfterLastChunk(func() { epilogueWithCleanupDownload(jptm, dl, dstFile, dstWriter) })

	// step 6: go through the blob range and schedule download chunk jobs
	// TODO: currently, the epilogue will only run if the number of completed chunks = numChunks.
	//     ...which means that we can't exit this loop early, if there is a cancellation or failure. Instead we
	//     ...must schedule the expected number of chunks, i.e. schedule all of them even if the transfer is already failed,
	//     ...so that the last of them will trigger the epilogue.
	//     ...Question: is that OK?
	// DECISION: 16 Jan, 2019: for now, we are leaving in place the above rule than number of of completed chunks must
	// eventually reach numChunks, since we have no better short-term alternative.

	chunkCount := uint32(0)
	for startIndex := int64(0); startIndex < fileSize; startIndex += downloadChunkSize {
		adjustedChunkSize := downloadChunkSize

		// compute exact size of the chunk
		if startIndex+downloadChunkSize > fileSize {
			adjustedChunkSize = fileSize - startIndex
		}

		id := common.NewChunkID(info.Destination, startIndex, adjustedChunkSize) // TODO: stop using adjustedChunkSize, below, and use the size that's in the ID

		// Wait until its OK to schedule it
		// To prevent excessive RAM consumption, we have a limit on the amount of scheduled-but-not-yet-saved data
		// TODO: as per comment above, currently, if there's an error here we must continue because we must schedule all chunks
		// TODO: ... Can we refactor/improve that?
		_ = dstWriter.WaitToScheduleChunk(jptm.Context(), id, adjustedChunkSize)

		// create download func that is a appropriate to the remote data source
		downloadFunc := dl.GenerateDownloadFunc(jptm, dstWriter, id, adjustedChunkSize, pacer)

		// schedule the download chunk job
		jptm.ScheduleChunks(downloadFunc)
		chunkCount++

		jptm.LogChunkStatus(id, common.EWaitReason.WorkerGR())
	}

	// sanity check to verify the number of chunks scheduled
	if chunkCount != numChunks {
		panic(fmt.Errorf("difference in the number of chunk calculated %v and actual chunks scheduled %v for src %s of size %v", numChunks, chunkCount, info.Source, fileSize))
	}

}

func createDestinationFile(jptm IJobPartTransferMgr, destination string, size int64, writeThrough bool) (file io.WriteCloser, err error) {
	ct := common.ECompressionType.None()
	if jptm.ShouldDecompress() {
		size = 0                                  // we don't know what the final size will be, so we can't pre-size it
		ct, err = jptm.GetSourceCompressionType() // calls same decompression getter routine as the front-end does
		if err != nil {                           // check this, and return error, before we create any disk file, since if we return err, then no cleanup of file will be required
			return nil, err
		}
		// Why get the decompression type again here, when we already looked at it at enumeration time?
		// Because we have better ability to report unsupported compression types here, with clear "transfer failed" handling,
		// and we still need to set size to zero here, so relying on enumeration more wouldn't simply this code much, if at all.
	}

	var dstFile io.WriteCloser
	dstFile, err = common.CreateFileOfSizeWithWriteThroughOption(destination, size, writeThrough, jptm.GetFolderCreationTracker(), jptm.GetForceIfReadOnly())
	if err != nil {
		return nil, err
	}
	if jptm.ShouldDecompress() {
		jptm.LogAtLevelForCurrentTransfer(common.LogInfo, "will be decompressed from "+ct.String())

		// wrap for automatic decompression
		dstFile = common.NewDecompressingWriter(dstFile, ct)
		// why don't we just let Go's network stack automatically decompress for us? Because
		// 1. Then we can't check the MD5 hash (since logically, any stored hash should be the hash of the file that exists in Storage, i.e. the compressed one)
		// 2. Then we can't pre-plan a certain number of fixed-size chunks (which is required by the way our architecture currently works).
	}
	return dstFile, nil
}

// complete epilogue. Handles both success and failure
func epilogueWithCleanupDownload(jptm IJobPartTransferMgr, dl downloader, activeDstFile io.WriteCloser, cw common.ChunkedFileWriter) {
	info := jptm.Info()

	// allow our usual state tracking mechanism to keep count of how many epilogues are running at any given instant, for perf diagnostics
	pseudoId := common.NewPseudoChunkIDForWholeFile(info.Source)
	jptm.LogChunkStatus(pseudoId, common.EWaitReason.Epilogue())
	defer jptm.LogChunkStatus(pseudoId, common.EWaitReason.ChunkDone()) // normal setting to done doesn't apply to these pseudo ids

	if jptm.WasCanceled() {
		// This is where we first realize that the transfer is cancelled. Further statements are no-op and
		// do not set any transfer status because they all of them verify that jptm is live.
		jptm.SetStatus(common.ETransferStatus.Cancelled())
	}

	haveNonEmptyFile := activeDstFile != nil
	if haveNonEmptyFile {

		// wait until all received chunks are flushed out
		md5OfFileAsWritten, flushError := cw.Flush(jptm.Context())
		closeErr := activeDstFile.Close() // always try to close if, even if flush failed
		if flushError != nil {
			jptm.FailActiveDownload("Flushing file", flushError)
		}
		if closeErr != nil {
			jptm.FailActiveDownload("Closing file", closeErr)
			jptm.LogAtLevelForCurrentTransfer(common.LogInfo, "Error closing file: "+closeErr.Error()) // log this way so that this line will be logged even if transfer is already failed
		}

		// Check MD5 (but only if file was fully flushed and saved - else no point and may not have actualAsSaved hash anyway)
		if jptm.IsLive() {
			comparison := md5Comparer{
				expected:         info.SrcHTTPHeaders.ContentMD5, // the MD5 that came back from Service when we enumerated the source
				actualAsSaved:    md5OfFileAsWritten,
				validationOption: jptm.MD5ValidationOption(),
				logger:           jptm}
			err := comparison.Check()
			if err != nil {
				jptm.FailActiveDownload("Checking MD5 hash", err)
			}

			// check length if enabled (except for dev null and decompression case, where that's impossible)
			if info.DestLengthValidation && info.Destination != common.Dev_Null && !jptm.ShouldDecompress() {
				fi, err := common.OSStat(info.getDownloadPath())

				if err != nil {
					jptm.FailActiveDownload("Download length check", err)
				} else if fi.Size() != info.SourceSize {
					jptm.FailActiveDownload("Download length check", errors.New("destination length did not match source length"))
				}
			}

			// check if we need to rename back to original name. At this point, we're sure the file is completely
			// downloaded and not corrupt.
			renameNecessary := !strings.EqualFold(info.getDownloadPath(), info.Destination) &&
				!strings.EqualFold(info.Destination, common.Dev_Null)
			if err == nil && renameNecessary {
				renameErr := os.Rename(info.getDownloadPath(), info.Destination)
				if renameErr != nil {
					jptm.FailActiveDownload("Download rename", renameErr)
				}
			}
		}
	}

	if dl != nil {
		// TODO: should we refactor to force this to accept jptm isLive as a parameter, to encourage it to be checked?
		//  or should we redefine epilogue to be success-path only, and only call it in that case?
		dl.Epilogue() // it can release resources here
	}

	// Preserve modified time
	if jptm.IsLive() {
		// TODO: the old version of this code did NOT consider it an error to be unable to set the modification date/time
		// TODO: ...So I have preserved that behavior here.
		// TODO: question: But is that correct?
		lastModifiedTime, preserveLastModifiedTime := jptm.PreserveLastModifiedTime()
		if preserveLastModifiedTime && !info.PreserveInfo {
			err := os.Chtimes(jptm.Info().Destination, lastModifiedTime, lastModifiedTime)
			if err != nil {
				jptm.LogError(info.Destination, "Changing Modified Time ", err)
				// do NOT return, since final status and cleanup logging still to come
			} else {
				jptm.Log(common.LogInfo, fmt.Sprintf(" Preserved Modified Time for %s", info.Destination))
			}
		}
	}

	commonDownloaderCompletion(jptm, info, common.EEntityType.File())
}

func commonDownloaderCompletion(jptm IJobPartTransferMgr, info *TransferInfo, entityType common.EntityType) {
redoCompletion:
	// note that we do not really know whether the context was canceled because of an error, or because the user asked for it
	// if was an intentional cancel, the status is still "in progress", so we are still counting it as pending
	// we leave these transfer status alone
	// in case of errors, the status was already set, so we don't need to do anything here either
	if jptm.IsDeadInflight() || jptm.IsDeadBeforeStart() {
		// If failed, log and delete the "bad" local file
		// If the current transfer status value is less than or equal to 0
		// then transfer either failed or was cancelled
		if jptm.ShouldLog(common.LogDebug) {
			jptm.Log(common.LogDebug, " Finalizing Transfer Cancellation/Failure")
		}
		// for files only, cleanup local file if applicable
		if entityType == entityType.File() && jptm.IsDeadInflight() && jptm.HoldsDestinationLock() {
			jptm.LogAtLevelForCurrentTransfer(common.LogInfo, "Deleting incomplete destination file")

			// the file created locally should be deleted
			tryDeleteFile(info, jptm)
		}
	} else {
		if !jptm.IsLive() {
			panic("reached branch where jptm is assumed to be live, but it isn't")
		}

		// Attempt to put MD5 data if necessary, compliant with the sync hash scheme
		if jptm.ShouldPutMd5() {
			fi, err := os.Stat(info.Destination)
			if err != nil {
				jptm.FailActiveDownload("saving MD5 data (stat to pull LMT)", err)
				goto redoCompletion // let fail as expected
			}

			plan := jptm.(*jobPartTransferMgr).jobPartMgr.Plan()
			dataRoot := string(plan.DestinationRoot[:plan.DestinationRootLength])
			_, idx := jptm.TransferIndex()
			_, relDest := plan.TransferSrcDstRelatives(idx)
			adapter, err := common.NewHashDataAdapter(common.LocalHashDir, dataRoot, common.LocalHashStorageMode)
			if err != nil {
				jptm.FailActiveDownload("initializing hash adapter", err)
			}
			err = adapter.SetHashData(relDest, &common.SyncHashData{
				Mode: common.ESyncHashType.MD5(),
				LMT:  fi.ModTime(),
				Data: base64.StdEncoding.EncodeToString(info.SrcHTTPHeaders.ContentMD5),
			})
			if err != nil {
				jptm.FailActiveDownload("saving MD5 data (writing alternate data stream)", err)
				goto redoCompletion // let fail as expected
			}
		}

		// We know all chunks are done (because this routine was called)
		// and we know the transfer didn't fail (because just checked its status above),
		// so it must have succeeded. So make sure its not left "in progress" state
		jptm.SetStatus(common.ETransferStatus.Success())

		// Final logging
		if jptm.ShouldLog(common.LogInfo) { // TODO: question: can we remove these ShouldLogs?  Aren't they inside Log?
			jptm.Log(common.LogInfo, fmt.Sprintf("DOWNLOADSUCCESSFUL: %s%s", info.entityTypeLogIndicator(), info.Destination))
		}
		if jptm.ShouldLog(common.LogDebug) {
			jptm.Log(common.LogDebug, "Finalizing Transfer")
		}
	}

	// must always do this, and do it last
	jptm.EnsureDestinationUnlocked()

	// successful or unsuccessful, it's definitely over
	jptm.ReportTransferDone()
}

// create an empty file and its parent directories, without any content
func createEmptyFile(jptm IJobPartTransferMgr, destinationPath string) error {
	err := common.CreateParentDirectoryIfNotExist(destinationPath, jptm.GetFolderCreationTracker())
	if err != nil {
		return err
	}
	f, err := common.OSOpenFile(destinationPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, common.DEFAULT_FILE_PERM)
	if err != nil {
		return err
	}
	_ = f.Close()
	return nil
}

// deletes the file
func deleteFile(destinationPath string) error {
	return os.Remove(destinationPath)
}

// tries to delete file, but if that fails just logs and returns
func tryDeleteFile(info *TransferInfo, jptm IJobPartTransferMgr) {
	// skip deleting if we are targeting dev null and throwing away the data
	if strings.EqualFold(common.Dev_Null, info.Destination) {
		return
	}

	err := deleteFile(info.getDownloadPath())
	if err != nil {
		// If there was an error deleting the file, log the error
		jptm.LogError(info.Destination, "Delete File Error ", err)
	}
}

// Returns the path of file to be downloaded. If we want to
// download to a temp path we return a temp path in format
// /actual/parent/path/.azDownload-<jobID>-<actualFileName>
func (info *TransferInfo) getDownloadPath() string {
	downloadToTempPath, err := strconv.ParseBool(common.GetEnvironmentVariable(common.EEnvironmentVariable.DownloadToTempPath()))
	if err != nil {
		// By default, we'll download to temp path
		downloadToTempPath = true
	}

	if downloadToTempPath && info.SourceSize > 0 { // 0-byte files don't need a rename.
		parent, fileName := filepath.Split(info.Destination)
		fileName = fmt.Sprintf(azcopyTempDownloadPrefix, info.JobID.String()) + fileName
		tempPath := filepath.Join(parent, fileName)
		// Ensure temp path also uses extended path format on Windows for long paths
		return common.ToExtendedPath(tempPath)
	}
	return info.Destination
}

// conforms to io.Writer and io.Closer
// does absolutely nothing to discard the given data
type devNullWriter struct{}

func (devNullWriter) Write(p []byte) (n int, err error) {
	return len(p), nil
}

func (devNullWriter) Close() error {
	return nil
}
