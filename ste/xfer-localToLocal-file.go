package ste

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

func localToLocal(jptm IJobPartTransferMgr) {
	info := jptm.Info()

	switch info.EntityType {
	case common.EEntityType.Folder():
		localToLocal_folder(jptm)
	case common.EEntityType.FileProperties():
		//anyToRemote_fileProperties(jptm)
	case common.EEntityType.File():
		localToLocal_file(jptm)
	}
}

func localToLocal_file(jptm IJobPartTransferMgr) {
	// step 1. perform initial checks
	if jptm.WasCanceled() {
		/* This is the earliest we detect jptm has been cancelled before scheduling chunks */
		jptm.SetStatus(common.ETransferStatus.Cancelled())
		jptm.ReportTransferDone()
		return
	}
	info := jptm.Info()
	fileSize := int64(info.SourceSize)
	pseudoId := common.NewPseudoChunkIDForWholeFile(info.Source)
	jptm.LogChunkStatus(pseudoId, common.EWaitReason.XferStart())
	defer jptm.LogChunkStatus(pseudoId, common.EWaitReason.ChunkDone())

	src := info.Source
	//dst := info.Destination
	sourceFileStat, err := os.Stat(src)
	if err != nil {
		jptm.LogSendError(info.Source, info.Destination, "Cannot stat source File"+err.Error(), 0)
		jptm.SetStatus(common.ETransferStatus.Failed()) // is a real failure, not just a SkippedFileAlreadyExists, in this case
		jptm.ReportTransferDone()
		return
	}
	//  check overwrite option
	// if the force Write flags is set to false or prompt
	// then check the file exists at the location
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
				//	jptm.LogAtLevelForCurrentTransfer(pipeline.LogWarning, "File already exists, so will be skipped")
				jptm.SetStatus(common.ETransferStatus.SkippedEntityAlreadyExists())
				jptm.ReportTransferDone()
				return
			}
		}
	}

	// step 4a: mark destination as modified before we take our first action there (which is to create the destination file)
	jptm.SetDestinationIsModified()

	common.GetLifecycleMgr().E2EAwaitAllowOpenFiles()
	jptm.LogChunkStatus(pseudoId, common.EWaitReason.OpenLocalSource())

	if !sourceFileStat.Mode().IsRegular() {
		jptm.LogSendError(info.Source, info.Destination, "file is not regular file", 0)
		jptm.SetStatus(common.ETransferStatus.Failed())
		jptm.ReportTransferDone()
		return
	}
	//Although we are transfering the folders but we don't know whether file in that folder will be executed before or after.
	//As the enumeration is done using parllel workers
	//todo:creationTImeDownloader file create
	writeThrough := false
	var dstFilePtr *os.File

	// special handling for empty files
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
		epilogueWithRename(jptm, nil)
		return
	}

	// step 4c: normal file creation when source has content

	failFileCreation := func(err error) {
		jptm.LogDownloadError(info.Source, info.Destination, "File Creation Error "+err.Error(), 0)
		jptm.SetStatus(common.ETransferStatus.Failed())
	}
	// block until we can safely use a file handle
	err = jptm.WaitUntilLockDestination(jptm.Context())
	if err != nil {
		failFileCreation(err)
		return
	}

	if strings.EqualFold(info.Destination, common.Dev_Null) {
		// the user wants to discard the downloaded data
		//dstFile = devNullWriter{}
	} else {
		// Normal scenario, create the destination file as expected
		// Use pseudo chunk id to allow our usual state tracking mechanism to keep count of how many
		// file creations are running at any given instant, for perf diagnostics
		//
		// We create the file to a temporary location with name .azcopy-<jobID>-<actualName> and then move it
		// to correct name.
		pseudoId := common.NewPseudoChunkIDForWholeFile(info.Source)
		jptm.LogChunkStatus(pseudoId, common.EWaitReason.CreateLocalFile())
		dstFilePtr, err = createDestinationFile_return_filePtr(jptm, info.getDownloadPath(), fileSize, writeThrough)
		jptm.LogChunkStatus(pseudoId, common.EWaitReason.ChunkDone()) // normal setting to done doesn't apply to these pseudo ids
		if err != nil {
			failFileCreation(err)
			return
		}
	}
	// tell jptm what to expect, and how to clean up at the end
	var numChunks uint32 = 1
	jptm.SetNumberOfChunks(numChunks)
	jptm.SetActionAfterLastChunk(func() { epilogueWithRename(jptm, dstFilePtr) })

	body := func() {
		if info.SourceSize > 0 {
			_, err = copyFile(jptm.Context(), src, dstFilePtr)

			//	if destinationSize != jptm.Info().SourceSize || err != nil {
			// 	err1 := removeFile(dst)
			// 	if err1 != nil {
			// 		//gives error when destination file not exist means that
			// 		//context was cancelled before we created the file
			// 		jptm.LogSendError(info.Source, info.Destination, "Transfer got failed", 0)
			// 		jptm.SetStatus(common.ETransferStatus.Failed())
			// 		jptm.ReportTransferDone()
			// 		return
			// 	}
			// 	jptm.LogSendError(info.Source, info.Destination, "CleanUp Successful and transfer got failed", 0) //cleanup was successful
			// 	jptm.SetStatus(common.ETransferStatus.Failed())
			// 	jptm.ReportTransferDone()
			// 	return
			//}
		}
		// jptm.SetStatus(common.ETransferStatus.Success()) // is a real failure, not just a SkippedFileAlreadyExists, in this case
		// jptm.ReportTransferDone()
	}
	cf := createChunkFunc(true, jptm, common.NewChunkID(src, 0, info.SourceSize), body)
	jptm.ScheduleChunks(cf)

}

type readerCtx struct {
	ctx context.Context
	r   io.Reader
}

func (r *readerCtx) Read(p []byte) (n int, err error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.r.Read(p)
}
func NewReader(ctx context.Context, r io.Reader) io.Reader {
	return &readerCtx{
		ctx: ctx,
		r:   r,
	}
}
func copyFile(ctx context.Context, src string, dstFilePtr *os.File) (written int64, err error) {
	source, err := os.Open(src)
	if err != nil {
		return 0, err
	}
	defer source.Close()
	return io.Copy(dstFilePtr, NewReader(ctx, source))
}

func removeFile(dst string) error {
	err := os.Remove(dst)
	return err
}

func createDestinationFile_return_filePtr(jptm IJobPartTransferMgr, destination string, size int64, writeThrough bool) (file *os.File, err error) {
	// ct := common.ECompressionType.None()
	// if jptm.ShouldDecompress() {
	// 	size = 0                                  // we don't know what the final size will be, so we can't pre-size it
	// 	ct, err = jptm.GetSourceCompressionType() // calls same decompression getter routine as the front-end does
	// 	if err != nil {                           // check this, and return error, before we create any disk file, since if we return err, then no cleanup of file will be required
	// 		return nil, err
	// 	}
	// 	// Why get the decompression type again here, when we already looked at it at enumeration time?
	// 	// Because we have better ability to report unsupported compression types here, with clear "transfer failed" handling,
	// 	// and we still need to set size to zero here, so relying on enumeration more wouldn't simply this code much, if at all.
	// }

	dstFile, err := common.CreateFileOfSizeWithWriteThroughOption(destination, size, writeThrough, jptm.GetFolderCreationTracker(), jptm.GetForceIfReadOnly())
	if err != nil {
		return nil, err
	}
	//TODO: check for decompress
	// if jptm.ShouldDecompress() {
	// 	jptm.LogAtLevelForCurrentTransfer(pipeline.LogInfo, "will be decompressed from "+ct.String())

	// 	// wrap for automatic decompression
	// 	dstFile = common.NewDecompressingWriter(dstFile, ct)
	// 	// why don't we just let Go's network stack automatically decompress for us? Because
	// 	// 1. Then we can't check the MD5 hash (since logically, any stored hash should be the hash of the file that exists in Storage, i.e. the compressed one)
	// 	// 2. Then we can't pre-plan a certain number of fixed-size chunks (which is required by the way our architecture currently works).
	// }
	return dstFile, nil
}

func epilogueWithRename(jptm IJobPartTransferMgr, activeDstFile *os.File) {
	info := jptm.Info()

	// allow our usual state tracking mechanism to keep count of how many epilogues are running at any given instant, for perf diagnostics
	pseudoId := common.NewPseudoChunkIDForWholeFile(info.Source)
	jptm.LogChunkStatus(pseudoId, common.EWaitReason.Epilogue())
	defer jptm.LogChunkStatus(pseudoId, common.EWaitReason.ChunkDone()) // normal setting to done doesn't apply to these pseudo ids

	if jptm.WasCanceled() {
		// This is where we first realize that the transfer is cancelled. Further statements are no-op and
		jptm.SetStatus(common.ETransferStatus.Cancelled())
	}
	haveNonEmptyFile := activeDstFile != nil
	if haveNonEmptyFile {

		closeErr := activeDstFile.Close() // always try to close if, even if flush failed

		if closeErr != nil {
			jptm.FailActiveDownload("Closing file", closeErr)
		}

		// Check MD5 (but only if file was fully flushed and saved - else no point and may not have actualAsSaved hash anyway)
		if jptm.IsLive() {
			// comparison := md5Comparer{
			// 	expected:         info.SrcHTTPHeaders.ContentMD5, // the MD5 that came back from Service when we enumerated the source
			// 	actualAsSaved:    md5OfFileAsWritten,
			// 	validationOption: jptm.MD5ValidationOption(),
			// 	logger:           jptm}
			// err := comparison.Check()
			// if err != nil {
			// 	jptm.FailActiveDownload("Checking MD5 hash", err)
			// }

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
			// downloaded and not corrupt. In fact, post this point we should only log errors and
			// not fail the transfer.
			renameNecessary := !strings.EqualFold(info.getDownloadPath(), info.Destination) &&
				!strings.EqualFold(info.Destination, common.Dev_Null)
			if renameNecessary {
				renameErr := os.Rename(info.getDownloadPath(), info.Destination)
				if renameErr != nil {
					jptm.LogError(info.Destination, fmt.Sprintf(
						"Failed to rename. File at %s", info.getDownloadPath()), renameErr)
				}
			}
		}
	}

	// Preserve modified time
	if jptm.IsLive() {
		// TODO: the old version of this code did NOT consider it an error to be unable to set the modification date/time
		// TODO: ...So I have preserved that behavior here.
		// TODO: question: But is that correct?
		lastModifiedTime, preserveLastModifiedTime := jptm.PreserveLastModifiedTime()
		if preserveLastModifiedTime && !info.PreserveSMBInfo {
			err := os.Chtimes(jptm.Info().Destination, lastModifiedTime, lastModifiedTime)
			if err != nil {
				jptm.LogError(info.Destination, "Changing Modified Time ", err)
				// do NOT return, since final status and cleanup logging still to come
			} else {
				//successfully modified last modified time
				//jptm.Log(pipeline.LogInfo, fmt.Sprintf(" Preserved Modified Time for %s", info.Destination))
			}
		}
	}
	// We know that file(1chunk) has completed transfer (because this routine was called)
	// and we know the transfer didn't fail (because just checked its status above),
	// so it must have succeeded. So make sure its not left "in progress" state
	jptm.SetStatus(common.ETransferStatus.Success())

	// must always do this, and do it last
	jptm.EnsureDestinationUnlocked()

	// successful or unsuccessful, it's definitely over
	jptm.ReportTransferDone()

}
