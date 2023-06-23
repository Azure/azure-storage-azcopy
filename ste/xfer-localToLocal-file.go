package ste

import (
	"context"
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
	case common.EEntityType.File():
		localToLocal_file(jptm)
	}
}

func localToLocal_file(jptm IJobPartTransferMgr) {
	info := jptm.Info()
	//step 1: Get the source Info
	fileSize := int64(info.SourceSize)
	src := info.Source

	// step 2: perform initial checks
	if jptm.WasCanceled() {
		/* This is the earliest we detect jptm has been cancelled before scheduling chunks */
		jptm.SetStatus(common.ETransferStatus.Cancelled())
		jptm.ReportTransferDone()
		return
	}
	pseudoId := common.NewPseudoChunkIDForWholeFile(info.Source)
	jptm.LogChunkStatus(pseudoId, common.EWaitReason.XferStart())
	defer jptm.LogChunkStatus(pseudoId, common.EWaitReason.ChunkDone())

	//step 3: Get the source file info
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
	//step 4a:
	//mark destination as modified before we take our first action there (which is to create the destination file)
	jptm.SetDestinationIsModified()

	common.GetLifecycleMgr().E2EAwaitAllowOpenFiles()
	jptm.LogChunkStatus(pseudoId, common.EWaitReason.OpenLocalSource())

	//step 4b: check the file is regular or not to modify
	if !sourceFileStat.Mode().IsRegular() {
		jptm.LogSendError(info.Source, info.Destination, "file is not regular file", 0)
		jptm.SetStatus(common.ETransferStatus.Failed())
		jptm.ReportTransferDone()
		return
	}

	//step5 : Create file for the destination
	//Although we are transfering the folders but we don't know whether file in that folder will be executed before or after.
	//As the enumeration is done using parllel workers
	writeThrough := false
	var dstFile io.WriteCloser

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

	//normal file creation when source has content

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
		// the user wants to discard the Copy data
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
		dstFile, err = common.CreateFileOfSizeWithWriteThroughOption(info.getDownloadPath(), fileSize, writeThrough, jptm.GetFolderCreationTracker(), jptm.GetForceIfReadOnly())
		jptm.LogChunkStatus(pseudoId, common.EWaitReason.ChunkDone()) // normal setting to done doesn't apply to these pseudo ids
		if err != nil {
			failFileCreation(err)
			return
		}
	}
	//step 6: Initialize the no of chunks as 1, as we are transfering the entire file using 1 chunk processor thread.
	// tell jptm what to expect, and how to clean up at the end
	var numChunks uint32 = 1
	jptm.SetNumberOfChunks(numChunks)
	jptm.SetActionAfterLastChunk(func() { epilogueWithRename(jptm, dstFile) })

	//step 7: Create the body for the chunk function
	body := func() {
		if info.SourceSize > 0 {
			bytesWritten, err := copyFile(jptm.Context(), src, dstFile)
			if err != nil || bytesWritten != fileSize {
				//transfer is not successful
				err = removeFile(info.getDownloadPath())
				if err != nil {
					jptm.LogSendError(info.Source, info.Destination, "Destination File cannot be deleted"+err.Error(), 0)
				}
				jptm.SetStatus(common.ETransferStatus.Failed())
				jptm.ReportTransferDone()
			}
		}
	}
	//step 8: Create chunk function and schedule it.
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
func copyFile(ctx context.Context, src string, dstFile io.WriteCloser) (written int64, err error) {
	source, err := os.Open(src)
	if err != nil {
		return 0, err
	}
	defer source.Close()
	return io.Copy(dstFile, NewReader(ctx, source))
}

func removeFile(dst string) error {
	err := os.Remove(dst)
	return err
}

func epilogueWithRename(jptm IJobPartTransferMgr, activeDstFile io.WriteCloser) {
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

		if jptm.IsLive() {
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
		lastModifiedTime, preserveLastModifiedTime := jptm.PreserveLastModifiedTime()
		if preserveLastModifiedTime && !info.PreserveSMBInfo {
			err := os.Chtimes(jptm.Info().Destination, lastModifiedTime, lastModifiedTime)
			if err != nil {
				jptm.LogError(info.Destination, "Changing Modified Time ", err)
				// do NOT return, since final status and cleanup logging still to come
			} else {
				//successfully modified last modified time
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
