package ste

import (
	"context"
	"io"
	"os"
	"path/filepath"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

func localToLocal(jptm IJobPartTransferMgr) {
	localToLocal_file(jptm)
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
	pseudoId := common.NewPseudoChunkIDForWholeFile(info.Source)
	jptm.LogChunkStatus(pseudoId, common.EWaitReason.XferStart())
	defer jptm.LogChunkStatus(pseudoId, common.EWaitReason.ChunkDone())

	src := info.Source
	dst := info.Destination
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
	common.GetLifecycleMgr().E2EAwaitAllowOpenFiles()
	jptm.LogChunkStatus(pseudoId, common.EWaitReason.OpenLocalSource())

	if !sourceFileStat.Mode().IsRegular() {
		jptm.LogSendError(info.Source, info.Destination, "file is not regular file", 0)
		jptm.SetStatus(common.ETransferStatus.Failed())
		jptm.ReportTransferDone()
		return
	}

	if _, err := os.Stat(dst); os.IsNotExist(err) {
		os.MkdirAll(filepath.Dir(dst), 0700) // Create your file
	}

	body := func() {
		destinationSize, err := copyFile(jptm.Context(), src, dst)

		if destinationSize != jptm.Info().SourceSize || err != nil {
			err1 := removeFile(dst)
			if err1 != nil {
				//gives error when destination file not exist means that
				//context was cancelled before we created the file
				jptm.LogSendError(info.Source, info.Destination, "Transfer got failed", 0)
				jptm.SetStatus(common.ETransferStatus.Failed())
				jptm.ReportTransferDone()
				return
			}
			jptm.LogSendError(info.Source, info.Destination, "CleanUp Successful and transfer got failed", 0) //cleanup was successful
			jptm.SetStatus(common.ETransferStatus.Failed())
			jptm.ReportTransferDone()
			return
		}
		jptm.SetStatus(common.ETransferStatus.Success()) // is a real failure, not just a SkippedFileAlreadyExists, in this case
		jptm.ReportTransferDone()
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
func copyFile(ctx context.Context, src string, dst string) (written int64, err error) {
	source, err := os.Open(src)
	if err != nil {
		return 0, err
	}
	defer source.Close()
	destination, err := os.Create(dst)
	if err != nil {
		return 0, err
	}
	defer destination.Close()
	return io.Copy(destination, NewReader(ctx, source))
}

func removeFile(dst string) error {
	err := os.Remove(dst)
	return err
}
