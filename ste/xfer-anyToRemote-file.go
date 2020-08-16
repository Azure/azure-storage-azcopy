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
	"crypto/md5"
	"errors"
	"fmt"
	"hash"
	"net/http"
	"net/url"
	"runtime"
	"strings"
	"sync"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/Azure/azure-storage-blob-go/azblob"
)

// This code for blob tier safety is _not_ safe for multiple jobs at once.
// That's alright, but it's good to know on the off chance.
// This sync.Once is present to ensure we output information about a S2S access tier preservation failure to stdout once
var s2sAccessTierFailureLogStdout sync.Once
var checkLengthFailureOnReadOnlyDst sync.Once

// This sync.Once and string pair ensures that we only get a user's destination account kind once when handling set-tier
// Premium block blob doesn't support tiering, and page blobs only support P1-80.
// There are also size restrictions on tiering.
var destAccountSKU string
var destAccountKind string
var tierSetPossibleFail bool
var getDestAccountInfo sync.Once
var getDestAccountInfoError error

func prepareDestAccountInfo(bURL azblob.BlobURL, jptm IJobPartTransferMgr, ctx context.Context, mustGet bool) {
	getDestAccountInfo.Do(func() {
		infoResp, err := bURL.GetAccountInfo(ctx)
		if err != nil {
			// If GetAccountInfo fails, this transfer should fail because we lack at least one available permission
			// UNLESS the user is using OAuth. In which case, the account owner can still get the info.
			// If we fail to get the info under OAuth, don't fail the transfer.
			// (https://docs.microsoft.com/en-us/rest/api/storageservices/get-account-information#authorization)
			if mustGet {
				getDestAccountInfoError = err
			} else {
				tierSetPossibleFail = true
				destAccountSKU = "failget"
				destAccountKind = "failget"
			}
		} else {
			destAccountSKU = string(infoResp.SkuName())
			destAccountKind = string(infoResp.AccountKind())
		}
	})

	if getDestAccountInfoError != nil {
		jptm.FailActiveSendWithStatus("Checking destination tier availability (Set blob tier) ", getDestAccountInfoError, common.ETransferStatus.TierAvailabilityCheckFailure())
	}
}

// TODO: Infer availability based upon blob size as well, for premium page blobs.
func BlobTierAllowed(destTier azblob.AccessTierType) bool {
	// If we failed to get the account info, just return true.
	// This is because we can't infer whether it's possible or not, and the setTier operation could possibly succeed (or fail)
	if tierSetPossibleFail {
		return true
	}

	// If the account is premium, Storage/StorageV2 only supports page blobs (Tiers P1-80). Block blob does not support tiering whatsoever.
	if strings.Contains(destAccountSKU, "Premium") {
		// storage V1/V2
		if destAccountKind == "StorageV2" {
			// P1-80 possible.
			return premiumPageBlobTierRegex.MatchString(string(destTier))
		}

		if destAccountKind == "Storage" {
			// No tier setting is allowed.
			return false
		}

		if strings.Contains(destAccountKind, "Block") {
			// No tier setting is allowed.
			return false
		}

		// Any other storage type would have to be file storage, and we can't set tier there.
		panic("Cannot set tier on azure files.")
	} else {
		// Standard storage account. If it's Hot, Cool, or Archive, we're A-OK.
		// Page blobs, however, don't have an access tier on Standard accounts.
		// However, this is also OK, because the pageblob sender code prevents us from using a standard access tier type.
		return destTier == azblob.AccessTierArchive || destTier == azblob.AccessTierCool || destTier == azblob.AccessTierHot
	}
}

func AttemptSetBlobTier(jptm IJobPartTransferMgr, blobTier azblob.AccessTierType, blobURL azblob.BlobURL, ctx context.Context) {
	if jptm.IsLive() && blobTier != azblob.AccessTierNone {
		// Set the latest service version from sdk as service version in the context.
		ctxWithLatestServiceVersion := context.WithValue(ctx, ServiceAPIVersionOverride, azblob.ServiceVersion)

		// Let's check if we can confirm we'll be able to check the destination blob's account info.
		// A SAS token, even with write-only permissions is enough. OR, OAuth with the account owner.
		// We can't guess that last information, so we'll take a gamble and try to get account info anyway.
		destParts := azblob.NewBlobURLParts(blobURL.URL())
		mustGet := destParts.SAS.Encode() != ""

		prepareDestAccountInfo(blobURL, jptm, ctxWithLatestServiceVersion, mustGet)
		tierAvailable := BlobTierAllowed(blobTier)

		if tierAvailable {
			_, err := blobURL.SetTier(ctxWithLatestServiceVersion, blobTier, azblob.LeaseAccessConditions{})
			if err != nil {
				// This uses a currently true assumption about the code:
				// the blobTier passed into this is the destination blob tier, which may be overridden by the user.
				// If the user overrides the blob tier, S2SSrcBlobTier is not overridden.
				if jptm.Info().S2SSrcBlobTier == blobTier {
					jptm.LogTransferInfo(pipeline.LogError, jptm.Info().Source, jptm.Info().Destination, "Failed to replicate blob tier at destination. Try transferring with the flag --s2s-preserve-access-tier=false")
					s2sAccessTierFailureLogStdout.Do(func() {
						glcm := common.GetLifecycleMgr()
						glcm.Info("One or more blobs have failed blob tier replication at the destination. Try transferring with the flag --s2s-preserve-access-tier=false")
					})
				}

				// If we know the destination tier is possible, something's wrong and we should error out.
				if tierSetPossibleFail {
					jptm.LogTransferInfo(pipeline.LogWarning, jptm.Info().Source, jptm.Info().Destination, "Cannot set destination block blob to the pending access tier ("+string(blobTier)+"), because either the destination account or blob type does not support it. The transfer will still succeed.")
				} else {
					jptm.FailActiveSendWithStatus("Setting tier", err, common.ETransferStatus.BlobTierFailure())
				}
			}
		} else {
			jptm.LogTransferInfo(pipeline.LogWarning, jptm.Info().Source, jptm.Info().Destination, "The intended tier ("+string(blobTier)+") isn't available on the destination blob type or storage account, so it was left as the default.")
		}
	}
}

// xfer.go requires just a single xfer function for the whole job.
// This routine serves that role for uploads and S2S copies, and redirects for each transfer to a file or folder implementation
func anyToRemote(jptm IJobPartTransferMgr, p pipeline.Pipeline, pacer pacer, senderFactory senderFactory, sipf sourceInfoProviderFactory) {
	info := jptm.Info()
	fromTo := jptm.FromTo()

	// Ensure that the transfer isn't the same item, and fail it if it is.
	// This scenario can only happen with S2S. We'll parse the URLs and compare the host and path.
	if fromTo.IsS2S() {
		srcURL, err := url.Parse(info.Source)
		common.PanicIfErr(err)
		dstURL, err := url.Parse(info.Destination)
		common.PanicIfErr(err)

		if srcURL.Hostname() == dstURL.Hostname() &&
			srcURL.EscapedPath() == dstURL.EscapedPath() {

			srcRQ := srcURL.Query()
			dstRQ := dstURL.Query()
			if (len(srcRQ["versionId"]) > 0 || len(srcRQ["versionid"]) > 0) && !(len(dstRQ["versionId"]) > 0 || len(dstRQ["versionid"]) > 0) {
				// Case: Replacing the current version of the blob with the previous version.
				// In this particular case, source URL should contain version id and destination URL should not have any version id specified
			} else {
				jptm.LogSendError(info.Source, info.Destination, "Transfer source and destination are the same, which would cause data loss. Aborting transfer.", 0)
				jptm.SetStatus(common.ETransferStatus.Failed())
				jptm.ReportTransferDone()
				return
			}
		}
	}

	if info.IsFolderPropertiesTransfer() {
		anyToRemote_folder(jptm, info, p, pacer, senderFactory, sipf)
	} else {
		anyToRemote_file(jptm, info, p, pacer, senderFactory, sipf)
	}
}

// anyToRemote_file handles all kinds of sender operations for files - both uploads from local files, and S2S copies
func anyToRemote_file(jptm IJobPartTransferMgr, info TransferInfo, p pipeline.Pipeline, pacer pacer, senderFactory senderFactory, sipf sourceInfoProviderFactory) {

	pseudoId := common.NewPseudoChunkIDForWholeFile(info.Source)
	jptm.LogChunkStatus(pseudoId, common.EWaitReason.XferStart())
	defer jptm.LogChunkStatus(pseudoId, common.EWaitReason.ChunkDone())

	srcSize := info.SourceSize

	// step 1. perform initial checks
	if jptm.WasCanceled() {
		/* This is the earliest we detect jptm has been cancelled before scheduling chunks */
		jptm.SetStatus(common.ETransferStatus.Cancelled())
		jptm.ReportTransferDone()
		return
	}

	// step 2a. Create sender
	srcInfoProvider, err := sipf(jptm)
	if err != nil {
		jptm.LogSendError(info.Source, info.Destination, err.Error(), 0)
		jptm.SetStatus(common.ETransferStatus.Failed())
		jptm.ReportTransferDone()
		return
	}
	if srcInfoProvider.EntityType() != common.EEntityType.File() {
		panic("configuration error. Source Info Provider does not have File entity type")
	}

	s, err := senderFactory(jptm, info.Destination, p, pacer, srcInfoProvider)
	if err != nil {
		jptm.LogSendError(info.Source, info.Destination, err.Error(), 0)
		jptm.SetStatus(common.ETransferStatus.Failed())
		jptm.ReportTransferDone()
		return
	}

	// step 2b. Read chunk size and count from the sender (since it may have applied its own defaults and/or calculations to produce these values
	numChunks := s.NumChunks()
	if jptm.ShouldLog(pipeline.LogInfo) {
		jptm.LogTransferStart(info.Source, info.Destination, fmt.Sprintf("Specified chunk size %d", s.ChunkSize()))
	}
	if s.NumChunks() == 0 {
		panic("must always schedule one chunk, even if file is empty") // this keeps our code structure simpler, by using a dummy chunk for empty files
	}

	// step 3: check overwrite option
	// if the force Write flags is set to false or prompt
	// then check the file exists at the remote location
	// if it does, react accordingly
	if jptm.GetOverwriteOption() != common.EOverwriteOption.True() {
		exists, dstLmt, existenceErr := s.RemoteFileExists()
		if existenceErr != nil {
			jptm.LogSendError(info.Source, info.Destination, "Could not check destination file existence. "+existenceErr.Error(), 0)
			jptm.SetStatus(common.ETransferStatus.Failed()) // is a real failure, not just a SkippedFileAlreadyExists, in this case
			jptm.ReportTransferDone()
			return
		}
		if exists {
			shouldOverwrite := false

			// if necessary, prompt to confirm user's intent
			if jptm.GetOverwriteOption() == common.EOverwriteOption.Prompt() {
				// remove the SAS before prompting the user
				parsed, _ := url.Parse(info.Destination)
				parsed.RawQuery = ""
				shouldOverwrite = jptm.GetOverwritePrompter().ShouldOverwrite(parsed.String(), common.EEntityType.File())
			} else if jptm.GetOverwriteOption() == common.EOverwriteOption.IfSourceNewer() {
				// only overwrite if source lmt is newer (after) the destination
				if jptm.LastModifiedTime().After(dstLmt) {
					shouldOverwrite = true
				}
			}

			if !shouldOverwrite {
				// logging as Warning so that it turns up even in compact logs, and because previously we use Error here
				jptm.LogAtLevelForCurrentTransfer(pipeline.LogWarning, "File already exists, so will be skipped")
				jptm.SetStatus(common.ETransferStatus.SkippedEntityAlreadyExists())
				jptm.ReportTransferDone()
				return
			}
		}
	}

	// step 4: Open the local Source File (if any)
	common.GetLifecycleMgr().E2EAwaitAllowOpenFiles()
	jptm.LogChunkStatus(pseudoId, common.EWaitReason.OpenLocalSource())
	var sourceFileFactory func() (common.CloseableReaderAt, error)
	srcFile := (common.CloseableReaderAt)(nil)
	if srcInfoProvider.IsLocal() {
		sourceFileFactory = srcInfoProvider.(ILocalSourceInfoProvider).OpenSourceFile // all local providers must implement this interface
		srcFile, err = sourceFileFactory()
		if err != nil {
			suffix := ""
			if strings.Contains(err.Error(), "Access is denied") && runtime.GOOS == "windows" {
				suffix = " See --" + common.BackupModeFlagName + " flag if you need to read all files regardless of their permissions"
			}
			jptm.LogSendError(info.Source, info.Destination, "Couldn't open source. "+err.Error()+suffix, 0)
			jptm.SetStatus(common.ETransferStatus.Failed())
			jptm.ReportTransferDone()
			return
		}
		defer srcFile.Close() // we read all the chunks in this routine, so can close the file at the end
	}

	// We always to LMT verification after the transfer. Also do it here, before transfer, when:
	// 1) Source is local, and source's size is > 1 chunk.  (why not always?  Since getting LMT is not "free" at very small sizes)
	// 2) Source is remote, i.e. S2S copy case. And source's size is larger than one chunk. So verification can possibly save transfer's cost.
	jptm.LogChunkStatus(pseudoId, common.EWaitReason.ModifiedTimeRefresh())
	if _, isS2SCopier := s.(s2sCopier); numChunks > 1 &&
		(srcInfoProvider.IsLocal() || isS2SCopier && info.S2SSourceChangeValidation) {
		lmt, err := srcInfoProvider.GetFreshFileLastModifiedTime()
		if err != nil {
			jptm.LogSendError(info.Source, info.Destination, "Couldn't get source's last modified time-"+err.Error(), 0)
			jptm.SetStatus(common.ETransferStatus.Failed())
			jptm.ReportTransferDone()
			return
		}
		if !lmt.Equal(jptm.LastModifiedTime()) {
			jptm.LogSendError(info.Source, info.Destination, "File modified since transfer scheduled", 0)
			jptm.SetStatus(common.ETransferStatus.Failed())
			jptm.ReportTransferDone()
			return
		}
	}

	// step 5a: lock the destination
	// (is safe to do it relatively early here, before we run the prologue, because its just a internal lock, within the app)
	// But must be after all of the early returns that are above here (since
	// if we succeed here, we need to know the epilogue will definitely run to unlock later)
	jptm.LogChunkStatus(pseudoId, common.EWaitReason.LockDestination())
	err = jptm.WaitUntilLockDestination(jptm.Context())
	if err != nil {
		jptm.LogSendError(info.Source, info.Destination, err.Error(), 0)
		jptm.SetStatus(common.ETransferStatus.Failed())
		jptm.ReportTransferDone()
		return
	}

	// *****
	// Error-handling rules change here.
	// ABOVE this point, we end the transfer using the code as shown above
	// BELOW this point, this routine always schedules the expected number
	// of chunks, even if it has seen a failure, and the
	// workers (the chunkfunc implementations) must use
	// jptm.FailActiveSend when there's an error)
	// TODO: are we comfortable with this approach?
	//   DECISION: 16 Jan, 2019: for now, we are leaving in place the above rule than number of of completed chunks must
	//   eventually reach numChunks, since we have no better short-term alternative.
	// ******

	// step 5b: tell jptm what to expect, and how to clean up at the end
	jptm.SetNumberOfChunks(numChunks)
	jptm.SetActionAfterLastChunk(func() { epilogueWithCleanupSendToRemote(jptm, s, srcInfoProvider) })

	// stop tracking pseudo id (since real chunk id's will be tracked from here on)
	jptm.LogChunkStatus(pseudoId, common.EWaitReason.ChunkDone())

	// Step 6: Go through the file and schedule chunk messages to send each chunk
	scheduleSendChunks(jptm, info.Source, srcFile, srcSize, s, sourceFileFactory, srcInfoProvider)
}

var jobCancelledLocalPrefetchErr = errors.New("job was cancelled; Pre-fetching stopped")

// Schedule all the send chunks.
// For upload, we force preload of each chunk to memory, and we wait (block)
// here if the amount of preloaded data gets excessive. That's OK to do,
// because if we already have that much data preloaded (and scheduled for sending in
// chunks) then we don't need to schedule any more chunks right now, so the blocking
// is harmless (and a good thing, to avoid excessive RAM usage).
// To take advantage of the good sequential read performance provided by many file systems,
// and to be able to compute an MD5 hash for the file, we work sequentially through the file here.
func scheduleSendChunks(jptm IJobPartTransferMgr, srcPath string, srcFile common.CloseableReaderAt, srcSize int64, s sender, sourceFileFactory common.ChunkReaderSourceFactory, srcInfoProvider ISourceInfoProvider) {
	// For generic send
	chunkSize := s.ChunkSize()
	numChunks := s.NumChunks()

	// For upload
	var md5Channel chan<- []byte
	var prefetchErr error
	var chunkReader common.SingleChunkReader
	ps := common.PrologueState{}

	var md5Hasher hash.Hash
	if jptm.ShouldPutMd5() {
		md5Hasher = md5.New()
	} else {
		md5Hasher = common.NewNullHasher()
	}
	safeToUseHash := true

	if srcInfoProvider.IsLocal() {
		md5Channel = s.(uploader).Md5Channel()
		defer close(md5Channel)
	}

	chunkIDCount := int32(0)
	for startIndex := int64(0); startIndex < srcSize || isDummyChunkInEmptyFile(startIndex, srcSize); startIndex += int64(chunkSize) {

		adjustedChunkSize := int64(chunkSize)

		// compute actual size of the chunk
		if startIndex+int64(chunkSize) > srcSize {
			adjustedChunkSize = srcSize - startIndex
		}

		id := common.NewChunkID(srcPath, startIndex, adjustedChunkSize) // TODO: stop using adjustedChunkSize, below, and use the size that's in the ID

		if srcInfoProvider.IsLocal() {
			if jptm.WasCanceled() {
				prefetchErr = jobCancelledLocalPrefetchErr
			} else {
				// As long as the prefetch error is nil, we'll attempt a prefetch.
				// Otherwise, the chunk reader didn't need to be made.
				// It's a waste of time to prefetch here, too, if we already know we can't upload.
				// Furthermore, this prevents prefetchErr changing from under us.
				if prefetchErr == nil {
					// create reader and prefetch the data into it
					chunkReader = createPopulatedChunkReader(jptm, sourceFileFactory, id, adjustedChunkSize, srcFile)

					// Wait until we have enough RAM, and when we do, prefetch the data for this chunk.
					prefetchErr = chunkReader.BlockingPrefetch(srcFile, false)
					if prefetchErr == nil {
						chunkReader.WriteBufferTo(md5Hasher)
						ps = chunkReader.GetPrologueState()
					} else {
						safeToUseHash = false // because we've missed a chunk
					}
				}
			}
		}

		// If this is the the very first chunk, do special init steps
		if startIndex == 0 {
			// Run prologue before first chunk is scheduled.
			// If file is not local, we'll get no leading bytes, but we still run the prologue in case
			// there's other initialization to do in the sender.
			modified := s.Prologue(ps)
			if modified {
				jptm.SetDestinationIsModified()
			}
		}

		// schedule the chunk job/msg
		jptm.LogChunkStatus(id, common.EWaitReason.WorkerGR())
		isWholeFile := numChunks == 1
		var cf chunkFunc
		if srcInfoProvider.IsLocal() {
			if prefetchErr == nil {
				cf = s.(uploader).GenerateUploadFunc(id, chunkIDCount, chunkReader, isWholeFile)
			} else {
				if chunkReader != nil {
					_ = chunkReader.Close()
				}

				// Our jptm logic currently requires us to schedule every chunk, even if we know there's an error,
				// so we schedule a func that will just fail with the given error
				cf = createSendToRemoteChunkFunc(jptm, id, func() { jptm.FailActiveSend("chunk data read", prefetchErr) })
			}
		} else {
			cf = s.(s2sCopier).GenerateCopyFunc(id, chunkIDCount, adjustedChunkSize, isWholeFile)
		}
		jptm.ScheduleChunks(cf)

		chunkIDCount++
	}

	// sanity check to verify the number of chunks scheduled
	if chunkIDCount != int32(numChunks) {
		panic(fmt.Errorf("difference in the number of chunk calculated %v and actual chunks scheduled %v for src %s of size %v", numChunks, chunkIDCount, srcPath, srcSize))
	}

	if srcInfoProvider.IsLocal() && safeToUseHash {
		md5Channel <- md5Hasher.Sum(nil)
	}
}

// Make reader for this chunk.
// Each chunk reader also gets a factory to make a reader for the file, in case it needs to repeat its part
// of the file read later (when doing a retry)
// BTW, the reader we create here just works with a single chuck. (That's in contrast with downloads, where we have
// to use an object that encompasses the whole file, so that it can put the chunks back into order. We don't have that requirement here.)
func createPopulatedChunkReader(jptm IJobPartTransferMgr, sourceFileFactory common.ChunkReaderSourceFactory, id common.ChunkID, adjustedChunkSize int64, srcFile common.CloseableReaderAt) common.SingleChunkReader {
	chunkReader := common.NewSingleChunkReader(jptm.Context(),
		sourceFileFactory,
		id,
		adjustedChunkSize,
		jptm.ChunkStatusLogger(),
		jptm,
		jptm.SlicePool(),
		jptm.CacheLimiter())

	return chunkReader
}

func isDummyChunkInEmptyFile(startIndex int64, fileSize int64) bool {
	return startIndex == 0 && fileSize == 0
}

// Complete epilogue. Handles both success and failure.
func epilogueWithCleanupSendToRemote(jptm IJobPartTransferMgr, s sender, sip ISourceInfoProvider) {
	info := jptm.Info()
	// allow our usual state tracking mechanism to keep count of how many epilogues are running at any given instant, for perf diagnostics
	pseudoId := common.NewPseudoChunkIDForWholeFile(info.Source)
	jptm.LogChunkStatus(pseudoId, common.EWaitReason.Epilogue())
	defer jptm.LogChunkStatus(pseudoId, common.EWaitReason.ChunkDone()) // normal setting to done doesn't apply to these pseudo ids

	if jptm.WasCanceled() {
		// This is where we detect that transfer has been cancelled. Further statments do not act on
		// dead jptm. We set the status here.
		jptm.SetStatus(common.ETransferStatus.Cancelled())
	}
	if jptm.IsLive() {
		if _, isS2SCopier := s.(s2sCopier); sip.IsLocal() || (isS2SCopier && info.S2SSourceChangeValidation) {
			// Check the source to see if it was changed during transfer. If it was, mark the transfer as failed.
			lmt, err := sip.GetFreshFileLastModifiedTime()
			if err != nil {
				jptm.FailActiveSend("epilogueWithCleanupSendToRemote", err)
			}
			if !lmt.Equal(jptm.LastModifiedTime()) {
				jptm.FailActiveSend("epilogueWithCleanupSendToRemote", errors.New("source modified during transfer"))
			}
		}
	}

	// TODO: should we refactor to force this to accept jptm isLive as a parameter, to encourage it to be checked?
	//  or should we redefine epilogue to be success-path only, and only call it in that case?
	s.Epilogue() // Perform service-specific cleanup before jptm cleanup. Some services may actually require setup to make the file actually appear.

	if jptm.IsLive() && info.DestLengthValidation {
		_, isS2SCopier := s.(s2sCopier)
		shouldCheckLength := true
		destLength, err := s.GetDestinationLength()

		if resp, respOk := err.(pipeline.Response); respOk && resp.Response() != nil &&
			resp.Response().StatusCode == http.StatusForbidden {
			// The destination is write-only. Cannot verify length
			shouldCheckLength = false
			checkLengthFailureOnReadOnlyDst.Do(func() {
				var glcm = common.GetLifecycleMgr()
				msg := fmt.Sprintf("Could not read destination length. If the destination is write-only, use --check-length=false on the command line.")
				glcm.Info(msg)
				if jptm.ShouldLog(pipeline.LogError) {
					jptm.Log(pipeline.LogError, msg)
				}
			})
		}

		if shouldCheckLength {
			if err != nil {
				wrapped := fmt.Errorf("Could not read destination length. %w", err)
				jptm.FailActiveSend(common.IffString(isS2SCopier, "S2S ", "Upload ")+"Length check: Get destination length", wrapped)
			} else if destLength != jptm.Info().SourceSize {
				jptm.FailActiveSend(common.IffString(isS2SCopier, "S2S ", "Upload ")+"Length check", errors.New("destination length does not match source length"))
			}
		}
	}

	if jptm.HoldsDestinationLock() { // TODO consider add test of jptm.IsDeadInflight here, so we can remove that from inside all the cleanup methods
		s.Cleanup() // Perform jptm cleanup, if THIS jptm has the lock on the destination
	}

	commonSenderCompletion(jptm, s, info)
}

// commonSenderCompletion is used for both files and folders
func commonSenderCompletion(jptm IJobPartTransferMgr, s sender, info TransferInfo) {

	jptm.EnsureDestinationUnlocked()

	if jptm.TransferStatusIgnoringCancellation() == 0 {
		panic("think we're finished but status is notStarted")
	}
	// note that we do not really know whether the context was canceled because of an error, or because the user asked for it
	// if was an intentional cancel, the status is still "in progress", so we are still counting it as pending
	// we leave these transfer status alone
	// in case of errors, the status was already set, so we don't need to do anything here either
	//
	// it is entirely possible that all the chunks were finished, but then by the time we get to this line
	// the context is canceled. In this case, a completely transferred file would not be marked "completed".
	// it's definitely a case that we should be aware of, but given how rare it is, and how low the impact (the user can just resume), we don't have to do anything more to it atm.
	if jptm.IsLive() {
		// We know all chunks are done (because this routine was called)
		// and we know the transfer didn't fail (because just checked its status above and made sure the context was not canceled),
		// so it must have succeeded. So make sure its not left "in progress" state
		jptm.SetStatus(common.ETransferStatus.Success())

		// Final logging
		if jptm.ShouldLog(pipeline.LogInfo) { // TODO: question: can we remove these ShouldLogs?  Aren't they inside Log?
			if _, ok := s.(s2sCopier); ok {
				jptm.Log(pipeline.LogInfo, fmt.Sprintf("COPYSUCCESSFUL: %s%s", info.entityTypeLogIndicator(), strings.Split(info.Destination, "?")[0]))
			} else if _, ok := s.(uploader); ok {
				// Output relative path of file, includes file name.
				jptm.Log(pipeline.LogInfo, fmt.Sprintf("UPLOADSUCCESSFUL: %s%s", info.entityTypeLogIndicator(), strings.Split(info.Destination, "?")[0]))
			} else {
				panic("invalid state: epilogueWithCleanupSendToRemote should be used by COPY and UPLOAD")
			}
		}
		if jptm.ShouldLog(pipeline.LogDebug) {
			jptm.Log(pipeline.LogDebug, "Finalizing Transfer")
		}
	} else {
		if jptm.ShouldLog(pipeline.LogDebug) {
			jptm.Log(pipeline.LogDebug, "Finalizing Transfer Cancellation/Failure")
		}
	}
	// successful or unsuccessful, it's definitely over
	jptm.ReportTransferDone()
}
