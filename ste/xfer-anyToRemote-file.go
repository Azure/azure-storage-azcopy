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

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

// IBlobClient is an interface to allow ValidateTier to accept any type of client
type IBlobClient interface {
	URL() string
	GetAccountInfo(ctx context.Context, o *blob.GetAccountInfoOptions) (blob.GetAccountInfoResponse, error)
}

// This code for blob tier safety is _not_ safe for multiple jobs at once.
// That's alright, but it's good to know on the off chance.
// This sync.Once is present to ensure we output information about a S2S access tier preservation failure to stdout once
var tierNotAllowedFailure sync.Once
var checkLengthFailureOnReadOnlyDst sync.Once

// This sync.Once and string pair ensures that we only get a user's destination account kind once when handling set-tier
// Premium block blob doesn't support tiering, and page blobs only support P1-80.
// There are also size restrictions on tiering.
var destAccountSKU string

var destAccountKind string
var tierSetPossibleFail bool
var getDestAccountInfo sync.Once
var getDestAccountInfoError error

func prepareDestAccountInfo(client IBlobClient, jptm IJobPartTransferMgr, ctx context.Context, mustGet bool) {
	getDestAccountInfo.Do(func() {
		infoResp, err := client.GetAccountInfo(ctx, nil)
		if err != nil {
			// If GetAccountInfo fails, this transfer should fail because we lack at least one available permission
			// UNLESS the user is using OAuth. In which case, the account owner can still get the info.
			// If we fail to get the info under OAuth, don't fail the transfer.
			// (https://docs.microsoft.com/en-us/rest/api/storageservices/get-account-information#authorization)
			if mustGet {
				getDestAccountInfoError = err
			} else {
				tierSetPossibleFail = true
				glcm := common.GetLifecycleMgr()
				glcm.Info("Transfers could fail because AzCopy could not verify if the destination supports tiers.")
				destAccountSKU = "failget"
				destAccountKind = "failget"
			}
		} else {
			sku := infoResp.SKUName
			kind := infoResp.AccountKind
			destAccountSKU = string(*sku)
			destAccountKind = string(*kind)
		}
	})

	if getDestAccountInfoError != nil {
		jptm.FailActiveSendWithStatus("Checking destination tier availability (Set blob tier) ", getDestAccountInfoError, common.ETransferStatus.TierAvailabilityCheckFailure())
	}
}

// // TODO: Infer availability based upon blob size as well, for premium page blobs.
func BlobTierAllowed(destTier *blob.AccessTier) bool {
	// Note: destTier is guaranteed to be non nil.
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
			return premiumPageBlobTierRegex.MatchString(string(*destTier))
		}

		if destAccountKind == "Storage" {
			// No tier setting is allowed.
			return false
		}

		if strings.Contains(destAccountKind, "Block") {
			// Setting tier on Premium Block Blob accounts is allowable in certain regions on whitelisted subscriptions.
			// If setting tier fails, we allow service to throw error instead of taking preventative measures in AzCopy.
			return true
		}

		// Any other storage type would have to be file storage, and we can't set tier there.
		panic("Cannot set tier on azure files.")
	} else {
		if destAccountKind == "Storage" { // Tier setting not allowed on classic accounts
			return false
		}
		// Standard storage account. If it's Hot, Cool, or Archive, we're A-OK.
		// Page blobs, however, don't have an access tier on Standard accounts.
		// However, this is also OK, because the pageblob sender code prevents us from using a standard access tier type.
		return *destTier == blob.AccessTierArchive || *destTier == blob.AccessTierCool || *destTier == common.EBlockBlobTier.Cold().ToAccessTierType() || *destTier == blob.AccessTierHot
	}
}

func ValidateTier(jptm IJobPartTransferMgr, blobTier *blob.AccessTier, client IBlobClient, ctx context.Context, performQuietly bool) (isValid bool) {

	if jptm.IsLive() && blobTier != nil {

		// Let's check if we can confirm we'll be able to check the destination blob's account info.
		// A SAS token, even with write-only permissions is enough. OR, OAuth with the account owner.
		// We can't guess that last information, so we'll take a gamble and try to get account info anyway.
		// User delegation SAS is the same as OAuth
		destParts, err := blob.ParseURL(client.URL())
		if err != nil {
			return false
		}
		mustGet := destParts.SAS.Encode() != "" && destParts.SAS.SignedTID() == ""

		prepareDestAccountInfo(client, jptm, ctx, mustGet)
		tierAvailable := BlobTierAllowed(blobTier)

		if tierAvailable {
			return true
		} else if !performQuietly {
			tierNotAllowedFailure.Do(func() {
				glcm := common.GetLifecycleMgr()
				glcm.Info("Destination could not accommodate the tier " + string(*blobTier) + ". Going ahead with the default tier. In case of service to service transfer, consider setting the flag --s2s-preserve-access-tier=false.")
			})
		}
		return false
	} else {
		return false
	}
}

// xfer.go requires just a single xfer function for the whole job.
// This routine serves that role for uploads and S2S copies, and redirects for each transfer to a file or folder implementation
func anyToRemote(jptm IJobPartTransferMgr, pacer pacer, senderFactory senderFactory, sipf sourceInfoProviderFactory) {
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

			// src and dst point to the same object
			// if src does not have snapshot/versionId, then error out as we cannot copy into the object itself
			// if dst has snapshot or versionId specified, do not error and let the service fail the request with clear message
			srcRQ := srcURL.Query()

			if len(srcRQ["sharesnapshot"]) == 0 && len(srcRQ["snapshot"]) == 0 && len(srcRQ["versionid"]) == 0 {
				jptm.LogSendError(info.Source, info.Destination, "Transfer source and destination are the same, which would cause data loss. Aborting transfer.", 0)
				jptm.SetStatus(common.ETransferStatus.Failed())
				jptm.ReportTransferDone()
				return
			}
		}
	}

	switch info.EntityType {
	case common.EEntityType.Folder():
		anyToRemote_folder(jptm, info, pacer, senderFactory, sipf)
	case common.EEntityType.FileProperties():
		anyToRemote_fileProperties(jptm, info, pacer, senderFactory, sipf)
	case common.EEntityType.File(), common.EEntityType.Hardlink():
		if jptm.GetOverwriteOption() == common.EOverwriteOption.PosixProperties() {
			anyToRemote_fileProperties(jptm, info, pacer, senderFactory, sipf)
		} else {
			if info.EntityType == common.EEntityType.Hardlink() {
				fmt.Println("Hardlink reached here.......", info.Source)
			}
			anyToRemote_file(jptm, info, pacer, senderFactory, sipf)
		}
	case common.EEntityType.Symlink():
		anyToRemote_symlink(jptm, info, pacer, senderFactory, sipf)
	}
}

// anyToRemote_file handles all kinds of sender operations for files - both uploads from local files, and S2S copies
func anyToRemote_file(jptm IJobPartTransferMgr, info *TransferInfo, pacer pacer, senderFactory senderFactory, sipf sourceInfoProviderFactory) {

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

	// TODO: Phase I - NFS Hardlink Handling
	// For Phase I of NFS support, hardlinks are treated as regular files.
	// This check ensures that the source entity is either a File or a Hardlink,
	// since other types (e.g., folders or symlinks) are not expected here.
	//
	// The condition prevents a panic that would occur if an unsupported entity type
	// is passed to this code path.
	//
	// NOTE: In future phases, when hardlinks are handled differently, this logic may need to be updated.
	if srcInfoProvider.EntityType() != common.EEntityType.File() && srcInfoProvider.EntityType() != common.EEntityType.Hardlink() {
		panic("configuration error. Source Info Provider does not have File entity type")
	}

	s, err := senderFactory(jptm, info.Destination, pacer, srcInfoProvider)
	if err != nil {
		jptm.LogSendError(info.Source, info.Destination, err.Error(), 0)
		jptm.SetStatus(common.ETransferStatus.Failed())
		jptm.ReportTransferDone()
		return
	}

	// step 2b. Read chunk size and count from the sender (since it may have applied its own defaults and/or calculations to produce these values
	numChunks := s.NumChunks()
	if jptm.ShouldLog(common.LogInfo) {
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
				jptm.LogAtLevelForCurrentTransfer(common.LogWarning, "File already exists, so will be skipped")
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
						// *** NOTE: the hasher hashes the buffer as it is right now.  IF the chunk upload fails, then
						//     the chunkReader will repeat the read from disk. So there is an essential dependency
						//     between the hashing and our change detection logic.
						common.DocumentationForDependencyOnChangeDetection() // <-- read the documentation here ***

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
		// This is where we detect that transfer has been cancelled. Further statements do not act on
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
				// **** Note that this check is ESSENTIAL and not just for the obvious reason of not wanting to upload
				//      corrupt or inconsistent data. It's also essential to the integrity of our MD5 hashes.
				common.DocumentationForDependencyOnChangeDetection() // <-- read the documentation here ***

				jptm.Log(common.LogError, fmt.Sprintf("Source Modified during transfer. Enumeration %v, current %v", jptm.LastModifiedTime(), lmt))
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

		var respErr *azcore.ResponseError
		if errors.As(err, &respErr) && respErr.StatusCode == http.StatusForbidden {
			// The destination is write-only. Cannot verify length
			shouldCheckLength = false
			checkLengthFailureOnReadOnlyDst.Do(func() {
				var glcm = common.GetLifecycleMgr()
				msg := "Could not read destination length. If the destination is write-only, use --check-length=false on the command line."
				glcm.Info(msg)
				if jptm.ShouldLog(common.LogError) {
					jptm.Log(common.LogError, msg)
				}
			})
		}

		if shouldCheckLength {
			if err != nil {
				wrapped := fmt.Errorf("Could not read destination length. %w", err)
				jptm.FailActiveSend(common.Iff(isS2SCopier, "S2S ", "Upload ")+"Length check: Get destination length", wrapped)
			} else if destLength != jptm.Info().SourceSize {
				jptm.FailActiveSend(common.Iff(isS2SCopier, "S2S ", "Upload ")+"Length check", errors.New("destination length does not match source length"))
			}
		}
	}

	if jptm.HoldsDestinationLock() { // TODO consider add test of jptm.IsDeadInflight here, so we can remove that from inside all the cleanup methods
		s.Cleanup() // Perform jptm cleanup, if THIS jptm has the lock on the destination
	}

	commonSenderCompletion(jptm, s, info)
}

// commonSenderCompletion is used for both files and folders
func commonSenderCompletion(jptm IJobPartTransferMgr, s sender, info *TransferInfo) {

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
		if jptm.Info().EntityType == common.EEntityType.Hardlink() {
			common.HardlinkNode.MarkDone(info.Source)
		}

		// Final logging
		if jptm.ShouldLog(common.LogInfo) { // TODO: question: can we remove these ShouldLogs?  Aren't they inside Log?
			if _, ok := s.(s2sCopier); ok {
				jptm.Log(common.LogInfo, fmt.Sprintf("COPYSUCCESSFUL: %s%s", info.entityTypeLogIndicator(), strings.Split(info.Destination, "?")[0]))
			} else if _, ok := s.(uploader); ok {
				// Output relative path of file, includes file name.
				jptm.Log(common.LogInfo, fmt.Sprintf("UPLOADSUCCESSFUL: %s%s", info.entityTypeLogIndicator(), strings.Split(info.Destination, "?")[0]))
			} else {
				panic("invalid state: epilogueWithCleanupSendToRemote should be used by COPY and UPLOAD")
			}
		}
		if jptm.ShouldLog(common.LogDebug) {
			jptm.Log(common.LogDebug, "Finalizing Transfer")
		}
	} else {
		if jptm.ShouldLog(common.LogDebug) {
			jptm.Log(common.LogDebug, "Finalizing Transfer Cancellation/Failure")
		}
	}
	// successful or unsuccessful, it's definitely over
	jptm.ReportTransferDone()
}
