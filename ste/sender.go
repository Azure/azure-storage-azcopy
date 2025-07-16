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
	"errors"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

// ///////////////////////////////////////////////////////////////////////////////////////////////
// sender is the abstraction that contains common sender behavior, for sending files/blobs.
// ///////////////////////////////////////////////////////////////////////////////////////////////
type sender interface {
	// ChunkSize returns the chunk size that should be used
	ChunkSize() int64

	// NumChunks returns the number of chunks that will be required for the target file
	NumChunks() uint32

	// RemoteFileExists is called to see whether the file already exists at the remote location (so we know whether we'll be overwriting it)
	// the lmt is returned if the file exists
	RemoteFileExists() (bool, time.Time, error)

	// Prologue is called automatically before the first chunkFunc is generated.
	// Implementation should do any initialization that is necessary - e.g.
	// creating the remote file for those destinations that require an explicit
	// creation step.
	// Implementations MUST return true if they may have modified the destination (i.e. false should only be returned if you KNOW you have not)
	Prologue(state common.PrologueState) (destinationModified bool)

	// Epilogue will be called automatically once we know all the chunk funcs have been processed.
	// This should handle any service-specific cleanup.
	// jptm cleanup is handled in Cleanup() now.
	Epilogue()

	// Cleanup will be called after epilogue.
	// Implementation should interact with its jptm to do
	// post-success processing if transfer has been successful so far,
	// or post-failure processing otherwise.
	Cleanup()

	// GetDestinationLength returns a integer containing the length of the file at the remote location
	GetDestinationLength() (int64, error)
}

//////////////////////////////////////////////////////////////////////////////////////////////////
// propertiesSender is a sender that can copy properties like metadata/tags/tier alone to
// to destination instead of full copy
//

type propertiesSender interface {
	sender

	GenerateCopyMetadata(id common.ChunkID) chunkFunc
}

// ///////////////////////////////////////////////////////////////////////////////////////////////
// folderSender is a sender that also knows how to send folder property information
// ///////////////////////////////////////////////////////////////////////////////////////////////
type folderSender interface {
	EnsureFolderExists() error
	SetFolderProperties() error
	DirUrlToString() string // This is only used in folder tracking, so this should trim the SAS token.
}

// We wrote properties at creation time.
type folderPropertiesSetInCreation struct{}

func (f folderPropertiesSetInCreation) Error() string {
	panic("Not a real error")
}

// ShouldSetProperties was called in creation and we got back a no.
type folderPropertiesNotOverwroteInCreation struct{}

func (f folderPropertiesNotOverwroteInCreation) Error() string {
	panic("Not a real error")
}

// ///////////////////////////////////////////////////////////////////////////////////////////////
// symlinkSender is a sender that also knows how to send symlink properties
// ///////////////////////////////////////////////////////////////////////////////////////////////
type symlinkSender interface {
	SendSymlink(linkData string) error
}

type senderFactory func(jptm IJobPartTransferMgr, destination string, sip ISourceInfoProvider) (sender, error)

/////////////////////////////////////////////////////////////////////////////////////////////////
// For copying folder properties, many of the ISender of the methods needed to copy one file from URL to a remote location
/////////////////////////////////////////////////////////////////////////////////////////////////

// ///////////////////////////////////////////////////////////////////////////////////////////////
// Abstraction of the methods needed to copy one file from URL to a remote location
// ///////////////////////////////////////////////////////////////////////////////////////////////
type s2sCopier interface {
	sender

	// GenerateCopyFunc returns a func() that will copy the specified portion of the source URL file to the remote location.
	GenerateCopyFunc(chunkID common.ChunkID, blockIndex int32, adjustedChunkSize int64, chunkIsWholeFile bool) chunkFunc
}

// Abstraction of the methods needed to upload one file to a remote location
// ///////////////////////////////////////////////////////////////////////////////////////////////
type uploader interface {
	sender

	// GenerateUploadFunc returns a func() that will upload the specified portion of the local file to the remote location
	// Instead of taking local file as a parameter, it takes a helper that will read from the file. That keeps details of
	// file IO out of the upload func, and lets that func concentrate only on the details of the remote endpoint
	GenerateUploadFunc(chunkID common.ChunkID, blockIndex int32, reader common.SingleChunkReader, chunkIsWholeFile bool) chunkFunc

	// Md5Channel returns the channel on which anyToRemote should send the MD5 hash to the uploader
	Md5Channel() chan<- []byte
}

func newMd5Channel() chan []byte {
	return make(chan []byte, 1) // must be buffered, so as not to hold up the goroutine running anyToRemote (which needs to start on the NEXT file after finishing its current one)
}

// Tries to set the MD5 hash using the given function
// Fails the upload if any error happens.
// This should be used only by those uploads that require a separate operation to PUT the hash at the end.
// Others, such as the block blob uploader piggyback their MD5 setting on other calls, and so won't use this.
func tryPutMd5Hash(jptm IJobPartTransferMgr, md5Channel <-chan []byte, worker func(hash []byte) error) {
	md5Hash, ok := <-md5Channel
	if ok {
		err := worker(md5Hash)
		if err != nil {
			jptm.FailActiveUpload("Setting hash", err)
		}
	} else {
		jptm.FailActiveUpload("Setting hash", errNoHash)
	}
}

var errNoHash = errors.New("no hash computed")

/////////////////////////////////////////////////////////////////////////////////////////////////

func getNumChunks(fileSize int64, chunkSize int64, putBlobSize int64) uint32 {
	numChunks := uint32(1) // we always map zero-size source files to ONE (empty) chunk
	if fileSize > 0 && fileSize > putBlobSize {
		chunkSizeI := chunkSize
		numChunks = common.Iff(
			fileSize%chunkSizeI == 0,
			uint32(fileSize/chunkSizeI),
			uint32(fileSize/chunkSizeI)+1)
	}
	return numChunks
}

func createSendToRemoteChunkFunc(jptm IJobPartTransferMgr, id common.ChunkID, body func()) chunkFunc {
	// For senders(uploader and s2sCopier), we set the chunk status to done as soon as the chunkFunc completes.
	// But we don't do that for downloads, since for those the chunk is not "done" until its flushed out
	// by the ChunkedFileWriter. (The ChunkedFileWriter will set the status to done at that time.)
	return createChunkFunc(true, jptm, id, body)
}

// createChunkFunc adds a standard prefix, which all chunkFuncs require, to the given body
func createChunkFunc(setDoneStatusOnExit bool, jptm IJobPartTransferMgr, id common.ChunkID, body func()) chunkFunc {
	return func(workerId int) {

		// BEGIN standard prefix that all chunk funcs need
		defer jptm.ReportChunkDone(id) // whether successful or failed, it's always "done" and we must always tell the jptm

		jptm.OccupyAConnection() // TODO: added the two operations for debugging purpose. remove later
		defer jptm.ReleaseAConnection()

		if jptm.WasCanceled() {
			jptm.LogChunkStatus(id, common.EWaitReason.Cancelled())
			return
		} else {
			if setDoneStatusOnExit {
				defer jptm.LogChunkStatus(id, common.EWaitReason.ChunkDone())
			}
		}

		// tell the jptm that the destination should be assumed to have been modified
		// (this is necessary for those cases where the prologue does not modify the dest, so the flag will not have been set at prologue time)
		// It's idempotent, so we call it every time rather than, say, test OffsetInFile and assume that id.OffsetInFile == 0 will always run first.
		jptm.SetDestinationIsModified()

		// END standard prefix

		body()
	}
}

// newBlobUploader detects blob type and creates a uploader manually
func newBlobUploader(jptm IJobPartTransferMgr, destination string, sip ISourceInfoProvider) (sender, error) {
	override := jptm.BlobTypeOverride()
	intendedType := override.ToBlobType()

	if override == common.EBlobType.Detect() {
		intendedType = inferBlobType(jptm.Info().Source, blob.BlobTypeBlockBlob)
		// jptm.LogTransferInfo(fmt.Sprintf("Autodetected %s blob type as %s.", jptm.Info().Source , intendedType))
		// TODO: Log these? @JohnRusk and @zezha-msft this creates quite a bit of spam in the logs but is important info.
		// TODO: Perhaps we should log it only if it isn't a block blob?
	}

	if jptm.Info().IsFolderPropertiesTransfer() {
		return newBlobFolderSender(jptm, destination, sip)
	} else if jptm.Info().EntityType == common.EEntityType.Symlink() {
		return newBlobSymlinkSender(jptm, destination, sip)
	}

	switch intendedType {
	case blob.BlobTypeBlockBlob:
		return newBlockBlobUploader(jptm, sip)
	case blob.BlobTypePageBlob:
		return newPageBlobUploader(jptm, destination, sip)
	case blob.BlobTypeAppendBlob:
		return newAppendBlobUploader(jptm, destination, sip)
	default:
		return newBlockBlobUploader(jptm, sip) // If no blob type was inferred, assume block blob.
	}
}

const TagsHeaderMaxLength = 2000

// If length of tags <= 2kb, pass it in the header x-ms-tags. Else do a separate SetTags call
func separateSetTagsRequired(tagsMap common.BlobTags) bool {
	tagsLength := 0
	for k, v := range tagsMap {
		tagsLength += len(k) + len(v) + 2
	}

	if tagsLength > TagsHeaderMaxLength {
		return true
	} else {
		return false
	}
}
