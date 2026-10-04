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
	"io"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

// Abstraction of the methods needed to download files/blobs from a remote location
type downloader interface {
	// Prologue does any necessary first-time setup
	Prologue(jptm IJobPartTransferMgr)

	// GenerateDownloadFunc returns a func() that will download the specified portion of the remote file into dstFile
	// Instead of taking destination file as a parameter, it takes a helper that will write to the file. That keeps details of
	// file IO out out the download func, and lets that func concentrate only on the details of the remote endpoint
	GenerateDownloadFunc(jptm IJobPartTransferMgr, writer common.ChunkedFileWriter, id common.ChunkID, length int64, pacer pacer) chunkFunc

	// Epilogue does cleanup. MAY be the only method that gets called (in error cases). So must not fail simply because
	// Prologue has not yet been called
	Epilogue()
}

// creationTimeDownloader is a downloader that has custom functionality for creating files
// This is currently only utilized on Linux for persisting file type and reference device (folder, symlink, FIFO, etc.)
type creationTimeDownloader interface {
	downloader
	// CreateFile is expected to handle
	// in some cases (e.g. symlinks) the file may be > 0 bytes, but not need any chunks written.
	CreateFile(jptm IJobPartTransferMgr, destination string, size int64, writeThrough bool, t FolderCreationTracker) (file io.WriteCloser, needWriteChunks bool, err error)
}

type unixPropertyAwareDownloader interface {
	downloader

	ApplyUnixProperties(adapter common.UnixStatAdapter) (stage string, err error)
}

// folderDownloader is a downloader that can also process folder properties
type folderDownloader interface {
	downloader
	SetFolderProperties(jptm IJobPartTransferMgr) error
}

// symlinkDownloader is a downloader that can also handle symbolic links.
type symlinkDownloader interface {
	downloader
	CreateSymlink(jptm IJobPartTransferMgr) error
}

// smbPropertyAwareDownloader is a windows and linux triggered interface.
// Code outside of windows-specific files shouldn't implement this ever.
type smbPropertyAwareDownloader interface {
	PutSMBProperties(sip ISMBPropertyBearingSourceInfoProvider, txInfo *TransferInfo) error
}

type smbACLAwareDownloader interface {
	PutSDDL(sip ISMBPropertyBearingSourceInfoProvider, txInfo *TransferInfo) error
}

// nfsPropertyAwareDownloader
type nfsPropertyAwareDownloader interface {
	PutNFSProperties(sip INFSPropertyBearingSourceInfoProvider, txInfo *TransferInfo) error
}

// nfsPermissionsAwareDownloader
type nfsPermissionsAwareDownloader interface {
	PutNFSPermissions(sip INFSPropertyBearingSourceInfoProvider, txInfo *TransferInfo) error
}

type downloaderFactory func(jptm IJobPartTransferMgr) (downloader, error)

func createDownloadChunkFunc(jptm IJobPartTransferMgr, id common.ChunkID, body func()) chunkFunc {
	// If uploading, we set the chunk status to done as soon as the chunkFunc completes.
	// But we don't do that for downloads, since for those the chunk is not "done" until its flushed out
	// by the ChunkedFileWriter. (The ChunkedFileWriter will set the status to done at that time.)
	return createChunkFunc(false, jptm, id, body)
}
