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
	"fmt"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/file"
	"github.com/Azure/azure-storage-azcopy/v10/common"
)

type azureFileUploader struct {
	azureFileSenderBase
	md5Channel chan []byte
}

func newAzureFilesUploader(jptm IJobPartTransferMgr, destination string, pacer pacer, sip ISourceInfoProvider) (sender, error) {
	senderBase, err := newAzureFileSenderBase(jptm, destination, pacer, sip)
	if err != nil {
		return nil, err
	}

	return &azureFileUploader{azureFileSenderBase: *senderBase, md5Channel: newMd5Channel()}, nil
}

func (u *azureFileUploader) Md5Channel() chan<- []byte {
	return u.md5Channel
}

func (u *azureFileUploader) GenerateUploadFunc(id common.ChunkID, blockIndex int32, reader common.SingleChunkReader, chunkIsWholeFile bool) chunkFunc {

	return createSendToRemoteChunkFunc(u.jptm, id, func() {
		jptm := u.jptm

		defer reader.Close() // In case of memory leak in sparse file case.

		if jptm.Info().SourceSize == 0 {
			// nothing to do, since this is a dummy chunk in a zero-size file, and the prologue will have done all the real work
			return
		}

		if reader.HasPrefetchedEntirelyZeros() {
			// for this destination type, there is no need to upload ranges than consist entirely of zeros
			jptm.Log(common.LogDebug,
				fmt.Sprintf("Not uploading range from %d to %d,  all bytes are zero",
					id.OffsetInFile(), id.OffsetInFile()+reader.Length()))
			return
		}

		// upload the byte range represented by this chunk
		jptm.LogChunkStatus(id, common.EWaitReason.Body())
		body := newPacedRequestBody(u.ctx, reader, u.pacer)
		_, err := u.getFileClient().UploadRange(u.ctx, id.OffsetInFile(), body, nil)
		if err != nil {
			jptm.FailActiveUpload("Uploading range", err)
			return
		}
	})
}

func (u *azureFileUploader) Epilogue() {
	u.azureFileSenderBase.Epilogue()

	jptm := u.jptm

	// set content MD5 (only way to do this is to re-PUT all the headers, this time with the MD5 included)
	if jptm.IsLive() {
		tryPutMd5Hash(jptm, u.md5Channel, func(md5Hash []byte) error {
			if len(md5Hash) == 0 {
				return nil
			}

			u.headersToApply.ContentMD5 = md5Hash
			_, err := u.getFileClient().SetHTTPHeaders(u.ctx, &file.SetHTTPHeadersOptions{
				HTTPHeaders:   &u.headersToApply,
				Permissions:   &u.permissionsToApply,
				SMBProperties: &u.smbPropertiesToApply,
			})
			return err
		})
	}
}

func (s *azureFileUploader) GenerateCopyMetadata(id common.ChunkID) chunkFunc {
	return createChunkFunc(true, s.jptm, id, func() {
		info := s.jptm.Info()
		var err error

		if common.IsNFSCopy() {
			_, err = s.addNFSPermissionsToHeaders(info, s.getFileClient().URL())
			if err != nil {
				s.jptm.FailActiveSend("Setting file permissions", err)
				return
			}

			_, err = s.addNFSPropertiesToHeaders(info)
			if err != nil {
				s.jptm.FailActiveSend("Setting file properties", err)
				return
			}

		} else {
			_, err = s.addPermissionsToHeaders(info, s.getFileClient().URL())
			if err != nil {
				s.jptm.FailActiveSend("Setting file permissions", err)
				return
			}

			_, err = s.addSMBPropertiesToHeaders(info)
			if err != nil {
				s.jptm.FailActiveSend("Setting file properties", err)
				return
			}
		}

		err = common.DoWithOverrideReadOnlyOnAzureFiles(s.ctx,
			func() (interface{}, error) {
				var resp interface{}
				var err error
				_, err = s.getFileClient().SetMetadata(s.ctx, &file.SetMetadataOptions{Metadata: s.metadataToApply})
				if err != nil {
					return nil, err
				}
				if common.IsNFSCopy() {
					resp, err = s.getFileClient().SetHTTPHeaders(s.ctx, &file.SetHTTPHeadersOptions{
						HTTPHeaders: &s.headersToApply,
						NFSProperties: &file.NFSProperties{
							CreationTime:  s.nfsPropertiesToApply.CreationTime,
							LastWriteTime: s.nfsPropertiesToApply.LastWriteTime,
							FileMode:      s.nfsPropertiesToApply.FileMode,
							Owner:         s.nfsPropertiesToApply.Owner,
							Group:         s.nfsPropertiesToApply.Group,
						},
					})
					if err != nil {
						s.jptm.FailActiveSend("Applying final attribute settings", err)
						return nil, err
					}
				} else {
					resp, err = s.getFileClient().SetHTTPHeaders(s.ctx, &file.SetHTTPHeadersOptions{
						HTTPHeaders:   &s.headersToApply,
						Permissions:   &s.permissionsToApply,
						SMBProperties: &s.smbPropertiesToApply,
					})
					if err != nil {
						s.jptm.FailActiveSend("Applying final attribute settings", err)
						return nil, err
					}
				}
				return resp, nil
			},
			s.fileOrDirClient,
			s.jptm.GetForceIfReadOnly())

		if err != nil {
			s.jptm.FailActiveUpload("Applying final attribute settings", err)
			return
		}
	})
}
