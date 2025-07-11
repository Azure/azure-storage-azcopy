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
	"context"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-storage-azcopy/v10/common"
)

var _ IJobPartTransferMgr = &testJobPartTransferManager{}

type testJobPartTransferManager struct {
	info       *TransferInfo
	fromTo     common.FromTo
	jobPartMgr jobPartMgr
	ctx        context.Context
	status     common.TransferStatus
}

func (t *testJobPartTransferManager) DeleteDestinationFileIfNecessary() bool {
	return t.jobPartMgr.DeleteDestinationFileIfNecessary()
}

func (t *testJobPartTransferManager) Info() *TransferInfo {
	return t.info
}

func (t *testJobPartTransferManager) SrcServiceClient() *common.ServiceClient {
	options := t.S2SSourceClientOptions()
	var azureFileSpecificOptions any
	if t.fromTo.From() == common.ELocation.File() {
		azureFileSpecificOptions = &common.FileClientOptions{
			AllowTrailingDot: true,
		}
	}

	client, _ := common.GetServiceClientForLocation(
		t.fromTo.From(),
		common.ResourceString{Value: t.info.Source},
		t.S2SSourceCredentialInfo().CredentialType,
		t.S2SSourceCredentialInfo().OAuthTokenInfo.TokenCredential,
		&options,
		azureFileSpecificOptions,
	)
	return client
}

func (t *testJobPartTransferManager) DstServiceClient() *common.ServiceClient {
	options := t.ClientOptions()
	var azureFileSpecificOptions any
	if t.fromTo.To() == common.ELocation.File() {
		azureFileSpecificOptions = &common.FileClientOptions{
			AllowTrailingDot:       true,
			AllowSourceTrailingDot: true,
		}
	}

	client, _ := common.GetServiceClientForLocation(
		t.fromTo.To(),
		common.ResourceString{Value: t.info.Destination},
		t.CredentialInfo().CredentialType,
		t.CredentialInfo().OAuthTokenInfo.TokenCredential,
		&options,
		azureFileSpecificOptions,
	)
	return client
}

func (t *testJobPartTransferManager) SourceTrailingDot() *common.TrailingDotOption {
	if (t.fromTo.IsS2S() || t.fromTo.IsDownload()) && (t.fromTo.From() == common.ELocation.File()) {
		return to.Ptr(common.ETrailingDotOption.Enable())
	}
	return nil
}

func (t *testJobPartTransferManager) TrailingDot() *common.TrailingDotOption {
	return to.Ptr(common.ETrailingDotOption.Enable())
}

func (t *testJobPartTransferManager) From() *common.Location {
	return to.Ptr(t.fromTo.From())
}

func (t *testJobPartTransferManager) FromTo() common.FromTo {
	return t.fromTo
}

func (t *testJobPartTransferManager) ResourceDstData(dataFileToXfer []byte) (headers common.ResourceHTTPHeaders, metadata common.Metadata, blobTags common.BlobTags, cpkOptions common.CpkOptions) {
	panic("implement me")
}

func (t *testJobPartTransferManager) LastModifiedTime() time.Time {
	panic("implement me")
}

func (t *testJobPartTransferManager) PreserveLastModifiedTime() (time.Time, bool) {
	panic("implement me")
}

func (t *testJobPartTransferManager) ShouldPutMd5() bool {
	panic("implement me")
}

func (t *testJobPartTransferManager) MD5ValidationOption() common.HashValidationOption {
	panic("implement me")
}

func (t *testJobPartTransferManager) BlobTypeOverride() common.BlobType {
	panic("implement me")
}

func (t *testJobPartTransferManager) BlobTiers() (blockBlobTier common.BlockBlobTier, pageBlobTier common.PageBlobTier) {
	panic("implement me")
}

func (t *testJobPartTransferManager) JobHasLowFileCount() bool {
	panic("implement me")
}

func (t *testJobPartTransferManager) Context() context.Context {
	return context.Background()
}

func (t *testJobPartTransferManager) SlicePool() common.ByteSlicePooler {
	panic("implement me")
}

func (t *testJobPartTransferManager) CacheLimiter() common.CacheLimiter {
	panic("implement me")
}

func (t *testJobPartTransferManager) WaitUntilLockDestination(ctx context.Context) error {
	panic("implement me")
}

func (t *testJobPartTransferManager) EnsureDestinationUnlocked() {
	panic("implement me")
}

func (t *testJobPartTransferManager) HoldsDestinationLock() bool {
	panic("implement me")
}

func (t *testJobPartTransferManager) StartJobXfer() {
	panic("implement me")
}

func (t *testJobPartTransferManager) GetOverwriteOption() common.OverwriteOption {
	panic("implement me")
}

func (t *testJobPartTransferManager) GetForceIfReadOnly() bool {
	panic("implement me")
}

func (t *testJobPartTransferManager) ShouldDecompress() bool {
	panic("implement me")
}

func (t *testJobPartTransferManager) GetSourceCompressionType() (common.CompressionType, error) {
	panic("implement me")
}

func (t *testJobPartTransferManager) ReportChunkDone(id common.ChunkID) (lastChunk bool, chunksDone uint32) {
	panic("implement me")
}

func (t *testJobPartTransferManager) TransferStatusIgnoringCancellation() common.TransferStatus {
	panic("implement me")
}

func (t *testJobPartTransferManager) SetStatus(status common.TransferStatus) {
	t.status = status
}

func (t *testJobPartTransferManager) SetErrorCode(errorCode int32) {
	panic("implement me")
}

func (t *testJobPartTransferManager) SetNumberOfChunks(numChunks uint32) {
	panic("implement me")
}

func (t *testJobPartTransferManager) SetActionAfterLastChunk(f func()) {
	panic("implement me")
}

func (t *testJobPartTransferManager) ReportTransferDone() uint32 {
	// return value is the no of transfer's done for this job part.
	return 1
}

func (t *testJobPartTransferManager) RescheduleTransfer() {
	panic("implement me")
}

func (t *testJobPartTransferManager) ScheduleChunks(chunkFunc chunkFunc) {
	panic("implement me")
}

func (t *testJobPartTransferManager) SetDestinationIsModified() {
	panic("implement me")
}

func (t *testJobPartTransferManager) Cancel() {
	panic("implement me")
}

func (t *testJobPartTransferManager) WasCanceled() bool {
	panic("implement me")
}

func (t *testJobPartTransferManager) IsLive() bool {
	panic("implement me")
}

func (t *testJobPartTransferManager) IsDeadBeforeStart() bool {
	panic("implement me")
}

func (t *testJobPartTransferManager) IsDeadInflight() bool {
	panic("implement me")
}

func (t *testJobPartTransferManager) OccupyAConnection() {
	panic("implement me")
}

func (t *testJobPartTransferManager) ReleaseAConnection() {
	panic("implement me")
}

func (t *testJobPartTransferManager) CredentialInfo() common.CredentialInfo {
	panic("implement me")
}

func (t *testJobPartTransferManager) ClientOptions() azcore.ClientOptions {
	panic("implement me")
}

func (t *testJobPartTransferManager) S2SSourceCredentialInfo() common.CredentialInfo {
	return common.CredentialInfo{CredentialType: common.ECredentialType.Anonymous()}
}

func (t *testJobPartTransferManager) GetS2SSourceTokenCredential(ctx context.Context) (token *string, err error) {
	panic("implement me")
}

func (t *testJobPartTransferManager) S2SSourceClientOptions() azcore.ClientOptions {
	retryOptions := policy.RetryOptions{
		MaxRetries:    UploadMaxTries,
		TryTimeout:    UploadTryTimeout,
		RetryDelay:    UploadRetryDelay,
		MaxRetryDelay: UploadMaxRetryDelay,
	}

	var userAgent string
	if t.fromTo.From() == common.ELocation.S3() {
		userAgent = common.S3ImportUserAgent
	} else if t.fromTo.From() == common.ELocation.GCP() {
		userAgent = common.GCPImportUserAgent
	} else if t.fromTo.From() == common.ELocation.Benchmark() || t.fromTo.To() == common.ELocation.Benchmark() {
		userAgent = common.BenchmarkUserAgent
	} else {
		userAgent = common.AddUserAgentPrefix(common.UserAgent)
	}
	telemetryOptions := policy.TelemetryOptions{ApplicationID: userAgent}

	httpClient := NewAzcopyHTTPClient(4)

	return NewClientOptions(retryOptions, telemetryOptions, httpClient, LogOptions{}, nil, nil)
}

func (t *testJobPartTransferManager) CredentialOpOptions() *common.CredentialOpOptions {
	return nil
}

func (t *testJobPartTransferManager) FailActiveUpload(where string, err error) {
	panic("implement me")
}

func (t *testJobPartTransferManager) FailActiveDownload(where string, err error) {
	panic("implement me")
}

func (t *testJobPartTransferManager) FailActiveUploadWithStatus(where string, err error, failureStatus common.TransferStatus) {
	panic("implement me")
}

func (t *testJobPartTransferManager) FailActiveDownloadWithStatus(where string, err error, failureStatus common.TransferStatus) {
	panic("implement me")
}

func (t *testJobPartTransferManager) FailActiveS2SCopy(where string, err error) {
	panic("implement me")
}

func (t *testJobPartTransferManager) FailActiveS2SCopyWithStatus(where string, err error, failureStatus common.TransferStatus) {
	panic("implement me")
}

func (t *testJobPartTransferManager) FailActiveSend(where string, err error) {
	panic("implement me")
}

func (t *testJobPartTransferManager) FailActiveSendWithStatus(where string, err error, failureStatus common.TransferStatus) {
	panic("implement me")
}

func (t *testJobPartTransferManager) LogUploadError(source, destination, errorMsg string, status int) {
	panic("implement me")
}

func (t *testJobPartTransferManager) LogDownloadError(source, destination, errorMsg string, status int) {
	panic("implement me")
}

func (t *testJobPartTransferManager) LogS2SCopyError(source, destination, errorMsg string, status int) {
	panic("implement me")
}

func (t *testJobPartTransferManager) LogSendError(source, destination, errorMsg string, status int) {
	panic("implement me")
}

func (t *testJobPartTransferManager) LogError(_, _ string, _ error) {}

func (t *testJobPartTransferManager) LogTransferInfo(_ common.LogLevel, _, _, _ string) {}

func (t *testJobPartTransferManager) LogTransferStart(source, destination, description string) {
	panic("implement me")
}

func (t *testJobPartTransferManager) LogChunkStatus(id common.ChunkID, reason common.WaitReason) {
	panic("implement me")
}

func (t *testJobPartTransferManager) ChunkStatusLogger() common.ChunkStatusLogger {
	panic("implement me")
}

func (t *testJobPartTransferManager) LogAtLevelForCurrentTransfer(level common.LogLevel, msg string) {
	panic("implement me")
}

func (t *testJobPartTransferManager) GetOverwritePrompter() *overwritePrompter {
	panic("implement me")
}

func (t *testJobPartTransferManager) GetFolderCreationTracker() FolderCreationTracker {
	panic("implement me")
}

func (t *testJobPartTransferManager) ShouldLog(level common.LogLevel) bool {
	return false
}

func (t *testJobPartTransferManager) Log(level common.LogLevel, msg string) {
}

func (t *testJobPartTransferManager) Panic(err error) {
	panic("implement me")
}

func (t *testJobPartTransferManager) DeleteSnapshotsOption() common.DeleteSnapshotsOption {
	panic("implement me")
}

func (t *testJobPartTransferManager) PermanentDeleteOption() common.PermanentDeleteOption {
	panic("implement me")
}

func (t *testJobPartTransferManager) SecurityInfoPersistenceManager() *securityInfoPersistenceManager {
	panic("implement me")
}

func (t *testJobPartTransferManager) FolderDeletionManager() common.FolderDeletionManager {
	panic("implement me")
}

func (t *testJobPartTransferManager) GetDestinationRoot() string {
	panic("implement me")
}

func (t *testJobPartTransferManager) ShouldInferContentType() bool {
	fromTo := t.FromTo()
	return fromTo.From() == common.ELocation.Local()
}

func (t *testJobPartTransferManager) CpkInfo() *blob.CPKInfo {
	return nil
}

func (t *testJobPartTransferManager) CpkScopeInfo() *blob.CPKScopeInfo {
	return nil
}

func (t *testJobPartTransferManager) IsSourceEncrypted() bool {
	panic("implement me")
}

func (t *testJobPartTransferManager) PropertiesToTransfer() common.SetPropertiesFlags {
	panic("implement me")
}

func (t *testJobPartTransferManager) ResetSourceSize() {
	panic("implement me")
}

func (t *testJobPartTransferManager) SuccessfulBytesTransferred() int64 {
	panic("implement me")
}

func (t *testJobPartTransferManager) TransferIndex() (partNum, transferIndex uint32) {
	panic("implement me")
}

func (t *testJobPartTransferManager) RestartedTransfer() bool {
	return false
}

func (t *testJobPartTransferManager) IsLastModifiedUnixEpoch() bool {
	panic("implement me")
}

// LastModifiedEpochTime returns the last modified time as Unix epoch seconds.
func (t *testJobPartTransferManager) LastModifiedEpochTime() int64 {
	panic("implement me")
}
