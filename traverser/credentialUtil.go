package traverser

import (
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/ste"
)

// ==============================================================================================
// pipeline factory methods
// ==============================================================================================
// CreateClientOptions creates generic client options which are required to create any
// client to interact with storage service. Default options are modified to suit azcopy.
// srcCred is required in cases where source is authenticated via oAuth for S2S transfers
func CreateClientOptions(logger common.ILoggerResetable, srcCred *common.ScopedToken, reauthCred *common.ScopedAuthenticator) azcore.ClientOptions {
	logOptions := ste.LogOptions{}

	if logger != nil {
		logOptions.RequestLogOptions.SyslogDisabled = common.IsForceLoggingDisabled()
		logOptions.Log = logger.Log
		logOptions.ShouldLog = logger.ShouldLog
	}
	// Job-level/global client if available so we reuse connections and transports.
	client := common.GetGlobalHTTPClient(logger)

	return ste.NewClientOptions(
		policy.RetryOptions{
			MaxRetries:    ste.UploadMaxTries,
			TryTimeout:    ste.UploadTryTimeout,
			RetryDelay:    ste.UploadRetryDelay,
			MaxRetryDelay: ste.UploadMaxRetryDelay,
		},
		policy.TelemetryOptions{
			ApplicationID: common.AddUserAgentPrefix(common.UserAgent),
		},
		client, /*Use common.NewTracingTransport(client, "createClientOptions", logger) for http.Trace*/
		logOptions,
		srcCred,
		reauthCred)
}
