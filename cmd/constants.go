// Copyright © 2025 Microsoft <azcopydev@microsoft.com>
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
package cmd

// Common Error and Info messages
const (
	PreservePOSIXPropertiesIncompatibilityMsg = "to use the --preserve-posix-properties flag, both the source and destination must be POSIX-aware. Valid combinations are: Linux -> Blob, Blob -> Linux, or Blob -> Blob"
	PreservePermissionsDisabledMsg            = "Note: The preserve-permissions flag is set to false. As a result, AzCopy will not copy SMB ACLs between the source and destination. For more information, visit: https://aka.ms/AzCopyandAzureFiles."

	PreserveNFSPermissionsDisabledMsg = "Note: The preserve-permissions flag is set to false. As a result, AzCopy will not copy NFS permissions between the source and destination."
	InvalidFlagsForNFSMsg             = "nfs copy cannot be used with SMB-related flags. Please use the --preserve-info or --preserve-permissions flags instead"
	DstShareDoesNotExists             = "the destination file share does not exist; please create it manually with the required quota and settings before running the copy —refer to https://learn.microsoft.com/en-us/azure/storage/files/storage-how-to-create-file-share?tabs=azure-portal for SMB or https://learn.microsoft.com/en-us/azure/storage/files/storage-files-quick-create-use-linux for NFS."
)

// Flags associated with copy and sync commands
const (
	RequestPriorityFlag        = "request-priority"
	PreserveSMBInfoFlag        = "preserve-smb-info"
	PreserveSMBPermissionsFlag = "preserve-smb-permissions"
	PreservePermissionsFlag    = "preserve-permissions"
	PreserveInfoFlag           = "preserve-info"
	IsNFSProtocolFlag          = "nfs"
	HardlinksFlag              = "hardlinks"
)

const (
	pipingUploadParallelism = 5
	pipingDefaultBlockSize  = 8 * 1024 * 1024
	pipeLocation            = "~pipe~"
)

const (
	// For networking throughput in Mbps, (and only for networking), we divide by 1000*1000 (not 1024 * 1024) because
	// networking is traditionally done in base 10 units (not base 2).
	// E.g. "gigabit ethernet" means 10^9 bits/sec, not 2^30. So by using base 10 units
	// we give the best correspondence to the sizing of the user's network pipes.
	// See https://networkengineering.stackexchange.com/questions/3628/iec-or-si-units-binary-prefixes-used-for-network-measurement
	// NOTE that for everything else in the app (e.g. sizes of files) we use the base 2 units (i.e. 1024 * 1024) because
	// for RAM and disk file sizes, it is conventional to use the power-of-two-based units.
	base10Mega = 1000 * 1000
)

// credentials related consts
const (
	oauthLoginSessionCacheKeyName     = "AzCopyOAuthTokenCache"
	oauthLoginSessionCacheServiceName = "AzCopyV10"
	oauthLoginSessionCacheAccountName = "AzCopyOAuthTokenCache"
	trustedSuffixesNameAAD            = "trusted-microsoft-suffixes"
	trustedSuffixesAAD                = "*.core.windows.net;*.core.chinacloudapi.cn;*.core.cloudapi.de;*.core.usgovcloudapi.net;*.storage.azure.net"
)

const NumOfFilesPerDispatchJobPart = 10000
