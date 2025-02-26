package cmd

// Common Error and Info messages
const (
	PreservePOSIXPropertiesIncompatibilityMsg = "to use --preserve-posix-properties, both the source and destination must be POSIX-aware. Valid combinations are: Linux -> Blob, Blob -> Linux, or Blob -> Blob"
	PreservePermissionsDisabledMsg            = "Note: The preserve-permissions flag is set to false. As a result, AzCopy will not copy SMB ACLs between the source and destination. For more information, visit: https://aka.ms/AzCopyandAzureFiles."

	PreserveNFSPermissionsDisabledMsg = "Note: The preserve-nfs-permissions flag is set to false. As a result, AzCopy will not copy NFS permissions between the source and destination."
)

// Flags associated with copy and sync commands
const (
	PreserveSMBInfoFlag        = "preserve-smb-info"
	PreservePermissionsFlag    = "preserve-permissions"
	PreserveNFSInfoFlag        = "preserve-nfs-info"
	PreserveNFSPermissionsFlag = "preserve-nfs-permissions"
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

// credentials relates consts
const (
	oauthLoginSessionCacheKeyName     = "AzCopyOAuthTokenCache"
	oauthLoginSessionCacheServiceName = "AzCopyV10"
	oauthLoginSessionCacheAccountName = "AzCopyOAuthTokenCache"
	trustedSuffixesNameAAD            = "trusted-microsoft-suffixes"
	trustedSuffixesAAD                = "*.core.windows.net;*.core.chinacloudapi.cn;*.core.cloudapi.de;*.core.usgovcloudapi.net;*.storage.azure.net"
)
