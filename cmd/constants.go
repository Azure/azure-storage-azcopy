package cmd

// Common Error and Info messages
const (
	PreservePOSIXPropertiesErrMsg = "To use --preserve-posix-properties, both the source and destination must be POSIX-aware. Valid combinations are: Linux -> Blob, Blob -> Linux, or Blob -> Blob."
	PreservePermissionsInfoMsg    = "Note: The preserve-permissions flag is set to false. As a result, AzCopy will not copy SMB ACLs between the source and destination. For more information, visit: https://aka.ms/AzCopyandAzureFiles."

	PreserveNFSPermissions = "Note: The preserve-nfs-permissions flag is set to false. As a result, AzCopy will not copy NFS permissions between the source and destination."
)
