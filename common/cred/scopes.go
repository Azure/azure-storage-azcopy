package cred

// Resource used in azure storage OAuth authentication
const Resource = "https://storage.azure.com"
const MDResource = "https://disk.azure.com/" // There must be a trailing slash-- The service checks explicitly for "https://disk.azure.com/"

const StorageScope = "https://storage.azure.com/.default"

const ManagedDiskScope = "https://disk.azure.com//.default" // There must be a trailing slash-- The service checks explicitly for "https://disk.azure.com/"
