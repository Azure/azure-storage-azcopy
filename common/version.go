package common

const AzcopyVersion = "10.4.80-preview" // 80-90 range is reserved for private drops of parallel enumeration of local file systems
const UserAgent = "AzCopy/" + AzcopyVersion
const S3ImportUserAgent = "S3Import " + UserAgent
const BenchmarkUserAgent = "Benchmark " + UserAgent
