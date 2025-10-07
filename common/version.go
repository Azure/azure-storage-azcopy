package common

const AzcopyVersion = "10.31.0~preview.1"
const UserAgent = "AzCopy/" + AzcopyVersion
const S3ImportUserAgent = "S3Import " + UserAgent
const GCPImportUserAgent = "GCPImport " + UserAgent
const BenchmarkUserAgent = "Benchmark " + UserAgent

// AddUserAgentPrefix appends the global user agent prefix, if applicable
func AddUserAgentPrefix(userAgent string) string {
	prefix := GetEnvironmentVariable(EEnvironmentVariable.UserAgentPrefix())
	if len(prefix) > 0 {
		userAgent = prefix + " " + userAgent
	}

	return userAgent
}
