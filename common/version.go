package common

const AzcopyVersion = "10.32.1"
const UserAgent = "AzCopy/" + AzcopyVersion
const S3ImportUserAgent = "S3Import " + UserAgent
const GCPImportUserAgent = "GCPImport " + UserAgent
const BenchmarkUserAgent = "Benchmark " + UserAgent

// AddUserAgentPrefix appends the global user agent prefix, if applicable
func AddUserAgentPrefix(userAgent string) string {
	// In this case we don't lookup and use the OK value, because that behavior would be visibly incorrect.
	prefix := EEnvironmentVariable.UserAgentPrefix().Value()
	if len(prefix) > 0 {
		userAgent = prefix + " " + userAgent
	}

	return userAgent
}
