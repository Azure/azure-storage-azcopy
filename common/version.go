package common

<<<<<<< HEAD
const AzcopyVersion = "10.30.0~preview.2"
=======
const AzcopyVersion = "10.30.0"
>>>>>>> d555ee723f1e83c232a436f06e27320715994a6b
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
