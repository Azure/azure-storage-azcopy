package common

import (
	"net"
	"net/url"
	"strings"
)

var AzcopyJobPlanFolder string
var AzcopyCurrentJobLogger ILoggerResetable

// isIPEndpointStyle checkes if URL's host is IP, in this case the storage account endpoint will be composed as:
// http(s)://IP(:port)/storageaccount/container/...
// As url's Host property, host could be both host or host:port
func isIPEndpointStyle(host string) bool {
	if host == "" {
		return false
	}
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	// For IPv6, there could be case where SplitHostPort fails for cannot finding port.
	// In this case, eliminate the '[' and ']' in the URL.
	// For details about IPv6 URL, please refer to https://tools.ietf.org/html/rfc2732
	if host[0] == '[' && host[len(host)-1] == ']' {
		host = host[1 : len(host)-1]
	}
	return net.ParseIP(host) != nil
}

// TargetPathExcludingContainer returns blob/file/dir path excluding container.
// Ex. For input https://account1.blob.core.windows.net/container1/a/b/c/d
// a/b/c/d is returned.
func TargetPathExcludingContainer(u string) (string, error) {
	uri, err := url.Parse(u)
	if err != nil {
		return "", err
	}

	if uri.Path == "" {
		return "", nil
	}

	path := uri.Path
	if path[0] == '/' {
		path = path[1:]
	}

	if isIPEndpointStyle(uri.Host) {
		if accountEndIndex := strings.Index(path, "/"); accountEndIndex == -1 {
			// Slash not found; path has account name & no container name or blob
			return "", nil
		} else {
			path = path[accountEndIndex+1:]// path refers to portion after the account name now (container & blob names)
		}
	}

	containerEndIndex := strings.Index(path, "/") // Find the next slash (if it exists)
	if containerEndIndex == -1 {// Slash not found; path has container name & no blob name
		return "", nil
	}

	return path[containerEndIndex+1:], nil
}

func VerifyIsURLResolvable(url_string string) (error) {
	/* This function is disabled. But we should still fix this after fixing the below stuff.
	 * We can take this up after migration to new SDK. The pipeline infra may not be same then.
	 * 1. At someplaces we use Blob SDK directly to create pipeline - ex getBlobCredentialType()
	 *    We should create pipeline through helper functions create<Blob/File/blobfs>pipeline, where we
	 *    handle errors appropriately.
	 * 2. We should either do a http.Get or net.Dial instead of lookIP. If we are behind a proxy, we may 
	 *    not resolve this IP. #2144
	 * 3. DNS errors may by temporary, we should try for a minute before we give up.
	 */
	return nil
	/*
	url, err := url.Parse(url_string)
	if (err != nil) {
		return err
	}

	_, err = net.LookupIP(url.Host)
	return err
	*/
}