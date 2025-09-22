package cmd

import (
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
)

// errorURLs - map of error codes that currently have shorthand URLs
var errorURLs = map[bloberror.Code]string{
	bloberror.InvalidOperation:              "https://aka.ms/AzCopyError/InvalidOperation",
	bloberror.MissingRequiredQueryParameter: "https://aka.ms/AzCopyError/MissingRequiredQueryParameter",
	bloberror.InvalidHeaderValue:            "https://aka.ms/AzCopyError/InvalidHeaderValue",
	bloberror.InvalidAuthenticationInfo:     "https://aka.ms/AzCopyError/InvalidAuthenticationInfo",
	bloberror.NoAuthenticationInformation:   "https://aka.ms/AzCopyError/NoAuthenticationInformation",
	bloberror.AuthenticationFailed:          "https://aka.ms/AzCopyError/AuthenticationFailed",
	bloberror.AccountIsDisabled:             "https://aka.ms/AzCopyError/AccountIsDisabled",
	bloberror.ResourceNotFound:              "https://aka.ms/AzCopyError/ResourceNotFound",
	bloberror.ResourceTypeMismatch:          "https://aka.ms/AzCopyError/ResourceTypeMismatch",
	//bloberror.CannotVerifyCopySource:        "https://aka.ms/AzCopyError/CannotVerifyCopySource",
	bloberror.ServerBusy: "https://aka.ms/AzCopyError/ServerBusy",
}

// getErrorCodeUrl - returns url string for specific error codes
func getErrorCodeUrl(err error) string {
	var urls []string
	for code, url := range errorURLs {
		if hasCode(err, code) {
			urls = append(urls, url)
		}
	}

	if len(urls) > 0 {
		return "ERROR DETAILS: " + strings.Join(urls, "; ")
	}

	return "" // We do not currently have a URL for this specific error code
}

// hasCode - checks if err contains blob error code
func hasCode(err error, code bloberror.Code) bool {
	return strings.Contains(err.Error(), string(code))
}
