package common

import (
	"bytes"
	"net/http"
	"net/url"
	"strings"

	"github.com/Azure/azure-storage-file-go/azfile"
)

/////////////////////////////////////////////////////////////////////////////////////////////////
type URLStringExtension string

func (s URLStringExtension) RedactSecretQueryParamForLogging() string {
	u, err := url.Parse(string(s))
	if err != nil {
		return string(s)
	}
	ue := &URLExtension{URL: *u}

	// redact sig= in Azure
	ue = ue.RedactSigQueryParamForLogging()

	// rediact x-amx-signature in S3
	ue = ue.RedactAmzSignatureQueryParamForLogging()

	return ue.String()
}

/////////////////////////////////////////////////////////////////////////////////////////////////
type URLExtension struct {
	url.URL
}

func (u *URLExtension) RedactSigQueryParamForLogging() *URLExtension {
	if ok, rawQuery := redactSigQueryParam(u.RawQuery, "sig"); ok {
		u.RawQuery = rawQuery
	}

	return u
}

func (u *URLExtension) RedactAmzSignatureQueryParamForLogging() *URLExtension {
	if ok, rawQuery := redactSigQueryParam(u.RawQuery, "x-amz-signature"); ok {
		u.RawQuery = rawQuery
	}

	return u
}

func redactSigQueryParam(rawQuery, queryKeyNeedRedact string) (bool, string) {
	rawQuery = strings.ToLower(rawQuery) // lowercase the string so we can look for ?[queryKeyNeedRedact] and &[queryKeyNeedRedact]=
	sigFound := strings.Contains(rawQuery, "?"+queryKeyNeedRedact+"=")
	if !sigFound {
		sigFound = strings.Contains(rawQuery, "&"+queryKeyNeedRedact+"=")
		if !sigFound {
			return sigFound, rawQuery // [?|&][queryKeyNeedRedact]= not found; return same rawQuery passed in (no memory allocation)
		}
	}
	// [?|&][queryKeyNeedRedact]= found, redact its value
	values, _ := url.ParseQuery(rawQuery)
	for name := range values {
		if strings.EqualFold(name, queryKeyNeedRedact) {
			values[name] = []string{"REDACTED"}
		}
	}
	return sigFound, values.Encode()
}

/////////////////////////////////////////////////////////////////////////////////////////////////
type FileURLPartsExtension struct {
	azfile.FileURLParts
}

func (parts FileURLPartsExtension) GetShareURL() url.URL {
	parts.DirectoryOrFilePath = ""
	return parts.URL()
}

func (parts FileURLPartsExtension) GetServiceURL() url.URL {
	parts.ShareName = ""
	parts.DirectoryOrFilePath = ""
	return parts.URL()
}

/////////////////////////////////////////////////////////////////////////////////////////////////
type HTTPResponseExtension struct {
	*http.Response
}

// IsSuccessStatusCode checks if response's status code is contained in specified success status codes.
func (r HTTPResponseExtension) IsSuccessStatusCode(successStatusCodes ...int) bool {
	if r.Response == nil {
		return false
	}
	for _, i := range successStatusCodes {
		if i == r.StatusCode {
			return true
		}
	}
	return false
}

/////////////////////////////////////////////////////////////////////////////////////////////////
type ByteSlice []byte
type ByteSliceExtension struct {
	ByteSlice
}

// RemoveBOM removes any BOM from the byte slice
func (bs ByteSliceExtension) RemoveBOM() []byte {
	if bs.ByteSlice == nil {
		return nil
	}
	// UTF8
	return bytes.TrimPrefix(bs.ByteSlice, []byte("\xef\xbb\xbf"))
}
