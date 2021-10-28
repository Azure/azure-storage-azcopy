package azbfs

import (
	"encoding/base64"
	"strings"
)

// Converts metadata into a string of format "key1=value1, key2=value2" and Base64 encodes the values.
func buildMetadataString(md map[string]string) *string {
	if md == nil {
		return nil
	}
	var sb strings.Builder
	var i int
	for key, value := range md {
		sb.WriteString(key)
		sb.WriteRune('=')
		/*
			The service has an internal base64 decode when metadata is copied from ADLS to Storage, so getMetadata
			will work as normal. Doing this encoding for the customers preserves the existing behavior of
			metadata.
		*/
		sb.WriteString(base64.StdEncoding.EncodeToString([]byte(value)))
		if i != len(md)-1 {
			sb.WriteRune(',')
		}
		i += 1
	}
	mdStr := sb.String()
	return &mdStr
}
