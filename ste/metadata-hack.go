package ste

import (
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"regexp"
)

var ormetadataregex = regexp.MustCompile("((([A-F]|[a-f]|[0-9])+-?)+_?){2}")

// hacky hack, this does hacky things.
func FixBustedMetadata(m common.Metadata) common.Metadata {
	// copy all metadata
	out := common.Metadata{}

	for k, v := range m {
		if ormetadataregex.MatchString(k) {
			continue // ignore or metadata
		}

		out[k] = v
	}

	return out
}
