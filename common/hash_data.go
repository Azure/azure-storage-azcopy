package common

import "time"

// AzCopyHashDataStream is used as both the name of a data stream, xattr key, and the suffix of os-agnostic hash data files.
// The local traverser intentionally skips over files with this suffix.
const AzCopyHashDataStream = `.azcopysyncmeta`

type SyncHashData struct {
	Mode SyncHashType
	Data string // base64 encoded
	LMT  time.Time
}
