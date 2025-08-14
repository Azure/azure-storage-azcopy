package common

import (
	"github.com/JeffreyRichter/enum/enum"
	"reflect"
	"sync"
	"time"
)

// AzCopyHashDataStream is used as both the name of a data stream, xattr key, and the suffix of os-agnostic hash data files.
// The local traverser intentionally skips over files with this suffix.
const AzCopyHashDataStream = `.azcopysyncmeta`

type SyncHashData struct {
	Mode SyncHashType
	Data string // base64 encoded
	LMT  time.Time
}

// LocalHashStorageMode & LocalHashDir are temporary global variables pending some level of refactor on parameters
var LocalHashStorageMode = EHashStorageMode.Default()
var LocalHashDir = ""

var hashDataFailureLogOnce = &sync.Once{}

func LogHashStorageFailure() {
	hashDataFailureLogOnce.Do(func() {
		lcm.OnInfo("One or more hash storage operations (read/write) have failed. Check the scanning log for details.")
	})
}

type HashStorageMode uint8

var EHashStorageMode = HashStorageMode(0)

func (HashStorageMode) HiddenFiles() HashStorageMode { return 0 }

func (e *HashStorageMode) Default() HashStorageMode {
	if defaulter, ok := any(e).(interface{ osDefault() HashStorageMode }); ok { // allow specific OSes to override the default functionality
		return defaulter.osDefault()
	}

	return e.HiddenFiles()
}

func (mode HashStorageMode) IsOSAgnostic() bool {
	return mode == EHashStorageMode.HiddenFiles()
}

func (mode HashStorageMode) String() string {
	return enum.StringInt(mode, reflect.TypeOf(mode))
}

func (mode *HashStorageMode) Parse(s string) error {
	val, err := enum.ParseInt(reflect.TypeOf(mode), s, true, true)
	if err == nil {
		*mode = val.(HashStorageMode)
	}
	return err
}

// NewHashDataAdapter is a function that creates a new HiddenFileDataAdapter on systems that do not override the default functionality.
var NewHashDataAdapter = func(hashPath, dataPath string, mode HashStorageMode) (HashDataAdapter, error) {
	return &HiddenFileDataAdapter{hashPath, dataPath}, nil
}

// HashDataAdapter implements an interface to pull and set hash data on files based upon a relative path
type HashDataAdapter interface {
	GetHashData(relativePath string) (*SyncHashData, error)
	SetHashData(relativePath string, data *SyncHashData) error
	GetMode() HashStorageMode // for testing purposes primarily, should be a static value.
}
