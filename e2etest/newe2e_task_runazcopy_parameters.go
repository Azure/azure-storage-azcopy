package e2etest

import (
	"errors"
	"fmt"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"os"
	"reflect"
	"strings"
	"time"
)

// MapFromTags Recursively builds a map[string]string from a reflect.val
// As it searches recursively through the supplied struct,
// The tag searched for is `flag:"name,extdata"`
// All extra data is comma-separated.
// All flags should be nillable, because zero is always a valid state, and must be distinguishable from unspecified.
// available extdata:
// - "default:xyz" Defaults to something non-standard for AzCopy-- E.g. always forcing debug logging
// - "defaultfunc:SpecialDefault" Calls a `func Default*(a ScenarioAsserter) string` named SpecialDefault on the struct. Asserts if not found.
// - "serializer:SerializerFunc" Calls a `func Serialize*(value any, a ScenarioAsserter) string` named SerializerFunc on the struct. Asserts if not found.
// If special characters , or : are for some reason used, \ can be used as an escape.
func MapFromTags(val reflect.Value, tagName string, a ScenarioAsserter) map[string]string {
	queue := []reflect.Value{val}
	out := make(map[string]string)

	for len(queue) != 0 {
		val := queue[0]
		queue = queue[1:]

		ptrVal := val
		for val.Kind() == reflect.Pointer || val.Kind() == reflect.Interface { // deref pointers/interfaces
			val = val.Elem()
		}

		if !val.IsValid() {
			continue
		}

		t := val.Type()
		numField := t.NumField()

		for i := 0; i < numField; i++ {
			key, ok := t.Field(i).Tag.Lookup(tagName)

			if ok {
				field := val.Field(i)
				// break the key down
				tag := parseFlagTag(key)

				switch field.Kind() {
				case reflect.Chan, reflect.Func, reflect.Map,
					reflect.Pointer, reflect.UnsafePointer,
					reflect.Slice, reflect.Interface:
				default:
					a.Error(fmt.Sprintf("Field %s of struct %s must be nillable", t.Field(i).Name, t.Name()))
				}

				tryFindMethod := func(name string) reflect.Value {
					out := val.MethodByName(name) // First, target the real value, this catches non-pointer methods
					if !out.IsValid() {
						out = ptrVal.MethodByName(name) // If that fails, call the pointer version, we shouldn't be double pointering anyway.
					}

					if !out.IsValid() {
						a.Error("could not find method " + name)
					}
					//a.AssertNow("could not find method "+name, Equal{}, out.IsValid(), true)
					return out
				}

				// values should always be nillable
				if field.IsNil() {
					if tag.defaultData != nil { // if we have default data, it's easy.
						out[tag.flagKey] = *tag.defaultData
					} else if tag.defaultFunc != nil {
						// find the function & call it
						result := tryFindMethod(*tag.defaultFunc).Call([]reflect.Value{reflect.ValueOf(a)}) // todo: we could validate that we're getting what we expect, but no need to do that because reflect will panic for us, then the test will catch it.
						out[tag.flagKey] = result[0].String()
					}
				} else {
					if field.Kind() == reflect.Pointer {
						field = field.Elem()
					}

					if tag.serializerFunc != nil {
						result := tryFindMethod(*tag.serializerFunc).Call([]reflect.Value{field, reflect.ValueOf(a)})
						out[tag.flagKey] = result[0].String()
					} else {
						out[tag.flagKey] = fmt.Sprint(field)
					}
				}
			} else if val.Field(i).Kind() == reflect.Struct {
				queue = append(queue, val.Field(i))
			}
		}
	}

	return out
}

type flagTag struct {
	flagKey        string
	defaultData    *string
	defaultFunc    *string
	serializerFunc *string
}

func parseFlagTag(tag string) flagTag {
	//sections := make([]string, 0)
	out := flagTag{}
	var fieldTarget *string
	token := make([]rune, 0)

	var escapeNext bool
	tagRunes := []rune(tag)

	for char := 0; char < len(tagRunes); char++ {
		finalize := func(targetField bool) {
			defer func() { token = make([]rune, 0) }()

			if fieldTarget != nil {
				*fieldTarget = string(token)
				fieldTarget = nil
				return
			}

			if targetField {
				var data string

				switch strings.ToLower(string(token)) {
				case "default":
					out.defaultData = &data
				case "defaultfunc":
					out.defaultFunc = &data
				case "serializer":
					out.serializerFunc = &data
				}

				fieldTarget = &data
			} else {
				out.flagKey = string(token)
			}
		}

		if escapeNext {
			token = append(token, tagRunes[char])
			escapeNext = false
			continue
		}

		switch tag[char] {
		case '\\':
			escapeNext = true
		case ',':
			finalize(false)
		case ':':
			finalize(true)
		case ' ':
			// Space should always be ignored.
			// If it is truly necessary for some sort of default,
			// It should be manually escaped.
			// That said, there are no cases (as of 1/26/2024) where a flag requires a space as any part of the flag
			// as some part of enumeration or otherwise.
		default:
			token = append(token, tagRunes[char])
		}

		if char == len(tagRunes)-1 {
			finalize(false)
		}
	}

	return out
}

// The below structs are intended to be mixed and matched as much as possible,
// such that a variety of verbs can be used with a single struct (e.g. copy and sync)
// in a test, without rewriting all the flags for every use case.

type GlobalFlags struct {
	CapMbps          *float64 `flag:"cap-mbps"`
	TrustedSuffixes  []string `flag:"trusted-microsoft-suffixes"`
	SkipVersionCheck *bool    `flag:"skip-version-check"`

	// TODO : Flags default seems to be broken; WI#26954065
	OutputType  *common.OutputFormat    `flag:"output-type,default:json"`
	LogLevel    *common.LogLevel        `flag:"log-level,default:DEBUG"`
	OutputLevel *common.OutputVerbosity `flag:"output-level,default:DEFAULT"`

	// TODO: reconsider/reengineer this flag; WI#26475473
	//DebugSkipFiles   []string                `flag:"debug-skip-files"`

	// TODO: handle prompting and input; WI#26475441
	//CancelFromStdin *bool `flag:"cancel-from-stdin"`
	AwaitContinue *bool `flag:"await-continue,defaultfunc:DefaultAwaitContinue"`
	//AwaitOpen       *bool `flag:"await-open"`
}

func (GlobalFlags) DefaultAwaitContinue(a ScenarioAsserter) string {
	return common.Iff(isLaunchedByDebugger, "true", "false")
}

type CommonFilterFlags struct {
	IncludePattern []string `flag:"include-pattern,serializer:SerializeStrings"`
	ExcludePattern []string `flag:"exclude-pattern,serializer:SerializeStrings"`

	IncludeRegex []string `flag:"include-regex,serializer:SerializeStrings"`
	ExcludeRegex []string `flag:"include-regex,serializer:SerializeStrings"`

	IncludePath []string `flag:"include-path,serializer:SerializeStrings"`
	ExcludePath []string `flag:"exclude-path,serializer:SerializeStrings"`

	// Copy/remove only
	IncludeBefore *time.Time `flag:"include-before,serializer:SerializeTime"`
	IncludeAfter  *time.Time `flag:"include-after,serializer:SerializeTime"`

	// primarily for testing errors, copy/sync only
	LegacyInclude []string `flag:"include,serializer:SerializeStrings"`
	LegacyExclude []string `flag:"exclude,serializer:SerializeStrings"`

	IncludeAttributes []WindowsAttribute `flag:"include-attributes,serializer:SerializeAttributeList"`
	ExcludeAttributes []WindowsAttribute `flag:"exclude-attributes,serializer:SerializeAttributeList"`
}

func (CommonFilterFlags) SerializeTime(t any, a ScenarioAsserter) string {
	return GetTypeOrAssert[*time.Time](a, t).UTC().Format(time.RFC3339)
}

func (CommonFilterFlags) SerializeStrings(list any, a ScenarioAsserter) string {
	return strings.Join(GetTypeOrAssert[[]string](a, list), ";")
}

func (CommonFilterFlags) SerializeAttributeList(list any, a ScenarioAsserter) string {
	attrs := GetTypeOrAssert[[]WindowsAttribute](a, list)

	out := ""
	for _, v := range attrs {
		if len(out) > 0 {
			out += ";"
		}

		out += WindowsAttributeStrings[v]
	}

	return out
}

// CopySyncCommonFlags is a list of flags with feature parity across copy and sync.
type CopySyncCommonFlags struct {
	GlobalFlags
	CommonFilterFlags

	Recursive           *bool          `flag:"recursive"`
	FromTo              *common.FromTo `flag:"from-to"`
	BlockSizeMB         *float64       `flag:"block-size-mb"`
	PreservePermissions *bool          `flag:"preserve-permissions"`
	// PreserveSMBPermissions refers to explicitly using the classic, deprecated flag, in case we want to validate the warning is spat out.
	PreserveSMBPermissions  *bool                        `flag:"preserve-smb-permissions"`
	PreservePOSIXProperties *bool                        `flag:"preserve-posix-properties"`
	ForceIfReadOnly         *bool                        `flag:"force-if-read-only"`
	PutMD5                  *bool                        `flag:"put-md5"`
	CheckMD5                *common.HashValidationOption `flag:"check-md5"`
	S2SPreserveAccessTier   *bool                        `flag:"s2s-preserve-access-tier"`
	S2SPreserveBlobTags     *bool                        `flag:"s2s-preserve-blob-tags"`
	DryRun                  *bool                        `flag:"dry-run"`
	TrailingDot             *common.TrailingDotOption    `flag:"trailing-dot"`
	CPKByName               *string                      `flag:"cpk-by-name"`
	CPKByValue              *bool                        `flag:"cpk-by-value"`
}

// CopyFlags is a more exclusive struct including flags exclusi
type CopyFlags struct {
	CopySyncCommonFlags

	FollowSymlinks  *bool                 `flag:"follow-symlinks"`
	ListOfFiles     []string              `flag:"list-of-files,serializer:SerializeListingFile"`
	Overwrite       *bool                 `flag:"overwrite"`
	Decompress      *bool                 `flag:"decompress"`
	ExcludeBlobType *common.BlobType      `flag:"exclude-blob-type"`
	BlobType        *common.BlobType      `flag:"blob-type"`
	BlockBlobTier   *common.BlockBlobTier `flag:"block-blob-tier"`
	PageBlobTier    *common.PageBlobTier  `flag:"page-blob-tier"`
	Metadata        common.Metadata       `flag:"metadata,serializer:SerializeMetadata"`

	ContentType        *string `flag:"content-type"`
	ContentEncoding    *string `flag:"content-encoding"`
	ContentDisposition *string `flag:"content-disposition"`
	ContentLanguage    *string `flag:"content-language"`
	CacheControl       *string `flag:"cache-control"`

	NoGuessMimeType *bool `flag:"no-guess-mime-type"`
	PreserveLMT     *bool `flag:"preserve-last-modified-time"`

	AsSubdir         *bool `flag:"as-subdir"`
	PreserveOwner    *bool `flag:"preserve-owner"`
	PreserveSymlinks *bool `flag:"preserve-symlinks"`

	// semi-related WIs for CheckLength present in GlobalFlags (WI#26475473, WI#26475441)
	// goal would be to test the unhappy case of CheckLength=true by altering after enumeration time
	CheckLength *bool `flag:"check-length"`

	S2SPreserveProperties     *bool           `flag:"check-length"`
	S2SDetectSourceChanged    *bool           `flag:"s2s-detect-source-changed"`
	ListOfVersions            []string        `flag:"list-of-versions,serializer:SerializeListingFile"`
	BlobTags                  common.Metadata `flag:"blob-tags,serializer:SerializeTags"`
	IncludeDirectoryStubs     *bool           `flag:"include-directory-stubs"`
	DisableAutoDecoding       *bool           `flag:"disable-auto-decoding"`
	S2SGetPropertiesInBackend *bool           `flag:"s2s-get-properties-in-backend"`
	ADLSFlushThreshold        *uint32         `flag:"flush-threshold"`

	// todo: Privileged environment testing; WI#26542582
	//BackupMode *bool `flag:"backup"`
}

func (CopyFlags) SerializeListingFile(in any, a ScenarioAsserter) string {
	if a.Dryrun() {
		// Dryruns won't actually run AzCopy, and dryruns shouldn't reach this code path, but just in case, we should cover it.
		return "listingfile.txt"
	}

	list := GetTypeOrAssert[[]string](a, in)

	file, err := os.CreateTemp("", "")
	a.NoError("must create temp file", err)
	path := file.Name()
	defer func(file *os.File) {
		_ = file.Close()
	}(file)

	a.Cleanup(func(a ScenarioAsserter) {
		a.NoError("cleanup list file", os.Remove(path))
	})

	for _, v := range list {
		_, err := file.WriteString(v + "\n")
		a.NoError("must write line to temp file", err)
	}

	return path
}

// SerializeKeyValues
// kvsep = what should be between a key and value
// elemsep = what should be between two pairs
func (CopyFlags) SerializeKeyValues(in any, a ScenarioAsserter, kvsep, elemsep string) string {
	meta := GetTypeOrAssert[common.Metadata](a, in)
	out := ""

	for k, v := range meta {
		if v == nil {
			continue
		}

		if len(out) > 0 {
			out += elemsep
		}

		out += fmt.Sprintf("%s%s%s", k, kvsep, *v)
	}

	return out
}

func (c CopyFlags) SerializeMetadata(meta any, a ScenarioAsserter) string {
	return c.SerializeKeyValues(meta, a, "=", ";")
}

func (c CopyFlags) SerializeTags(tags any, a ScenarioAsserter) string {
	return c.SerializeKeyValues(tags, a, "=", "&")
}

type SyncFlags struct {
	CopySyncCommonFlags

	DeleteDestination    *bool                   `flag:"delete-destination"`
	MirrorMode           *bool                   `flag:"mirror-mode"`
	CompareHash          *common.SyncHashType    `flag:"compare-hash"`
	LocalHashDir         *string                 `flag:"hash-meta-dir"`
	LocalHashStorageMode *common.HashStorageMode `flag:"local-hash-storage-mode"`
}

// RemoveFlags is not tiered like CopySyncCommonFlags is, because it is dissimilar in functionality, and would be hard to test in the same scenario.
type RemoveFlags struct {
	GlobalFlags
	CommonFilterFlags

	Recursive       *bool                         `flag:"recursive"`
	ForceIfReadOnly *bool                         `flag:"force-if-read-only"`
	ListOfFiles     []string                      `flag:"list-of-files"`
	ListOfVersions  []string                      `flag:"list-of-versions"`
	DryRun          *bool                         `flag:"dry-run"`
	FromTo          *common.FromTo                `flag:"from-to"`
	PermanentDelete *common.PermanentDeleteOption `flag:"permanent-delete"`
	TrailingDot     *common.TrailingDotOption     `flag:"trailing-dot"`
	CPKByName       *string                       `flag:"cpk-by-name"`
	CPKByValue      *bool                         `flag:"cpk-by-value"`
}

func (r RemoveFlags) SerializeListingFile(in any, scenarioAsserter ScenarioAsserter) {
	CopyFlags{}.SerializeListingFile(in, scenarioAsserter)
}

type ListFlags struct {
	GlobalFlags

	MachineReadable *bool                     `flag:"machine-readable"`
	RunningTally    *bool                     `flag:"running-tally"`
	MegaUnits       *bool                     `flag:"mega-units"`
	Properties      *string                   `flag:"properties"`
	TrailingDot     *common.TrailingDotOption `flag:"trailing-dot"`
}

type WindowsAttribute uint32

const (
	WindowsAttributeReadOnly WindowsAttribute = 1 << iota
	WindowsAttributeHidden
	WindowsAttributeSystemFile
	_ // blanks to increment iota
	_
	WindowsAttributeArchiveReady
	_ // blanks to increment iota
	WindowsAttributeNormalFile
	WindowsAttributeTemporaryFile
	_ // blanks to increment iota
	_
	WindowsAttributeCompressedFile
	WindowsAttributeOfflineFile
	WindowsAttributeNonIndexedFile
	WindowsAttributeEncryptedFile
)

var WindowsAttributeStrings = map[WindowsAttribute]string{
	WindowsAttributeReadOnly:       "R",
	WindowsAttributeHidden:         "H",
	WindowsAttributeSystemFile:     "S",
	WindowsAttributeArchiveReady:   "A",
	WindowsAttributeNormalFile:     "N",
	WindowsAttributeTemporaryFile:  "T",
	WindowsAttributeCompressedFile: "C",
	WindowsAttributeOfflineFile:    "O",
	WindowsAttributeNonIndexedFile: "I",
	WindowsAttributeEncryptedFile:  "E",
}

// Reference for File Attribute Constants:
// https://docs.microsoft.com/en-us/windows/win32/fileio/file-attribute-constants
var WindowsAttributesByName = map[string]WindowsAttribute{
	"R": WindowsAttributeReadOnly,
	"H": WindowsAttributeHidden,
	"S": WindowsAttributeSystemFile,
	"A": WindowsAttributeArchiveReady,
	"N": WindowsAttributeNormalFile,
	"T": WindowsAttributeTemporaryFile,
	"C": WindowsAttributeCompressedFile,
	"O": WindowsAttributeOfflineFile,
	"I": WindowsAttributeNonIndexedFile,
	"E": WindowsAttributeEncryptedFile,
}

func ParseNTFSAttributes(attr string) (WindowsAttribute, error) {
	out := WindowsAttribute(0)

	for _, v := range []rune(attr) {
		attrName := string(v)
		attr, ok := WindowsAttributesByName[attrName]

		if !ok {
			return 0, errors.New("could not parse attribute character " + attrName)
		}

		out |= attr
	}

	return out, nil
}
