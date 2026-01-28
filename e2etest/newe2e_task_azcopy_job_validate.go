package e2etest

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"strings"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/ste"
)

// ExpectedPlanFile
// todo: autoformat to this, leave manual creation for advanced use cases
type ExpectedPlanFile struct {
	// todo: if/when data piping changes, re-assess these fields.
	// validate is a bit dumber than prior tagging systems. The syntax is very rigid. It goes <Name>,<Path>
	// Path is always from the root of the real header object, regardless of where you are in this test plan file's tree.
	ForceWrite                     *common.OverwriteOption             `validate:"Force Write,ForceWrite"`
	ForceIfReadOnly                *bool                               `validate:"Force If Read Only,ForceIfReadOnly"`
	AutoDecompress                 *bool                               `validate:"Auto Decompress,AutoDecompress"`
	FromTo                         *common.FromTo                      `validate:"From-To,FromTo"`
	Fpo                            *common.FolderPropertyOption        `validate:"Folder Property Option,Fpo"`
	LogLevel                       *common.LogLevel                    `validate:"Log Level,LogLevel"`
	PreservePermissions            *common.PreservePermissionsOption   `validate:"Preserve Permissions,PreservePermissions"`
	PreserveSMBInfo                *bool                               `validate:"Preserve SMB Info,PreserveSMBInfo"`
	PreservePOSIXProperties        *bool                               `validate:"Preserve POSIX Properties,PreservePOSIXProperties"`
	S2SGetPropertiesInBackend      *bool                               `validate:"S2S Get Properties in Backend,S2SGetPropertiesInBackend"`
	S2SSourceChangeValidation      *bool                               `validate:"S2S Source Change Validation,S2SSourceChangeValidation"`
	DestLengthValidation           *bool                               `validate:"Destination Length Validation,DestLengthValidation"`
	S2SInvalidMetadataHandleOption *common.InvalidMetadataHandleOption `validate:"S2S Invalid Metadata Handling,S2SInvalidMetadataHandleOption"`
	BlobFSRecursiveDelete          *bool                               `validate:"BlobFS Recursive Delete,BlobFSRecursiveDelete"`
	DeleteSnapshotsOption          *common.DeleteSnapshotsOption       `validate:"Delete Snapshot Option,DeleteSnapshotsOption"`
	PermanentDeleteOption          *common.PermanentDeleteOption       `validate:"Permanent Delete,PermanentDeleteOption"`

	BlobData  ExpectedPlanFileBlobData
	LocalData ExpectedPlanFileLocalData
	FileData  ExpectedPlanFileFileDstData

	Objects map[PlanFilePath]PlanFileObject
}

type GeneratePlanFileObjectsOptions struct {
	// Morphs the destination path; source is retained as-is.
	DestPathProcessor func(path string) string
	// Processors morph objects
	Processors []func(def *ResourceDefinitionObject)
	// Filters return true to admit an object to the plan file,
	Filters []func(def *ResourceDefinitionObject) bool
}

func ParentDirDestPathProcessor(DirName string) func(path string) string {
	return func(modPath string) string {
		return path.Join(DirName, modPath)
	}
}

func GeneratePlanFileObjectsFromMapping(mapping ObjectResourceMapping, options ...GeneratePlanFileObjectsOptions) map[PlanFilePath]PlanFileObject {
	objects := mapping.Flatten()
	opts := FirstOrZero(options)

	out := make(map[PlanFilePath]PlanFileObject)

	ensurePathSeparatorPrefix := func(path string) string {
		sep := "/"

		if !strings.HasPrefix(path, sep) && len(path) > 0 {
			return sep + path
		}

		return path
	}

	for k, v := range objects {
		v = v.Clone()
		v.ObjectName = &k

		for _, proc := range opts.Processors {
			proc(&v)
		}

		keep := true
		for _, filter := range opts.Filters {
			keep = filter(&v)
			if !keep {
				break
			}
		}

		dstPath := *v.ObjectName
		if opts.DestPathProcessor != nil {
			dstPath = opts.DestPathProcessor(dstPath)
		}

		out[PlanFilePath{
			SrcPath: ensurePathSeparatorPrefix(*v.ObjectName),
			DstPath: ensurePathSeparatorPrefix(dstPath),
		}] = PlanFileObject{
			Properties:      v.ObjectProperties,
			ShouldBePresent: &keep,
		}
	}

	return out
}

func (e *ExpectedPlanFile) Validate(a Asserter, header *ste.JobPartPlanHeader) {
	a.AssertNow("Expected plan file cannot be nil", Not{IsNil{}}, e)
	a.AssertNow("Header cannot be nil", Not{IsNil{}}, header)

	self := reflect.ValueOf(e).Elem()
	hVal := reflect.ValueOf(header).Elem()

	type queueItem struct {
		val  reflect.Value
		path string
	}

	appendPath := func(base, new string) string {
		if len(base) == 0 {
			return new
		}

		return base + "." + new
	}

	queue := []queueItem{{self, ""}}
	for len(queue) > 0 {
		item := queue[0]
		queue = queue[1:]
		val := item.val
		vType := val.Type()
		numFields := val.NumField()
		for i := 0; i < numFields; i++ {
			field := val.Field(i)
			tag := vType.Field(i).Tag
			name := vType.Field(i).Name

			newPath := appendPath(item.path, name)

			kind := field.Kind()
			// deref if necessary
			if kind == reflect.Pointer || kind == reflect.Interface {
				field = field.Elem()
				if !field.IsValid() {
					continue
				}

				kind = field.Kind()
			}

			tagVal, ok := tag.Lookup("validate")
			if !ok {
				// If nothing was specified, and this isn't a struct, it probably has some other processing.
				if kind == reflect.Struct {
					queue = append(queue, queueItem{field, newPath})
					continue
				}

				continue
			}

			splits := strings.Split(tagVal, ",")
			a.AssertNow("validate tag must split into exactly two sets "+newPath, Equal{}, len(splits), 2)
			tagName := splits[0]
			tagPath := strings.Split(splits[1], ".")

			target := hVal
			for n, v := range tagPath {
				target = hVal.FieldByName(v)
				kind := target.Kind()

				if kind == reflect.Pointer || kind == reflect.Interface {
					target = target.Elem()
					kind = target.Kind()
				}

				if n != len(tagPath)-1 {
					a.AssertNow(fmt.Sprintf("Element %s must be a struct %s", v, newPath), Equal{}, kind, reflect.Struct)
				}
			}

			if !field.IsNil() {
				a.Assert(
					fmt.Sprintf("Element %s must equal value to the real job plan (%v (expected) != %v (actual))", tagName, field.Interface(), target.Interface()),
					Equal{Deep: true},
					field.Interface(),
					target.Interface(),
				)
			}
		}
	}

	// validate the additional manual fields
	//header.DstBlobData.
	if e.BlobData.CacheControl != nil {
		str := string(header.DstBlobData.CacheControl[:header.DstBlobData.CacheControlLength])
		a.Assert("CacheControl differs in header", Equal{}, *e.BlobData.CacheControl, str)
	}

	if e.BlobData.ContentType != nil {
		str := string(header.DstBlobData.ContentType[:header.DstBlobData.ContentTypeLength])
		a.Assert("ContentType differs in header", Equal{}, *e.BlobData.ContentType, str)
	}

	if e.BlobData.ContentEncoding != nil {
		str := string(header.DstBlobData.ContentEncoding[:header.DstBlobData.ContentEncodingLength])
		a.Assert("ContentEncoding differs in header", Equal{}, *e.BlobData.ContentEncoding, str)
	}

	if e.BlobData.ContentLanguage != nil {
		str := string(header.DstBlobData.ContentLanguage[:header.DstBlobData.ContentLanguageLength])
		a.Assert("ContentLanguage differs in header", Equal{}, *e.BlobData.ContentLanguage, str)
	}

	if e.BlobData.ContentDisposition != nil {
		str := string(header.DstBlobData.ContentDisposition[:header.DstBlobData.ContentDispositionLength])
		a.Assert("ContentDisposition differs in header", Equal{}, *e.BlobData.ContentDisposition, str)
	}

	if e.BlobData.Metadata != nil {
		metadataStr := string(header.DstBlobData.Metadata[:header.DstBlobData.MetadataLength])
		meta, err := common.StringToMetadata(metadataStr)
		a.NoError("Parse metadata", err)

		a.Assert("Metadata differs in header", Equal{Deep: true}, e.BlobData.Metadata, meta)
	}

	if e.BlobData.BlobTags != nil {
		tagsStr := string(header.DstBlobData.BlobTags[:header.DstBlobData.BlobTagsLength])
		tags := common.ToCommonBlobTagsMap(tagsStr)
		a.Assert("Tags differ in header", Equal{Deep: true}, e.BlobData.BlobTags, tags)
	}

	if e.BlobData.CpkScopeInfo != nil {
		CpkScopeStr := string(header.DstBlobData.CpkScopeInfo[:header.DstBlobData.CpkScopeInfoLength])
		a.Assert("CPK Scope Info differs in header", Equal{Deep: true}, *e.BlobData.CpkScopeInfo, CpkScopeStr)
	}
}

type ExpectedPlanFileBlobData struct {
	BlobType                         *common.BlobType `validate:"Blob Type,DstBlobData.BlobType"`
	NoGuessMimeType                  *bool            `validate:"Don't Guess MIME Type,DstBlobData.NoGuessMimeType'"`
	CacheControl                     *string
	ContentDisposition               *string
	ContentEncoding                  *string
	ContentLanguage                  *string
	ContentType                      *string
	BlockBlobTier                    *common.BlockBlobTier `validate:"Block Blob Tier,DstBlobData.BlockBlobTier"`
	PageBlobTier                     *common.PageBlobTier  `validate:"Page Blob Tier,DstBlobData.PageBlobTier"`
	PutMd5                           *bool                 `validate:"Put MD5 Data,DstBlobData.PutHash"`
	Metadata                         common.Metadata
	BlobTags                         common.BlobTags
	CpkInfo                          *bool
	IsSourceEncrypted                *bool
	CpkScopeInfo                     *string
	DeleteDestinationFileIfNecessary *bool
}

type ExpectedPlanFileLocalData struct {
	PreserveLastModifiedTime *bool
	MD5VerificationOption    *common.HashValidationOption
}

type ExpectedPlanFileFileDstData struct {
	TrailingDot *common.TrailingDotOption
}

type PlanFilePath struct {
	SrcPath, DstPath string
}

type PlanFileObject struct {
	// Properties defines properties expected to be seen within the plan file. Properties that are not stored in the plan file are not validated.
	Properties ObjectProperties
	// ShouldBePresent defaults to TRUE-- In that the object should be a part of the plan file. If set to false, will fail validation if present. Use this to validate filters.
	ShouldBePresent *bool
}

func (e *ExpectedPlanFile) Clone() *ExpectedPlanFile {
	out := &ExpectedPlanFile{
		ForceWrite:                     ClonePointer(e.ForceWrite),
		ForceIfReadOnly:                ClonePointer(e.ForceIfReadOnly),
		AutoDecompress:                 ClonePointer(e.AutoDecompress),
		FromTo:                         ClonePointer(e.FromTo),
		Fpo:                            ClonePointer(e.Fpo),
		LogLevel:                       ClonePointer(e.LogLevel),
		PreservePermissions:            ClonePointer(e.PreservePermissions),
		PreserveSMBInfo:                ClonePointer(e.PreserveSMBInfo),
		PreservePOSIXProperties:        ClonePointer(e.PreservePOSIXProperties),
		S2SGetPropertiesInBackend:      ClonePointer(e.S2SGetPropertiesInBackend),
		S2SSourceChangeValidation:      ClonePointer(e.S2SSourceChangeValidation),
		DestLengthValidation:           ClonePointer(e.DestLengthValidation),
		S2SInvalidMetadataHandleOption: ClonePointer(e.S2SInvalidMetadataHandleOption),

		Objects: make(map[PlanFilePath]PlanFileObject),
	}

	for k, v := range e.Objects {
		out.Objects[k] = v
	}

	return out
}

/*
ValidatePlanFiles expects an &AzCopyParsedCopySyncRemoveStdout, and validates that the plan files match the expectedFiles.

The ObjectResourceMapping will be flattened.
dstPrefix is trimmed from all destination relative paths

In account-wide transfers, containers are simply appended to the path that should be looked for in expectedFiles.

If copying from a sub-directory of an actual root ObjectResourceMapping, ObjectResourceMappingParentFolder features a Trim option.
*/
func ValidatePlanFiles(sm *ScenarioVariationManager, stdOut AzCopyStdout, expected ExpectedPlanFile) {
	if sm == nil || sm.Dryrun() {
		return
	}

	sm.HelperMarker().Helper()

	parsedStdout := GetTypeOrAssert[*AzCopyParsedCopySyncRemoveStdout](sm, stdOut)
	planFolder := parsedStdout.JobPlanFolder

	validation := expected.Clone()

	files := func(ext string) []os.FileInfo {
		var files []os.FileInfo
		_ = filepath.Walk(planFolder, func(path string, fileInfo os.FileInfo, err error) error {
			if err != nil {
				return err
			}

			if !fileInfo.IsDir() && strings.HasSuffix(fileInfo.Name(), ext) {
				files = append(files, fileInfo)
			}
			return nil
		})
		return files
	}(fmt.Sprintf(".steV%d", ste.DataSchemaVersion))

	for _, v := range files {
		planFile := ste.JobPartPlanFileName(filepath.Join(planFolder, v.Name()))
		mmf := planFile.Map()

		plan := mmf.Plan()

		if plan.PartNum == 0 {
			expected.Validate(sm, plan)
		}

		for i := uint32(0); i < plan.NumTransfers; i++ {
			//tx := plan.Transfer(i)
			src, dst := plan.TransferSrcDstRelatives(i)

			_, _, blobType, blobTier,
				propsInBackend, _, _, _, // DstLengthValidation, SourceChangeValidation, InvalidMetadataHandleOption
				entityType, version, _, tags, _ := plan.TransferSrcPropertiesAndMetadata(i) // missing snapshot ID

			errPrefix := fmt.Sprintf("object src: %s, dst: %s; ", src, dst)

			expectedObject, ok := validation.Objects[PlanFilePath{src, dst}]
			if !ok || !DerefOrDefault(expectedObject.ShouldBePresent, true) {
				sm.Assert(errPrefix+"was not expected to be in the plan file "+common.Iff(ok, "(explicit disinclude)", "(not present in expected plan file)"), Always{})

				continue
			}

			delete(expected.Objects, PlanFilePath{src, dst})

			sm.Assert(errPrefix+"Plan file entity type did not match expectation", Equal{}, expectedObject.Properties.EntityType, entityType)
			if !propsInBackend {
				//sm.Assert(errPrefix+"Headers did not match expectation", Equal{Deep: true}, expectedObject.Properties.HTTPHeaders.ToCommonHeaders(), headers)
				//sm.Assert(errPrefix+"Metadata did not match expectation", Equal{Deep: true}, expectedObject.Properties.Metadata, meta)
				if plan.FromTo.To() == common.ELocation.Blob() {
					sm.Assert(errPrefix+"BlobType did not match expectation", Equal{}, DerefOrZero(expectedObject.Properties.BlobProperties.Type), blobType)
					sm.Assert(errPrefix+"BlobTier did not match expectation", Equal{}, DerefOrZero(expectedObject.Properties.BlobProperties.BlockBlobAccessTier), blobTier)
					sm.Assert(errPrefix+"BlobVersion did not match expectation", Equal{}, DerefOrZero(expectedObject.Properties.BlobProperties.VersionId), version)
					sm.Assert(errPrefix+"BlobTags did not match expectation", Equal{Deep: true}, expectedObject.Properties.BlobProperties.Tags, tags)
				}
			}
		}

		mmf.Unmap()
	}

	for path, obj := range expected.Objects {
		if DerefOrDefault(obj.ShouldBePresent, true) {
			sm.Assert("object src: "+path.SrcPath+", dst: "+path.DstPath+"; was missing from the plan file.", Always{})
		}
	}
}
