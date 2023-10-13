package e2etest

import (
	"fmt"
	"runtime"
	"strings"
)

// ScenarioVariationManager manages one specific variation of a scenario.
type ScenarioVariationManager struct {
	Asserter
	Invalid bool
	Dryrun  bool

	Callcounts map[string]uint

	Parent *ScenarioManager
	// VariationData is a mapping of IDs to values, in order.
	VariationData *VariationDataContainer // todo call order, prepared options
}

var variationExcludedCallers = map[string]bool{
	"GetVariation":         true,
	"ResolveVariation":     true,
	"GetVariationCallerID": true,
}

func (svm *ScenarioVariationManager) VariationName() string {
	return svm.VariationData.GetTestName()
}

// GetVariation acts as a simple dictionary for IDs to variations.
// If no variation with the related ID is found
func (svm *ScenarioVariationManager) GetVariation(ID string, options []any) any {
	var currentVariation any
	var variationExists bool
	if svm.VariationData != nil {
		currentVariation, variationExists = svm.VariationData.Lookup(ID)
	}

	if variationExists {
		return currentVariation
	} else {
		currentVariation = options[0] // Default.
		options = options[1:]         // Trim for remaining variations

		if !svm.Invalid { // Don't spawn other variations if invalid
			svm.Parent.NewVariation(svm, ID, options)
		}

		svm.VariationData = svm.VariationData.Insert(ID, currentVariation)

		return currentVariation
	}
}

// GetVariationCallerID builds a raw caller ID based upon the current call stack.
// It returns an incremented caller ID (e.g. with the ";calls=n" suffix).
func (svm *ScenarioVariationManager) GetVariationCallerID() (callerID string) {
	type scopedFunction struct {
		Package string
		Scope   []string
		Name    string
	}

	type caller struct {
		// Func isn't ultimately needed for the return value, but it is needed to pick up functions we should avoid in the call stack.
		// todo: maybe find a better way to exclude these functions?
		Func scopedFunction
		File string
		Line int
	}

	// Get from test to variation in the call stack
	skippedCalls := 0
	callerIDs := make([]string, 0)
	for {
		callerPC, callerFile, callerLine, ok := runtime.Caller(len(callerIDs) + skippedCalls)
		// Ensure we're calling from the right place
		svm.AssertNow(fmt.Sprintf("%s must be included in the call stack prior to GetVariation", svm.Parent.scenario), Equal{}, true, ok)

		fn := runtime.FuncForPC(callerPC)
		rawName := fn.Name()
		// Trim package name
		lastSlash := strings.LastIndex(rawName, "/")
		var pkgName string
		if lastSlash != -1 {
			//prefixedFuncName := rawName[lastSlash+1:]
			//// There should always be a dot here. Sanity check if we don't.

			pkgName = rawName[:lastSlash+1]
			rawName = rawName[lastSlash+1:] // trim prefix from raw name
		}

		pkgDotIdx := strings.Index(rawName, ".")
		svm.AssertNow(fmt.Sprintf("functions must have a package prefix"), Not{Equal{}}, pkgDotIdx, -1)
		pkgName += rawName[:pkgDotIdx]
		rawName = rawName[pkgDotIdx+1:]

		scopeSegments := strings.Split(strings.TrimRight(rawName, "[...]"), ".")
		funcName := scopeSegments[len(scopeSegments)-1]

		currentCaller := caller{
			Func: scopedFunction{
				Package: pkgName,
				Scope:   scopeSegments[:len(scopeSegments)-1],
				Name:    funcName,
			},
			File: callerFile,
			Line: callerLine,
		}

		if ok := variationExcludedCallers[currentCaller.Func.Name]; !ok {
			callerIDs = append(callerIDs, fmt.Sprintf("%s:%d", currentCaller.File, currentCaller.Line))
		} else {
			skippedCalls++
		}

		if strings.EqualFold(currentCaller.Func.Name, svm.Parent.scenario) {
			break
		}
	}

	callerID = strings.Join(callerIDs, ";")
	svm.Callcounts[callerID]++
	callCount := svm.Callcounts[callerID]
	callerID += fmt.Sprintf(";calls=%d", callCount)

	return
}

func (svm *ScenarioVariationManager) InvalidateScenario() {
	svm.Invalid = true
}

// InsertVariationSeparator is mostly used to clean up variation names (e.g. rather than BlobBlobCopy, Blob->Blob_Copy)
func (svm *ScenarioVariationManager) InsertVariationSeparator(sep string) {
	// 1 variation won't spawn new runs
	svm.GetVariation(svm.GetVariationCallerID(), []any{sep})
}

// ResolveVariation wraps ScenarioVariationManager.GetVariation, returning the variation as the user's requested type, and using the call stack as the ID
// ResolveVariation doesn't have a type receiver, because the type itself must have a generic type in order for one of its methods to be generic
func ResolveVariation[T any](svm *ScenarioVariationManager, options []T) T {
	return GetTypeOrZero[T](svm.GetVariation(svm.GetVariationCallerID(), ListOfAny(options)))
}

// ResolveVariationByID is the same as ResolveVariation, but it's based upon the supplied ID rather than the call stack.
func ResolveVariationByID[T any](svm *ScenarioVariationManager, ID string, options []any) T {
	return GetTypeOrZero[T](svm.GetVariation(ID, ListOfAny(options)))
}
