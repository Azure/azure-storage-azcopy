package e2etest

import (
	"fmt"
	"runtime"
	"strings"
	"testing"
)

// ScenarioVariationManager manages one specific variation of a scenario.
type ScenarioVariationManager struct {
	// t is intentionally nil during dryruns.
	t *testing.T

	// isInvalid is synonymous with Failed. It serves two purposes:
	// 1. Invalidating dry-runs that would under no deterministic circumstances succeed.
	// 2. Failing wet-runs that encountered an error or unexpected results.
	// If you manually set invalid in a dry run, it will get caught when trying to spawn new variations
	// or when completing the current variation. No impact will occur.
	isInvalid bool
	// Dryrun is indicative that this is not a real run, Asserter will be filled with a ScenarioDryrunAsserter,
	// and this initial run is for mapping out calls to GetVariation(ID, options) (usually ResolveVariation).
	// The test will continue to spawn other variations until all variations have been mapped.
	//Dryrun bool

	// callcounts should under no circumstances be modified by hand.
	// If you are in need of a repeated singular variation result,
	// please call GetVariation using a custom static ID.
	callcounts map[string]uint

	// Parent refers to the running scenario.
	Parent *ScenarioManager
	// VariationData is a mapping of IDs to values, in order.
	VariationData *VariationDataContainer // todo call order, prepared options

	// wetrun data
	CreatedResources map[ResourceManager]bool
	CreatedAccounts  map[AccountResourceManager]bool
	RootResources    map[ResourceManager]bool
	ResourceTree     map[ResourceManager][]ResourceManager
}

func (svm *ScenarioVariationManager) initResourceTracker() {
	if svm.CreatedResources == nil || svm.CreatedAccounts == nil || svm.RootResources == nil || svm.ResourceTree == nil {
		svm.CreatedResources = make(map[ResourceManager]bool)
		svm.CreatedAccounts = make(map[AccountResourceManager]bool)
		svm.RootResources = make(map[ResourceManager]bool)
		svm.ResourceTree = make(map[ResourceManager][]ResourceManager)
	}
}

func (svm *ScenarioVariationManager) TrackCreatedResource(manager ResourceManager) {
	svm.initResourceTracker()

	svm.CreatedResources[manager] = true
	svm.ResourceTree[manager] = make([]ResourceManager, 0) // put something there so we know it still exists
	target := manager
	for { // attach it to the tree
		parent := target.Parent()

		if parent == nil {
			svm.RootResources[target] = true
			break
		}

		svm.ResourceTree[parent] = append(svm.ResourceTree[parent], target)
		target = parent
	}
}

func (svm *ScenarioVariationManager) TrackCreatedAccount(account AccountResourceManager) {
	svm.initResourceTracker()

	svm.CreatedAccounts[account] = true
}

func (svm *ScenarioVariationManager) DeleteCreatedResources() {
	svm.initResourceTracker()

	deletedAccounts := make(map[AccountResourceManager]bool)
	for res := range svm.RootResources {
		if ok := svm.CreatedAccounts[res.Account()]; ok {
			DeleteAccount(svm, res.Account())
			deletedAccounts[res.Account()] = true
		}

		if ok := deletedAccounts[res.Account()]; ok {
			delete(svm.RootResources, res)
		}
	}

	// now, delete the remaining resources
	queue := make([]ResourceManager, 0)
	for res := range svm.RootResources {
		queue = append(queue, res)
	}

	type deletable interface {
		Delete(a Asserter)
	}

	for len(queue) > 0 {
		target := queue[0]
		queue = queue[1:]

		if ok := svm.CreatedResources[target]; ok {
			del, isDeletable := target.(deletable)
			svm.Assert("must be deletable", Equal{}, isDeletable, true)

			if isDeletable {
				del.Delete(svm)

				delete(svm.CreatedResources, target) // no further tracking
				continue                             // Don't add children
			}
		}

		if children, ok := svm.ResourceTree[target]; ok {
			queue = append(queue, children...)
		}
	}

	svm.CreatedResources = nil
	svm.CreatedAccounts = nil
	svm.RootResources = nil
	svm.ResourceTree = nil
}

// Assertions

func (svm *ScenarioVariationManager) NoError(comment string, err error) {
	if svm.Dryrun() {
		return
	}

	svm.AssertNow(comment, IsNil{}, err)
}

func (svm *ScenarioVariationManager) Assert(comment string, assertion Assertion, items ...any) {
	if svm.Dryrun() {
		return
	}
	svm.t.Helper()

	if !assertion.Assert(items...) {
		svm.isInvalid = true // We've now failed, so we flip the shared bad flag

		if fa, ok := assertion.(FormattedAssertion); ok {
			svm.t.Logf("Assertion %s failed: %s (%s)", fa.Name(), fa.Format(items), comment)
		} else {
			svm.t.Logf("Assertion %s failed with items %v (%s)", assertion.Name(), items, comment)
		}
	}
}

func (svm *ScenarioVariationManager) AssertNow(comment string, assertion Assertion, items ...any) {
	if svm.Dryrun() {
		return
	}

	svm.Assert(comment, assertion, items...)
	if svm.Failed() {
		svm.t.FailNow()
	}
}

func (svm *ScenarioVariationManager) Error(reason string) {
	if svm.Dryrun() {
		return
	}

	svm.t.Helper()
	svm.isInvalid = true
	svm.t.Log("Error: " + reason)
	svm.t.FailNow()
}

func (svm *ScenarioVariationManager) Skip(reason string) {
	if svm.Dryrun() {
		return
	}

	svm.t.Helper()
	svm.t.Log("Skipped: " + reason)
	// No special flag is needed. We can surmise that if a test did a internalTestExit panic
	// and it is not invalid, that the only other logical reason is that we intentionally skipped it.
	svm.t.SkipNow()
	//panic(internalTestExit) // skip exits immediately
}

func (svm *ScenarioVariationManager) Log(format string, a ...any) {
	if svm.Dryrun() {
		return
	}

	svm.t.Helper()
	svm.t.Logf(format, a...)
}

func (svm *ScenarioVariationManager) Failed() bool {
	return svm.isInvalid // This is actually technically safe during dryruns.
}

// =========== Variation Handling ==========

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

		if !svm.isInvalid { // Don't spawn other variations if invalid
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
	svm.callcounts[callerID]++
	callCount := svm.callcounts[callerID]
	callerID += fmt.Sprintf(";calls=%d", callCount)

	return
}

// InsertVariationSeparator is mostly used to clean up variation names (e.g. rather than BlobBlobCopy, Blob->Blob_Copy)
func (svm *ScenarioVariationManager) InsertVariationSeparator(sep string) {
	// 1 variation won't spawn new runs
	svm.GetVariation(svm.GetVariationCallerID(), []any{sep})
}

func (svm *ScenarioVariationManager) Dryrun() bool {
	return svm.t == nil
}

func (svm *ScenarioVariationManager) Invalid() bool {
	return svm.isInvalid
}

func (svm *ScenarioVariationManager) InvalidateScenario() {
	svm.isInvalid = true
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
