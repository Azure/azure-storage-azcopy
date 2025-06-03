package e2etest

import (
	"context"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"runtime"
	"runtime/debug"
	"strings"
	"testing"
)

// ScenarioVariationManager manages one specific variation of a scenario.
type ScenarioVariationManager struct {
	// t is intentionally nil during dryruns.
	t *testing.T

	// runNow disables parallelism in testing, instead running all tests immediately, intended for run-first suites.
	runNow bool

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
	VariationUUID uuid.UUID

	// wetrun data
	RunContext       context.Context
	CreatedResources *PathTrie[createdResource]
	CleanupFuncs     []func(a Asserter)
}

func (svm *ScenarioVariationManager) GetTestName() string {
	if svm.t != nil {
		return svm.t.Name()
	} else {
		return svm.Parent.testingT.Name() + "/" + svm.VariationName()
	}
}

func (svm *ScenarioVariationManager) Context() context.Context {
	if svm.RunContext == nil {
		return context.Background()
	}

	return svm.RunContext
}

func (svm *ScenarioVariationManager) SetContext(ctx context.Context) {
	svm.RunContext = ctx
}

func (svm *ScenarioVariationManager) UUID() uuid.UUID {
	if svm.VariationUUID == uuid.Nil { // ensure we aren't handing back something empty
		svm.VariationUUID = uuid.New()
	}

	return svm.VariationUUID
}

type createdResource struct {
	acct AccountResourceManager
	res  ResourceManager
}

func (svm *ScenarioVariationManager) initResourceTracker() {
	if svm.CreatedResources == nil {
		svm.CreatedResources = NewTrie[createdResource]('/')
	}
}

func (svm *ScenarioVariationManager) TrackCreatedResource(manager ResourceManager) {
	svm.initResourceTracker()

	canon := manager.Canon()
	svm.CreatedResources.Insert(canon, &createdResource{res: manager})
}

func (svm *ScenarioVariationManager) TrackCreatedAccount(account AccountResourceManager) {
	svm.initResourceTracker()

	svm.CreatedResources.Insert(account.AccountName(), &createdResource{acct: account})
}

func (svm *ScenarioVariationManager) DeleteCreatedResources() {
	svm.initResourceTracker()

	type deletable interface {
		Delete(a Asserter)
	}

	svm.CreatedResources.Traverse(func(data *createdResource) TraversalOperation {
		if data.acct != nil {
			DeleteAccount(svm, data.acct)
		} else if data.res != nil {
			del, isDeletable := data.res.(deletable)

			if !isDeletable {
				return TraversalOperationContinue
			}

			del.Delete(svm)
		}

		return TraversalOperationRemove
	})

	svm.CreatedResources = nil
}

// Assertions

func (svm *ScenarioVariationManager) NoError(comment string, err error, failNow ...bool) {
	if svm.Dryrun() {
		return
	}
	svm.t.Helper()
	failFast := FirstOrZero(failNow)

	//svm.AssertNow(comment, IsNil{}, err)
	if err != nil {
		svm.t.Logf("Error was not nil (%s): %v", comment, err)
		svm.isInvalid = true // Flip the failed flag

		if failFast {
			svm.t.FailNow()
		} else {
			svm.t.Fail()
		}
	}
}

func (svm *ScenarioVariationManager) Assert(comment string, assertion Assertion, items ...any) {
	if svm.Dryrun() {
		return
	}
	svm.t.Helper()

	if !assertion.Assert(items...) {
		if fa, ok := assertion.(FormattedAssertion); ok {
			svm.t.Logf("Assertion %s failed: %s (%s)", fa.Name(), fa.Format(items...), comment)
		} else {
			svm.t.Logf("Assertion %s failed with items %v (%s)", assertion.Name(), items, comment)
		}

		svm.isInvalid = true // We've now failed, so we flip the shared bad flag
		svm.t.Fail()
	}
}

func (svm *ScenarioVariationManager) AssertNow(comment string, assertion Assertion, items ...any) {
	if svm.Dryrun() {
		return
	}
	svm.t.Helper()

	if !assertion.Assert(items...) {
		if fa, ok := assertion.(FormattedAssertion); ok {
			svm.t.Logf("Assertion %s failed: %s (%s)", fa.Name(), fa.Format(items...), comment)
		} else {
			svm.t.Logf("Assertion %s failed with items %v (%s)", assertion.Name(), items, comment)
		}

		svm.isInvalid = true // We've now failed, so we flip the shared bad flag
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

func (svm *ScenarioVariationManager) HelperMarker() HelperMarker {
	if svm.t != nil {
		return svm.t
	}

	return NilHelperMarker{}
}

// =========== Variation Handling ==========

var variationExcludedCallers = map[string]bool{
	"GetVariation":          true,
	"ResolveVariation":      true,
	"GetVariationCallerID":  true,
	"NamedResolveVariation": true,
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

func (svm *ScenarioVariationManager) Cleanup(cleanupFunc CleanupFunc) {
	if svm.Dryrun() {
		svm.Error("Sanity check: svm.Cleanup should not be called during a dry run. No real actions should be taken during a dry run.")
		return
	}

	svm.CleanupFuncs = append(svm.CleanupFuncs, cleanupFunc)
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

// NamedResolveVariation is similar to ResolveVariation, but instead resolves over the keys in options, and hands back T.
func NamedResolveVariation[T any](svm *ScenarioVariationManager, options map[string]T) T {
	variation := GetTypeOrZero[string](svm.GetVariation(svm.GetVariationCallerID(), AnyKeys(options)))

	return options[variation]
}

var CleanupStepEarlyExit = errors.New("cleanupEarlyExit")

type ScenarioVariationManagerCleanupAsserter struct {
	svm *ScenarioVariationManager
}

func (s *ScenarioVariationManagerCleanupAsserter) GetTestName() string {
	return s.svm.GetTestName()
}

func (s *ScenarioVariationManagerCleanupAsserter) WrapCleanup(cf CleanupFunc) {
	defer func() {
		if err := recover(); err != nil {
			if err == CleanupStepEarlyExit {
				return
			}

			stackTrace := debug.Stack()

			s.Log("Cleanup step panicked: %v\n%s", err, string(stackTrace))
		}
	}()

	cf(s)
}

func (s *ScenarioVariationManagerCleanupAsserter) NoError(comment string, err error, failNow ...bool) {
	s.svm.t.Helper()

	failFast := FirstOrZero(failNow)

	//svm.AssertNow(comment, IsNil{}, err)
	if err != nil {
		s.Log("Error was not nil (%s): %v", comment, err)
		s.svm.isInvalid = true // Flip the failed flag

		s.svm.t.Fail()
		if failFast {
			panic(CleanupStepEarlyExit)
		}
	}
}

func (s *ScenarioVariationManagerCleanupAsserter) Assert(comment string, assertion Assertion, items ...any) {
	s.svm.t.Helper()

	if !assertion.Assert(items...) {
		if fa, ok := assertion.(FormattedAssertion); ok {
			s.Log("Assertion %s failed: %s (%s)", fa.Name(), fa.Format(items...), comment)
		} else {
			s.Log("Assertion %s failed with items %v (%s)", assertion.Name(), items, comment)
		}

		s.svm.isInvalid = true // We've now failed, so we flip the shared bad flag
		s.svm.t.Fail()
	}
}

func (s *ScenarioVariationManagerCleanupAsserter) AssertNow(comment string, assertion Assertion, items ...any) {
	s.svm.t.Helper()

	if !assertion.Assert(items...) {
		if fa, ok := assertion.(FormattedAssertion); ok {
			s.Log("Assertion %s failed: %s (%s)", fa.Name(), fa.Format(items...), comment)
		} else {
			s.Log("Assertion %s failed with items %v (%s)", assertion.Name(), items, comment)
		}

		s.svm.isInvalid = true // We've now failed, so we flip the shared bad flag
		s.svm.t.Fail()
		panic(CleanupStepEarlyExit)
	}
}

func (s *ScenarioVariationManagerCleanupAsserter) Error(reason string) {
	s.svm.t.Helper()
	s.Log("Failed cleanup step: %v", reason)
	panic(CleanupStepEarlyExit)
}

func (s *ScenarioVariationManagerCleanupAsserter) Skip(reason string) {
	s.svm.t.Helper()
	s.Log("Cleanup step skipped: %v", reason)
	panic(CleanupStepEarlyExit)
}

func (s *ScenarioVariationManagerCleanupAsserter) Log(format string, a ...any) {
	s.svm.t.Helper()
	s.svm.Log(format, a...)
}

func (s *ScenarioVariationManagerCleanupAsserter) Failed() bool {
	return s.svm.Failed()
}

func (s *ScenarioVariationManagerCleanupAsserter) HelperMarker() HelperMarker {
	return s.svm.HelperMarker()
}
