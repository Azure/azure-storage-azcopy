package cmd

import "github.com/Azure/azure-storage-azcopy/v10/traverser"

// Keep existing Mover entry points while traversal implementation lives in traverser.
type StoredObject = traverser.StoredObject
type ResourceTraverser = traverser.ResourceTraverser
type InitResourceTraverserOptions = traverser.InitResourceTraverserOptions
type ResourceTraverserTemplate = traverser.ResourceTraverserTemplate
type TraverserErrorItemInfo = traverser.TraverserErrorItemInfo
type SyncOrchestratorOptions = traverser.SyncOrchestratorOptions
type CopyEnumerator = traverser.CopyEnumerator
type ObjectFilter = traverser.ObjectFilter
type objectProcessor = traverser.ObjectProcessor
type objectIndexer = traverser.ObjectIndexer
type syncEnumerator = traverser.SyncEnumerator

var InitResourceTraverser = traverser.InitResourceTraverser
var SplitResourceString = traverser.SplitResourceString
var NewSyncOrchestratorOptions = traverser.NewSyncOrchestratorOptions
var NewTestSyncOrchestratorOptions = traverser.NewTestSyncOrchestratorOptions
var DefaultSyncOrchestratorOptions = traverser.DefaultSyncOrchestratorOptions
var IsSyncOrchestratorOptionsValid = traverser.IsSyncOrchestratorOptionsValid
var GetSkippedFileErrorMessage = traverser.GetSkippedFileErrorMessage
var GetUnsupportedFileErrorMessage = traverser.GetUnsupportedFileErrorMessage
var newObjectIndexer = traverser.NewObjectIndexer
var newFpoAwareProcessor = traverser.NewFpoAwareProcessor
var processIfPassedFilters = traverser.ProcessIfPassedFilters
var noPreProccessor = traverser.NoPreProccessor

const SkippedItemErrorPrefix = traverser.SkippedItemErrorPrefix
const UnsupportedItemErrorPrefix = traverser.UnsupportedItemErrorPrefix

func NewDefaultSyncOrchestratorOptions() SyncOrchestratorOptions {
	return DefaultSyncOrchestratorOptions
}
