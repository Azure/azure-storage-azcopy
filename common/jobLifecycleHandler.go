package common

type JobLifecycleHandler interface {
	OnStart(ctx JobContext)
	OnScanProgress(progress ScanProgress) // only called during sync jobs
	OnTransferProgress(progress TransferProgress)
}

type JobContext struct {
	LogPath   string
	JobID     JobID
	IsCleanup bool
}

type ScanProgress struct {
	Source           uint64 // Files Scanned at Source
	Destination      uint64 // Files Scanned at Destination (only applicable for sync jobs)
	FirstPartOrdered bool
	Throughput       float64
}

type TransferProgress struct {
	ListJobSummaryResponse
	DeleteTotalTransfers     uint32 `json:",string"` // (only applicable for sync jobs)
	DeleteTransfersCompleted uint32 `json:",string"` // (only applicable for sync jobs)
}
