package azcopy

type JobLifecycleHandler interface {
	OnStart(ctx JobContext)
	OnComplete()
}

type JobContext struct {
	LogPath   string
	JobID     string
	IsCleanup bool
}
