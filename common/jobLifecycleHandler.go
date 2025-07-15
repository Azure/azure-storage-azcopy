package common

type JobLifecycleHandler interface {
	OnStart(ctx JobContext)
}

type JobContext struct {
	LogPath   string
	JobID     JobID
	IsCleanup bool
}
