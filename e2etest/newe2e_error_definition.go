package e2etest

type ErrorTier uint8

const (
	// ErrorTierInconsequential means no corrective action is needed.
	ErrorTierInconsequential ErrorTier = iota
	// ErrorTierFatal means it is impossible to continue running the test.
	ErrorTierFatal
	// todo: should we have an ErrorTierRecoverable?
)

type TieredError interface {
	error
	Tier() ErrorTier
}

type TieredErrorWrapper struct {
	error
	ErrorTier
}

func (w TieredErrorWrapper) Tier() ErrorTier {
	return w.ErrorTier
}
