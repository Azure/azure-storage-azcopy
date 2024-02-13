package e2etest

type ScenarioPipelineOptions struct {
	Parallelized *bool
}

func (spo *ScenarioPipelineOptions) IsParallel() bool {
	if spo == nil || spo.Parallelized == nil {
		return true
	}

	return *spo.Parallelized
}
