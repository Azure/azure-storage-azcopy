package e2etest

import "fmt"

// VariationDataContainer acts like a reverse linked list, sort of like a Context.
// It, too implements VariationData, if for some reason that's necessary
type VariationDataContainer struct {
	Previous *VariationDataContainer
	callerID string
	Value    any
}

func (vdc *VariationDataContainer) Insert(callerID string, value any) *VariationDataContainer {
	return &VariationDataContainer{
		Previous: vdc,
		callerID: callerID,
		Value:    value,
	}
}

func (vdc *VariationDataContainer) Lookup(callerID string) (any, bool) {
	// Traverse all VariationDataContainers attached
	dc := vdc
	for dc != nil {
		if dc.callerID == callerID {
			return dc.Value, true
		}

		dc = dc.Previous
	}

	return nil, false
}

func (vdc *VariationDataContainer) GetTestName() string {
	if vdc == nil {
		return "Test"
	}

	out := ""

	// Traverse all VariationDataContainers attached
	dc := vdc
	for dc != nil {
		out = fmt.Sprint(dc.Value) + out

		dc = dc.Previous
	}

	return out
}
