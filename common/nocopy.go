package common

import "errors"

// The NoCopy struct is used as a field inside another struct that should not be copied by value.
// After embedded this field, the out struct's members can call the Check method which will panic
// if it detects the out struct has been copied by value.
type NoCopy struct {
	nocopy *NoCopy
}

// Check panics if the struct embedded this NoCopy field has been copied by value.
func (nc *NoCopy) Check() {
	if nc.nocopy == nc {
		return // The reference matches the 1st-time reference
	}
	if nc.nocopy == nil { // The reference was never set, set it
		nc.nocopy = nc
		return
	}
	// The receiver's reference doesn't match the persisted reference; this must be a copy
	panic(errors.New("nocopy detected copy by value"))
}

