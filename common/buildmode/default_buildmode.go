//go:build !mover
// +build !mover

package buildmode

// IsMover always returns false when 'mover' build tag is not defined.
var IsMover = false
