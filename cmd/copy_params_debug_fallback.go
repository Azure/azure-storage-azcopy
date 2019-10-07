// +build !debug

package cmd

import (
	"github.com/spf13/cobra"
)

// noop func; actual debug params enabled by compiling with -tags debug
func setupDebugCpParams(CpCmd *cobra.Command, raw *rawCopyCmdArgs) {}
