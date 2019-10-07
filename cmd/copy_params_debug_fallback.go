// +build !debug

package cmd

// noop func; actual debug params enabled by compiling with -tags debug
func setupDebugCpParams(CpCmd *cobra.Command, raw *rawCopyCmdArgs) {}
