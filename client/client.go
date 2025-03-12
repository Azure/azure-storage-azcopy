package client

import "github.com/Azure/azure-storage-azcopy/v10/common"

type ClientOptions struct {
	OutputFormat     common.OutputFormat
	OutputLevel      common.OutputVerbosity
	LogLevel         common.LogLevel
	CapMbps          float64
	ExtraSuffixesAAD string
	SkipVersionCheck bool
}

type Client struct {
	ClientOptions
}

func (cc Client) initialize() error {
	return nil
}

type EnvOptions struct {
	ShowSensitive bool
}

func (cc Client) Env(options EnvOptions) error {
	return nil
}

// TODO (gapra-msft):
//[P0]Copy - API
//[P0]Sync - API
//[P0]Login - API
//[P0]Login Status - API
//[P0]Logout - API
//[P0]Jobs List - API
//[P0]Jobs Remove - API
//[P0]Jobs Resume - API
//[P1]Jobs Clean - API
//[P1]Jobs Show - API
//[P2]List - API
//[P2]Make - API
//[P2]Remove - API
//[P2]Set Properties - API
//[P2]Env - API
//[P2]Benchmark - API
//Jobs - Stub for subcommands
//Pause - Hidden
//Cancel - Hidden
//Load - Hidden
//LoadCLFS - Hidden
