package main

import (
	_ "embed"
	"fmt"
	"os"

	cmd2 "github.com/Azure/azure-storage-azcopy/v10/cmd"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/josephspurrier/goversioninfo"
	"github.com/spf13/cobra"
)

var cmd = cobra.Command{
	Use:   "syso_gen",
	Short: "Generates matching resource.syso files for all CPU architectures",

	RunE: func(cmd *cobra.Command, args []string) error {
		ver, err := cmd2.NewVersion(common.AzcopyVersion)
		if err != nil {
			return fmt.Errorf("failed to parse AzCopy Version: %w", err)
		}

		cfg := goversioninfo.NewCLIConfig()

		cfg.VerMajor = ver.Segments[0]
		cfg.VerMinor = ver.Segments[1]
		cfg.VerPatch = ver.Segments[2]
		cfg.ProductVerMajor = ver.Segments[0]
		cfg.ProductVerMinor = ver.Segments[1]
		cfg.ProductVerPatch = ver.Segments[2]
		// skips trying to read versioninfo.json... which we supply the relevant fields from it above.
		cfg.SkipVersionInfo = true

		cfg.ProductName = "Microsoft ® AzCopy v10"
		cfg.Copyright = "© Microsoft Corporation. All rights reserved"

		cfg.PlatformSpecific = true
		cfg.OutputFile = ""

		err = goversioninfo.RunCLI(cfg)
		return err
	},
}

func main() {
	err := cmd.Execute()
	if err != nil {
		fmt.Println("failed to generate syso file:", err)
		os.Exit(1)
	}
}
