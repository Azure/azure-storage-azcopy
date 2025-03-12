package main

import (
	"github.com/Azure/azure-storage-azcopy/v10/ste"
	"github.com/spf13/cobra"
)

var RootCmd = &cobra.Command{
	Use: "stress_gen",

	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		var retryStatusCodes = "408;429;500;502;503;504"
		if genConfig.RetryOptions.HttpRetryCodes != "" {
			retryStatusCodes += ";" + genConfig.RetryOptions.HttpRetryCodes
		}

		rsc, err := ste.ParseRetryCodes(retryStatusCodes)
		if err != nil {
			return err
		}
		ste.RetryStatusCodes = rsc

		return nil
	},
}

func main() {
	_ = RootCmd.Execute()
}
