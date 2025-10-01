// Copyright © 2018 NAME HERE <EMAIL ADDRESS>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"fmt"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/spf13/cobra"
)

var showSensitive = false

// envCmd represents the env command
var envCmd = &cobra.Command{
	Use:   "env",
	Short: envCmdShortDescription,
	Long:  envCmdLongDescription,
	Run: func(cmd *cobra.Command, args []string) {
		for _, env := range common.VisibleEnvironmentVariables {
			val := common.GetEnvironmentVariable(env)
			if env.Hidden && !showSensitive {
				val = "REDACTED"
			}

			glcm.Info(fmt.Sprintf("Name: %s\nCurrent Value: %s\nDescription: %s\n",
				env.Name, val, env.Description))
		}

		glcm.Exit(nil, common.EExitCode.Success())
	},
}

func init() {
	envCmd.PersistentFlags().BoolVar(&showSensitive, "show-sensitive", false, "Shows sensitive/secret environment variables.")
	rootCmd.AddCommand(envCmd)
}
