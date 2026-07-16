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
	"os"

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
		setDeprecatedVars := make(map[string]string)

		for _, env := range common.VisibleEnvironmentVariables {
			// do a direct lookup, instead of calling indirect lookup since indirect can get deprecated values.
			val, ok := os.LookupEnv(env.Name)

			if env.Hidden && !showSensitive && ok {
				val = "REDACTED"
			} else if !ok {
				val = "<Unset>"
			}

			// if this is a deprecated env var, don't display it unless we have a value. If we do display it, stash a warning to be displayed at the end, closest to the user's line of sight.
			if env.ReplacedBy != "" {
				if !ok {
					continue // skip displaying it
				} else {
					setDeprecatedVars[env.Name] = env.ReplacedBy
				}
			}

			glcm.Info(fmt.Sprintf("Name: %s\nCurrent Value: %s\nDescription: %s\n",
				env.Name, val, env.Description))
		}

		for depName, newName := range setDeprecatedVars {
			glcm.Info(fmt.Sprintf("Deprecated environment variable %s was set. Please check the new variable, %s, and migrate to it, as the deprecated variable may be removed in a future version.", depName, newName))
		}

		glcm.Exit(nil, EExitCode.Success())
	},
}

func init() {
	envCmd.PersistentFlags().BoolVar(&showSensitive, "show-sensitive", false, "Shows sensitive/secret environment variables.")
	rootCmd.AddCommand(envCmd)
}
