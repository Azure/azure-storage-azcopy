package main

import "github.com/spf13/cobra"

var RootCmd = &cobra.Command{
	Use: "stress_gen",
}

func init() {

}

func main() {
	err := RootCmd.Execute()
	if err != nil {
		panic(err)
	}
}
