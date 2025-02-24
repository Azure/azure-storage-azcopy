package main

import (
	"flag"
	"fmt"
	"github.com/Azure/azure-storage-azcopy/v10/e2etest"
	"github.com/spf13/cobra"
)

type Generator interface {
	Name() string
	Generate(manager e2etest.ServiceResourceManager) error
	RegisterFlags(flags *flag.FlagSet)
}

var GeneratorRegistry = make(map[string]Generator)

func RegisterGenerator(g Generator) {
	_, ok := GeneratorRegistry[g.Name()]
	if ok {
		panic("Generator " + g.Name() + " re-registered")
	}

	GeneratorRegistry[g.Name()] = g
	cmd := &cobra.Command{
		Use: fmt.Sprintf("%s <service-uri>", g.Name()),
		RunE: func(cmd *cobra.Command, args []string) error {
			return RunGenerator(g.Name(), args[0])
		},
		Args: RequireValidTarget,
	}

	RootCmd.AddCommand(cmd)
}

func RunGenerator(name, target string) error {
	svc, err := GetResourceManagerForURI(target)
	if err != nil {
		return fmt.Errorf("failed to get resource manager: %w", err)
	}

	gen, ok := GeneratorRegistry[name]
	if !ok {
		return fmt.Errorf("generator %s does not exist", name)
	}

	fmt.Printf("Generating scenario %s...", name)
	err = gen.Generate(svc)
	if err != nil {
		return fmt.Errorf("failed generating scenario %s: %w", name, err)
	}

	return nil
}
