package main

import (
	"flag"
	"fmt"
	"github.com/Azure/azure-storage-azcopy/v10/e2etest"
)

const (
	GenManyFoldersName = "many-folders"
)

func init() {
	RegisterGenerator(&ManyFoldersGenerator{})
}

type ManyFoldersGenerator struct {
	ContainerTarget string
}

func (m *ManyFoldersGenerator) Name() string {
	return GenManyFoldersName
}

func (m *ManyFoldersGenerator) Generate(manager e2etest.ServiceResourceManager) error {
	fmt.Println("asdf")
	return nil
}

func (m *ManyFoldersGenerator) RegisterFlags(pFlags *flag.FlagSet) {
	pFlags.StringVar(&m.ContainerTarget, FlagContainerName, e2etest.SyntheticContainerManyFoldersSource, "Set a custom container name")
}
