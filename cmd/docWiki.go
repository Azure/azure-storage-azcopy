// Copyright © Microsoft <wastore@microsoft.com>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package cmd

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"text/template"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

// WikiGenerator generates GitHub Wiki style documentation
type WikiGenerator struct {
	OutputDir string
	commands  []*cobra.Command
}

// FlagInfo holds information about a command flag for templating
type FlagInfo struct {
	Name         string
	Shorthand    string
	Type         string
	DefaultValue string
	Description  string
}

// CommandInfo holds information about a command for templating
type CommandInfo struct {
	Name              string
	FullName          string
	Short             string
	Long              string
	UseLine           string
	Example           string
	LocalFlags        []FlagInfo
	InheritedFlags    []FlagInfo
	SubCommands       []CommandInfo
	ParentName        string
	HasLocalFlags     bool
	HasInheritedFlags bool
	HasExample        bool
	HasSubCommands    bool
}

// NewWikiGenerator creates a new WikiGenerator
func NewWikiGenerator(outputDir string) *WikiGenerator {
	return &WikiGenerator{
		OutputDir: outputDir,
		commands:  make([]*cobra.Command, 0),
	}
}

// Generate generates wiki documentation for the given command and its children
func (g *WikiGenerator) Generate(cmd *cobra.Command) error {
	// Collect all commands
	g.collectCommands(cmd)

	// Generate individual command pages (root command becomes azcopy.md)
	for _, c := range g.commands {
		if err := g.generateCommandPage(c); err != nil {
			return fmt.Errorf("failed to generate page for %s: %w", c.Name(), err)
		}
	}

	return nil
}

func (g *WikiGenerator) collectCommands(cmd *cobra.Command) {
	g.commands = append(g.commands, cmd)
	for _, c := range cmd.Commands() {
		if !c.Hidden {
			g.collectCommands(c)
		}
	}
}

func (g *WikiGenerator) getCommandInfo(cmd *cobra.Command) CommandInfo {
	info := CommandInfo{
		Name:     cmd.Name(),
		FullName: cmd.CommandPath(),
		Short:    cmd.Short,
		Long:     cmd.Long,
		UseLine:  cmd.UseLine(),
		Example:  cmd.Example,
	}

	// Get local flags
	cmd.LocalFlags().VisitAll(func(f *pflag.Flag) {
		if f.Hidden {
			return
		}
		info.LocalFlags = append(info.LocalFlags, FlagInfo{
			Name:         f.Name,
			Shorthand:    f.Shorthand,
			Type:         f.Value.Type(),
			DefaultValue: f.DefValue,
			Description:  cleanDescription(f.Usage),
		})
	})

	// Get inherited flags
	cmd.InheritedFlags().VisitAll(func(f *pflag.Flag) {
		if f.Hidden {
			return
		}
		info.InheritedFlags = append(info.InheritedFlags, FlagInfo{
			Name:         f.Name,
			Shorthand:    f.Shorthand,
			Type:         f.Value.Type(),
			DefaultValue: f.DefValue,
			Description:  cleanDescription(f.Usage),
		})
	})

	// Get subcommands
	for _, c := range cmd.Commands() {
		if !c.Hidden {
			info.SubCommands = append(info.SubCommands, CommandInfo{
				Name:     c.Name(),
				FullName: c.CommandPath(),
				Short:    c.Short,
			})
		}
	}

	// Set parent name
	if cmd.HasParent() {
		info.ParentName = cmd.Parent().CommandPath()
	}

	// Set boolean helpers
	info.HasLocalFlags = len(info.LocalFlags) > 0
	info.HasInheritedFlags = len(info.InheritedFlags) > 0
	info.HasExample = len(info.Example) > 0
	info.HasSubCommands = len(info.SubCommands) > 0

	return info
}

func cleanDescription(s string) string {
	// Replace newlines with spaces and trim
	s = strings.ReplaceAll(s, "\n", " ")
	s = strings.ReplaceAll(s, "\t", " ")
	// Remove multiple spaces
	for strings.Contains(s, "  ") {
		s = strings.ReplaceAll(s, "  ", " ")
	}
	// Escape pipe characters for markdown tables
	s = strings.ReplaceAll(s, "|", "\\|")
	return strings.TrimSpace(s)
}

func commandToWikiFilename(cmd *cobra.Command) string {
	// Convert "azcopy copy" to "azcopy_copy" (same format as cobra default)
	return strings.ReplaceAll(cmd.CommandPath(), " ", "_")
}

func (g *WikiGenerator) generateCommandPage(cmd *cobra.Command) error {
	info := g.getCommandInfo(cmd)

	tmpl, err := template.New("command").Funcs(template.FuncMap{
		"wikiLink": func(fullName string) string {
			return strings.ReplaceAll(fullName, " ", "_")
		},
		"flagName": func(f FlagInfo) string {
			if f.Shorthand != "" {
				return fmt.Sprintf("`-%s`, `--%s`", f.Shorthand, f.Name)
			}
			return fmt.Sprintf("`--%s`", f.Name)
		},
	}).Parse(commandTemplate)
	if err != nil {
		return err
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, info); err != nil {
		return err
	}

	filename := filepath.Join(g.OutputDir, commandToWikiFilename(cmd)+".md")
	return os.WriteFile(filename, buf.Bytes(), 0644)
}

// Templates

const commandTemplate = `# {{ .FullName }}

{{ .Short }}

## Synopsis

{{ .Long }}

## Usage

` + "```" + `bash
{{ .UseLine }}
` + "```" + `
{{ if .HasExample }}
## Examples

` + "```" + `bash
{{ .Example }}
` + "```" + `
{{ end }}
{{ if .HasLocalFlags }}
## Options

| Flag | Type | Default | Description |
|------|------|---------|-------------|
{{ range .LocalFlags }}| {{ flagName . }} | {{ .Type }} | ` + "`{{ .DefaultValue }}`" + ` | {{ .Description }} |
{{ end }}{{ end }}
{{ if .HasInheritedFlags }}
## Global Options

These options are inherited from parent commands.

| Flag | Type | Default | Description |
|------|------|---------|-------------|
{{ range .InheritedFlags }}| {{ flagName . }} | {{ .Type }} | ` + "`{{ .DefaultValue }}`" + ` | {{ .Description }} |
{{ end }}{{ end }}
{{ if .HasSubCommands }}
## Subcommands

| Command | Description |
|---------|-------------|
{{ range .SubCommands }}| [{{ .FullName }}]({{ wikiLink .FullName }}) | {{ .Short }} |
{{ end }}{{ end }}
{{ if .ParentName }}
---

**Parent command:** [{{ .ParentName }}]({{ wikiLink .ParentName }})
{{ end }}
`
