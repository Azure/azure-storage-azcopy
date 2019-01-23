package cmd

import (
	chk "gopkg.in/check.v1"
)

type syncFilterSuite struct{}

var _ = chk.Suite(&syncFilterSuite{})

func (s *syncFilterSuite) TestIncludeFilter(c *chk.C) {
	// set up the filters
	raw := rawSyncCmdArgs{}
	includePatternList := raw.parsePatterns("*.pdf;*.jpeg;exactName")
	includeFilter := buildIncludeFilters(includePatternList)[0]

	// test the positive cases
	filesToInclude := []string{"bla.pdf", "fancy.jpeg", "socool.jpeg.pdf", "exactName"}
	for _, file := range filesToInclude {
		passed := includeFilter.pass(genericEntity{name: file})
		c.Assert(passed, chk.Equals, true)
	}

	// test the negative cases
	notToInclude := []string{"bla.pdff", "fancyjpeg", "socool.jpeg.pdf.wut", "eexactName"}
	for _, file := range notToInclude {
		passed := includeFilter.pass(genericEntity{name: file})
		c.Assert(passed, chk.Equals, false)
	}
}

func (s *syncFilterSuite) TestExcludeFilter(c *chk.C) {
	// set up the filters
	raw := rawSyncCmdArgs{}
	excludePatternList := raw.parsePatterns("*.pdf;*.jpeg;exactName")
	excludeFilterList := buildExcludeFilters(excludePatternList)

	// test the positive cases
	filesToPass := []string{"bla.pdfe", "fancy.jjpeg", "socool.png", "notexactName"}
	for _, file := range filesToPass {
		dummyProcessor := &dummyProcessor{}
		passed := processIfPassedFilters(excludeFilterList, genericEntity{name: file}, dummyProcessor)
		c.Assert(passed, chk.Equals, true)
	}

	// test the negative cases
	filesToNotPass := []string{"bla.pdff", "fancyjpeg", "socool.jpeg.pdf.wut", "eexactName"}
	for _, file := range filesToNotPass {
		passed := excludeFilter.pass(genericEntity{name: file})
		c.Assert(passed, chk.Equals, false)
	}
}
