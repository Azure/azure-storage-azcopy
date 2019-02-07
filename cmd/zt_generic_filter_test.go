package cmd

import (
	chk "gopkg.in/check.v1"
)

type genericFilterSuite struct{}

var _ = chk.Suite(&genericFilterSuite{})

func (s *genericFilterSuite) TestIncludeFilter(c *chk.C) {
	// set up the filters
	raw := rawSyncCmdArgs{}
	includePatternList := raw.parsePatterns("*.pdf;*.jpeg;exactName")
	includeFilter := buildIncludeFilters(includePatternList)[0]

	// test the positive cases
	filesToPass := []string{"bla.pdf", "fancy.jpeg", "socool.jpeg.pdf", "exactName"}
	for _, file := range filesToPass {
		passed := includeFilter.doesPass(storedObject{name: file})
		c.Assert(passed, chk.Equals, true)
	}

	// test the negative cases
	filesNotToPass := []string{"bla.pdff", "fancyjpeg", "socool.jpeg.pdf.wut", "eexactName"}
	for _, file := range filesNotToPass {
		passed := includeFilter.doesPass(storedObject{name: file})
		c.Assert(passed, chk.Equals, false)
	}
}

func (s *genericFilterSuite) TestExcludeFilter(c *chk.C) {
	// set up the filters
	raw := rawSyncCmdArgs{}
	excludePatternList := raw.parsePatterns("*.pdf;*.jpeg;exactName")
	excludeFilterList := buildExcludeFilters(excludePatternList)

	// test the positive cases
	filesToPass := []string{"bla.pdfe", "fancy.jjpeg", "socool.png", "eexactName"}
	for _, file := range filesToPass {
		dummyProcessor := &dummyProcessor{}
		err := processIfPassedFilters(excludeFilterList, storedObject{name: file}, dummyProcessor.process)
		c.Assert(err, chk.IsNil)
		c.Assert(len(dummyProcessor.record), chk.Equals, 1)
	}

	// test the negative cases
	filesToNotPass := []string{"bla.pdf", "fancy.jpeg", "socool.jpeg.pdf", "exactName"}
	for _, file := range filesToNotPass {
		dummyProcessor := &dummyProcessor{}
		err := processIfPassedFilters(excludeFilterList, storedObject{name: file}, dummyProcessor.process)
		c.Assert(err, chk.IsNil)
		c.Assert(len(dummyProcessor.record), chk.Equals, 0)
	}
}
