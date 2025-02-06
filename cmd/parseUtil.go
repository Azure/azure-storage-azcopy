package cmd

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"strings"
)

func NormalizeIncludePaths(listOfFilesPath string, includePath []string) (includePaths chan string, err error) {
	// A combined implementation reduces the amount of code duplication present.
	// However, it _does_ increase the amount of code-intertwining present.
	if listOfFilesPath != "" && len(includePath) != 0 {
		return nil, errors.New("cannot combine list of files and include path")
	}

	// unbuffered so this reads as we need it to rather than all at once in bulk
	includePaths = make(chan string)
	var f *os.File
	if listOfFilesPath != "" {
		f, err = os.Open(listOfFilesPath)
		if err != nil {
			return includePaths, fmt.Errorf("cannot open %s file passed with --list-of-files", listOfFilesPath)
		}
	}
	// Prepare UTF-8 byte order marker
	utf8BOM := string([]byte{0xEF, 0xBB, 0xBF})

	go func() {
		defer close(includePaths)

		addToChannel := func(v string, paramName string) {
			// empty strings should be ignored, otherwise the source root itself is selected
			if len(v) > 0 {
				warnIfHasWildcard(includeWarningOncer, paramName, v)
				includePaths <- v
			}
		}

		if f != nil {
			scanner := bufio.NewScanner(f)
			checkBOM := false
			headerLineNum := 0
			firstLineIsCurlyBrace := false

			for scanner.Scan() {
				v := scanner.Text()

				// Check if the UTF-8 BOM is on the first line and remove it if necessary.
				// Note that the UTF-8 BOM can be present on the same line feed as the first line of actual data, so just use TrimPrefix.
				// If the line feed were separate, the empty string would be skipped later.
				if !checkBOM {
					v = strings.TrimPrefix(v, utf8BOM)
					checkBOM = true
				}

				// provide clear warning if user uses old (obsolete) format by mistake
				if headerLineNum <= 1 {
					cleanedLine := strings.Replace(strings.Replace(v, " ", "", -1), "\t", "", -1)
					cleanedLine = strings.TrimSuffix(cleanedLine, "[") // don't care which line this is on, could be third line
					if cleanedLine == "{" && headerLineNum == 0 {
						firstLineIsCurlyBrace = true
					} else {
						const jsonStart = "{\"Files\":"
						jsonStartNoBrace := strings.TrimPrefix(jsonStart, "{")
						isJson := cleanedLine == jsonStart || firstLineIsCurlyBrace && cleanedLine == jsonStartNoBrace
						if isJson {
							glcm.Error("The format for list-of-files has changed. The old JSON format is no longer supported")
						}
					}
					headerLineNum++
				}

				addToChannel(v, "list-of-files")
			}
		}
		for _, v := range includePath {
			addToChannel(v, "include-path")
		}
	}()
	return
}

func ProcessVersionIds(versionIds string) (versions chan string, err error) {
	// Prepare UTF-8 byte order marker
	utf8BOM := string([]byte{0xEF, 0xBB, 0xBF})

	versions = make(chan string)
	var filePtr *os.File
	// Get file path from user which would contain list of all versionIDs
	// Process the file line by line and then prepare a list of all version ids of the blob.
	if versionIds != "" {
		filePtr, err = os.Open(versionIds)
		if err != nil {
			return nil, fmt.Errorf("cannot open %s file passed with the list-of-versions flag", versionIds)
		}
	}

	go func() {
		defer close(versions)
		addToChannel := func(v string) {
			if len(v) > 0 {
				versions <- v
			}
		}

		if filePtr != nil {
			scanner := bufio.NewScanner(filePtr)
			checkBOM := false
			for scanner.Scan() {
				v := scanner.Text()

				if !checkBOM {
					v = strings.TrimPrefix(v, utf8BOM)
					checkBOM = true
				}

				addToChannel(v)
			}
		}
	}()
	return
}
