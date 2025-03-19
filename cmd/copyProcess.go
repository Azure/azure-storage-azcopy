package cmd

import (
	"bufio"
	"fmt"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"os"
	"strings"
)

func (cca *CookedCopyCmdArgs) processArgs() (err error) {
	cca.jobID = azcopyCurrentJobID
	// set up the front end scanning logger
	azcopyScanningLogger = common.NewJobLogger(azcopyCurrentJobID, azcopyLogVerbosity, azcopyLogPathFolder, "-scanning")
	azcopyScanningLogger.OpenLog()
	glcm.RegisterCloseFunc(func() {
		azcopyScanningLogger.CloseLog()
	})
	// if no logging, set this empty so that we don't display the log location
	if azcopyLogVerbosity == common.LogNone {
		azcopyLogPathFolder = ""
	}
	if err = cca.SymlinkHandling.Determine(cca.followSymlinks, cca.preserveSymlinks); err != nil {
		return err
	}

	cca.putBlobSize, err = blockSizeInBytes(cca.PutBlobSizeMB)
	if err != nil {
		return err
	}

	// Everything uses the new implementation of list-of-files now.
	// This handles both list-of-files and include-path as a list enumerator.
	// This saves us time because we know *exactly* what we're looking for right off the bat.
	// Note that exclude-path is handled as a filter unlike include-path.

	// unbuffered so this reads as we need it to rather than all at once in bulk
	listChan := make(chan string)
	var f *os.File
	if cca.ListOfFiles != "" {
		f, err = os.Open(cca.ListOfFiles)

		if err != nil {
			return fmt.Errorf("cannot open %s file passed with the list-of-file flag", cca.ListOfFiles)
		}
	}
	// Prepare UTF-8 byte order marker
	utf8BOM := string([]byte{0xEF, 0xBB, 0xBF})

	go func() {
		defer close(listChan)

		addToChannel := func(v string, paramName string) {
			// empty strings should be ignored, otherwise the source root itself is selected
			if len(v) > 0 {
				warnIfHasWildcard(includeWarningOncer, paramName, v)
				listChan <- v
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

		for _, v := range cca.IncludePathPatterns {
			addToChannel(v, "include-path")
		}
	}()
	if cca.ListOfFiles != "" || len(cca.IncludePathPatterns) > 0 {
		cca.ListOfFilesChannel = listChan
	}
	versionsChan := make(chan string)
	var filePtr *os.File
	// Get file path from user which would contain list of all versionIDs
	// Process the file line by line and then prepare a list of all version ids of the blob.
	if cca.ListOfVersionIDs != "" {
		filePtr, err = os.Open(cca.ListOfVersionIDs)
		if err != nil {
			return fmt.Errorf("cannot open %s file passed with the list-of-versions flag", cca.ListOfVersionIDs)
		}
	}

	go func() {
		defer close(versionsChan)
		addToChannel := func(v string) {
			if len(v) > 0 {
				versionsChan <- v
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

	if cca.ListOfVersionIDs != "" {
		cca.ListOfVersionIDsChannel = versionsChan
	}
	cca.CpkOptions = common.CpkOptions{
		CpkScopeInfo: cca.cpkByName,  // Setting CPK-N
		CpkInfo:      cca.cpkByValue, // Setting CPK-V
		// Get the key (EncryptionKey and EncryptionKeySHA256) value from environment variables when required.
	}
	if cca.CpkOptions.CpkScopeInfo != "" || cca.CpkOptions.CpkInfo {
		// We only support transfer from source encrypted by user key when user wishes to download.
		// Due to service limitation, S2S transfer is not supported for source encrypted by user key.
		if cca.FromTo.IsDownload() || cca.FromTo.IsDelete() {
			glcm.Info("Client Provided Key (CPK) for encryption/decryption is provided for download or delete scenario. " +
				"Assuming source is encrypted.")
			cca.CpkOptions.IsSourceEncrypted = true
		}

		// TODO: Remove these warnings once service starts supporting it
		if cca.blockBlobTier != common.EBlockBlobTier.None() || cca.pageBlobTier != common.EPageBlobTier.None() {
			glcm.Info("Tier is provided by user explicitly. Ignoring it because Azure Service currently does" +
				" not support setting tier when client provided keys are involved.")
		}
	}
	if cca.preserveSMBInfo && !cca.isUserPersistingPermissions {
		glcm.Info("Please note: the preserve-permissions flag is set to false, thus AzCopy will not copy SMB ACLs between the source and destination. To learn more: https://aka.ms/AzCopyandAzureFiles.")
	}
	cca.preservePermissionsOption = common.NewPreservePermissionsOption(cca.isUserPersistingPermissions, cca.PreserveOwner.Value(), cca.FromTo)

	return nil
}
