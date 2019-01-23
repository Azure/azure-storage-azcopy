package cmd

import (
	"fmt"
	"github.com/Azure/azure-storage-azcopy/common"
	"os"
	"path/filepath"
	"strings"
)

type syncTransferProcessor struct {
	numOfTransfersPerPart int
	copyJobTemplate       *common.CopyJobPartOrderRequest
	source                string
	destination           string

	// keep a handle to initiate progress tracking
	cca *cookedSyncCmdArgs
}

func newSyncTransferProcessor(cca *cookedSyncCmdArgs, numOfTransfersPerPart int) *syncTransferProcessor {
	processor := syncTransferProcessor{}
	processor.copyJobTemplate = &common.CopyJobPartOrderRequest{
		JobID:         cca.jobID,
		CommandString: cca.commandString,
		FromTo:        cca.fromTo,

		// authentication related
		CredentialInfo: cca.credentialInfo,
		SourceSAS:      cca.sourceSAS,
		DestinationSAS: cca.destinationSAS,

		// flags
		BlobAttributes: common.BlobTransferAttributes{
			PreserveLastModifiedTime: true,
			MD5ValidationOption:      cca.md5ValidationOption,
		},
		ForceWrite: true,
		LogLevel:   cca.logVerbosity,
	}

	// useful for building transfers later
	processor.source = cca.source
	processor.destination = cca.destination

	processor.cca = cca
	processor.numOfTransfersPerPart = numOfTransfersPerPart
	return &processor
}

func (s *syncTransferProcessor) process(entity genericEntity) (err error) {
	if len(s.copyJobTemplate.Transfers) == s.numOfTransfersPerPart {
		err = s.sendPartToSte()
		if err != nil {
			return err
		}

		// reset the transfers buffer
		s.copyJobTemplate.Transfers = []common.CopyTransfer{}
		s.copyJobTemplate.PartNum++
	}

	// only append the transfer after we've checked and dispatched a part
	// so that there is at least one transfer for the final part
	s.copyJobTemplate.Transfers = append(s.copyJobTemplate.Transfers, common.CopyTransfer{
		Source:           s.appendEntityPathToResourcePath(entity.relativePath, s.source),
		Destination:      s.appendEntityPathToResourcePath(entity.relativePath, s.destination),
		SourceSize:       entity.size,
		LastModifiedTime: entity.lastModifiedTime,
	})
	return nil
}

func (s *syncTransferProcessor) appendEntityPathToResourcePath(entityPath, parentPath string) string {
	if entityPath == "" {
		return parentPath
	}

	return strings.Join([]string{parentPath, entityPath}, common.AZCOPY_PATH_SEPARATOR_STRING)
}

func (s *syncTransferProcessor) dispatchFinalPart() (copyJobInitiated bool, err error) {
	numberOfCopyTransfers := len(s.copyJobTemplate.Transfers)

	// if the number of transfer to copy is
	// and no part was dispatched, then it means there is no work to do
	if s.copyJobTemplate.PartNum == 0 && numberOfCopyTransfers == 0 {
		return false, nil
	}

	if numberOfCopyTransfers > 0 {
		s.copyJobTemplate.IsFinalPart = true
		err = s.sendPartToSte()
		if err != nil {
			return false, err
		}
	}

	s.cca.isEnumerationComplete = true
	return true, nil
}

func (s *syncTransferProcessor) sendPartToSte() error {
	var resp common.CopyJobPartOrderResponse
	Rpc(common.ERpcCmd.CopyJobPartOrder(), (*common.CopyJobPartOrderRequest)(s.copyJobTemplate), &resp)
	if !resp.JobStarted {
		return fmt.Errorf("copy job part order with JobId %s and part number %d failed to dispatch because %s",
			s.copyJobTemplate.JobID, s.copyJobTemplate.PartNum, resp.ErrorMsg)
	}

	// if the current part order sent to ste is 0, then alert the progress reporting routine
	if s.copyJobTemplate.PartNum == 0 {
		s.cca.setFirstPartOrdered()
	}

	return nil
}

type syncLocalDeleteProcessor struct {
	rootPath string

	// ask the user for permission the first time we delete a file
	hasPromptedUser bool

	// note down whether any delete should happen
	shouldDelete bool

	// keep a handle for progress tracking
	cca *cookedSyncCmdArgs
}

func newSyncLocalDeleteProcessor(cca *cookedSyncCmdArgs, isSource bool) *syncLocalDeleteProcessor {
	rootPath := cca.source
	if !isSource {
		rootPath = cca.destination
	}

	return &syncLocalDeleteProcessor{rootPath: rootPath, cca: cca, hasPromptedUser: false}
}

func (s *syncLocalDeleteProcessor) process(entity genericEntity) (err error) {
	if !s.hasPromptedUser {
		s.shouldDelete = s.promptForConfirmation()
	}

	if !s.shouldDelete {
		return nil
	}

	err = os.Remove(filepath.Join(s.rootPath, entity.relativePath))
	if err != nil {
		glcm.Info(fmt.Sprintf("error %s deleting the file %s", err.Error(), entity.relativePath))
	}

	return
}

func (s *syncLocalDeleteProcessor) promptForConfirmation() (shouldDelete bool) {
	shouldDelete = false

	// omit asking if the user has already specified
	if s.cca.force {
		shouldDelete = true
	} else {
		answer := glcm.Prompt(fmt.Sprintf("Sync has discovered local files that are not present at the source, would you like to delete them? Please confirm with y/n: "))
		if answer == "y" || answer == "yes" {
			shouldDelete = true
			glcm.Info("Confirmed. The extra local files will be deleted.")
		} else {
			glcm.Info("No deletions will happen.")
		}
	}

	s.hasPromptedUser = true
	return
}

func (s *syncLocalDeleteProcessor) wasAnyFileDeleted() bool {
	// we'd have prompted the user if any entity was passed in
	return s.hasPromptedUser
}
