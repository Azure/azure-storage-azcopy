package cmd

import (
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/jobsAdmin"
	"github.com/Azure/azure-storage-azcopy/v10/ste"
	"github.com/Azure/azure-storage-azcopy/v10/testSuite/cmd"
	"github.com/stretchr/testify/assert"
	"log"
	"os"
	"path"
	"testing"
)

func TestUnit(t *testing.T) {
	a := assert.New(t)
	azcopyCurrentJobID = common.NewJobID()
	azcopyLogPathFolder = common.GetLifecycleMgr().GetEnvironmentVariable(common.EEnvironmentVariable.LogLocation())     // user specified location for log files
	azcopyJobPlanFolder := common.GetLifecycleMgr().GetEnvironmentVariable(common.EEnvironmentVariable.JobPlanLocation()) // user specified location for plan files

	// note: azcopyAppPathFolder is the default location for all AzCopy data (logs, job plans, oauth token on Windows)
	// but all the above can be put elsewhere as they can become very large
	azcopyAppPathFolder := cmd.GetAzCopyAppPath()

	// the user can optionally put the log files somewhere else
	if azcopyLogPathFolder == "" {
		azcopyLogPathFolder = azcopyAppPathFolder
	}
	if err := os.Mkdir(azcopyLogPathFolder, os.ModeDir|os.ModePerm); err != nil && !os.IsExist(err) {
		log.Fatalf("Problem making .azcopy directory. Try setting AZCOPY_LOG_LOCATION env variable. %v", err)
	}

	// the user can optionally put the plan files somewhere else
	if azcopyJobPlanFolder == "" {
		// make the app path folder ".azcopy" first so we can make a plans folder in it
		if err := os.MkdirAll(azcopyAppPathFolder, os.ModeDir); err != nil && !os.IsExist(err) {
			log.Fatalf("Problem making .azcopy directory. Try setting AZCOPY_JOB_PLAN_LOCATION env variable. %v", err)
		}
		azcopyJobPlanFolder = path.Join(azcopyAppPathFolder, "plans")
	}

	if err := os.MkdirAll(azcopyJobPlanFolder, os.ModeDir|os.ModePerm); err != nil && !os.IsExist(err) {
		log.Fatalf("Problem making .azcopy directory. Try setting AZCOPY_JOB_PLAN_LOCATION env variable. %v", err)
	}
	common.AzcopyCurrentJobLogger = common.NewJobLogger(azcopyCurrentJobID, azcopyLogVerbosity, azcopyLogPathFolder, "")
	common.AzcopyCurrentJobLogger.OpenLog()
	common.AzcopyJobPlanFolder = azcopyJobPlanFolder
	concurrencySettings := ste.NewConcurrencySettings(azcopyMaxFileAndSocketHandles, false)
	err := jobsAdmin.MainSTE(concurrencySettings, float64(cmdLineCapMegaBitsPerSecond), azcopyJobPlanFolder, azcopyLogPathFolder, false)
	a.Nil(err)
	rawCopy := rawCopyCmdArgs{
		src: "https://azcopyperftestsource.blob.core.windows.net/noaa-bathymetry-pds/?sp=racwdli&st=2023-07-13T18:21:20Z&se=2024-07-14T02:21:20Z&spr=https&sv=2022-11-02&sr=c&sig=r2qpNrnESM0of9uEZaduJezFtAiZLj3lykMK4%2BX7gDA%3D",
		dst: "https://azcopyperftestdst.blob.core.windows.net/noaa-bathymetry-pds/?sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupiytfx&se=2024-07-14T02:25:51Z&st=2023-07-13T18:25:51Z&spr=https&sig=8%2FUXpjeOPp2fTqIWW5zEfm1UtDZJRffXFpxpwHKIpKM%3D",
		recursive: true,
		blockSizeMB: 128,
		forceWrite: "true",
		blobType: "blockblob",
		blockBlobTier: "none",
		pageBlobTier: "none",
		md5ValidationOption: "FailIfDifferent",
		preserveOwner: true,
		s2sInvalidMetadataHandleOption: "ExcludeIfInvalid",
	}
	cooked, err := rawCopy.cook()
	a.Nil(err)

	cooked.commandString = copyHandlerUtil{}.ConstructCommandStringFromArgs()
	err = cooked.process()
	a.Nil(err)
}