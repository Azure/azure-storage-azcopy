package e2etest

import (
	"os"
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

func setLocalFixtureTimeRelativeTo(a Asserter, local, reference ObjectResourceManager, offset time.Duration) {
	if dryrun, ok := a.(DryrunAsserter); ok && dryrun.Dryrun() {
		return
	}
	a.AssertNow("timestamp fixture must be local", Equal{}, local.Location(), common.ELocation.Local())
	modified := reference.GetProperties(a).LastModifiedTime
	a.AssertNow("reference fixture must report modification time", Not{IsNil{}}, modified)
	if modified == nil {
		return
	}
	target := modified.Add(offset)
	a.NoError("set fixture modification time relative to reference", os.Chtimes(local.URI(), target, target), true)
}
