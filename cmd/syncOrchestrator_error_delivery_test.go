//go:build smslidingwindow
// +build smslidingwindow

package cmd

import (
	"errors"
	"testing"
)

func TestSyncOrchestratorErrorDelivery(t *testing.T) {
	ch := fullErrorChannel()
	defer RequireErrorDelivery(ch)()
	checkDeliveryWaits(t, "writeSyncErrToChannel", ch, func() {
		writeSyncErrToChannel(ch, SyncOrchErrorInfo{ErrorMsg: errors.New("delivered")})
	})
}
