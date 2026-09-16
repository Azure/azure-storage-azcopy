package e2etest

import (
	"context"
	"strings"
	"sync"
	"time"
)

var azCLIStartupSlot = make(chan struct{}, 1)

func needsAzCLIStartupGate(platform, loginMode string, debugging bool) bool {
	return platform == "darwin" && strings.EqualFold(loginMode, "azcli") && !debugging
}

func acquireAzCLIStartup(ctx context.Context, slot chan struct{}, budget time.Duration) (func(), error) {
	ctx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	select {
	case slot <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	release := sync.OnceFunc(func() { <-slot })
	timer := time.AfterFunc(budget, release)
	return func() {
		timer.Stop()
		release()
	}, nil
}
