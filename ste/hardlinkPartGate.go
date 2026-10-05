package ste

import (
	"sync"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

// hardlinkPartGate retains runtime state, not mapped plans: completed Mover parts
// can be unmapped before the final hardlink part arrives.
type hardlinkPartGate struct {
	mu        sync.Mutex
	parts     map[PartNumber]common.JobPartType
	completed map[PartNumber]bool
	pending   []IJobPartMgr
	finalPart PartNumber
	finalSeen bool
	cancelled bool
}

func (g *hardlinkPartGate) reset() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.parts = nil
	g.completed = nil
	g.pending = nil
	g.finalSeen = false
	g.cancelled = false
}

func (g *hardlinkPartGate) register(part PartNumber, kind common.JobPartType, final bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.parts == nil {
		g.parts = make(map[PartNumber]common.JobPartType)
		g.completed = make(map[PartNumber]bool)
	}
	g.parts[part] = kind
	delete(g.completed, part)
	if final {
		g.finalSeen = true
		g.finalPart = part
	}
}

func (g *hardlinkPartGate) enqueue(part IJobPartMgr) []IJobPartMgr {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.pending = append(g.pending, part)
	return g.takeReady()
}

func (g *hardlinkPartGate) complete(part PartNumber) []IJobPartMgr {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.completed != nil {
		g.completed[part] = true
	}
	return g.takeReady()
}

func (g *hardlinkPartGate) cancel() []IJobPartMgr {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.cancelled = true
	return g.takeReady()
}

func (g *hardlinkPartGate) takeReady() []IJobPartMgr {
	if len(g.pending) == 0 {
		return nil
	}
	if !g.cancelled {
		if !g.finalSeen || uint64(len(g.parts)) != uint64(g.finalPart)+1 {
			return nil
		}
		for number, kind := range g.parts {
			if kind != common.EJobPartType.Hardlink() && !g.completed[number] {
				return nil
			}
		}
	}
	ready := g.pending
	g.pending = nil
	return ready
}

func (jm *jobMgr) dispatchHardlinkParts(parts []IJobPartMgr) {
	if len(parts) == 0 {
		return
	}
	// Never block the completion consumer on a full scheduling channel. Its next
	// receive may be needed by the scheduler that will make room in that channel.
	jm.drainTracker.add(1)
	go func() {
		defer jm.drainTracker.done(1)
		for _, part := range parts {
			jm.coordinatorChannels.partsChannel <- part
		}
	}()
}
