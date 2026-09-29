# Bandwidth Token Management — V4 Dual-Mode Design

**Proactive Equal-Share + Reactive AIMD (Cosmos DB-Based)**  
**Status:** Design

---

## Table of Contents

1. [Problem Statement](#1-problem-statement)
2. [Design Modes](#2-design-modes)
3. [Architecture Overview](#3-architecture-overview)
4. [Cosmos DB Data Model](#4-cosmos-db-data-model)
5. [Mode A: Proactive Equal-Share (Bandwidth-Capped Jobs)](#5-mode-a-proactive-equal-share-bandwidth-capped-jobs)
6. [Mode B: Reactive AIMD (Uncapped Jobs)](#6-mode-b-reactive-aimd-uncapped-jobs)
7. [Active Worker Tracking via Cosmos DB](#7-active-worker-tracking-via-cosmos-db)
8. [Throttle Detection & Signal Processing](#8-throttle-detection--signal-processing)
9. [Worker Design — Unified Rate Limiter](#9-worker-design--unified-rate-limiter)
10. [Multi-Worker Scaling Walkthrough](#10-multi-worker-scaling-walkthrough)
11. [Cosmos DB Failure Mitigation](#11-cosmos-db-failure-mitigation)
12. [Edge Cases & Starvation Fixes](#12-edge-cases--starvation-fixes)
13. [Metrics & Observability](#13-metrics--observability)
14. [Configuration Reference](#14-configuration-reference)
15. [Go Implementation](#15-go-implementation)

---

## 1. Problem Statement

Customer jobs transfer data across workers in a shared, dynamically-scaled pool. Two categories of jobs exist:

| Category | Customer intent | System behavior needed |
|----------|----------------|----------------------|
| **Bandwidth-capped** | "Do not exceed X MB/s" | Hard proactive enforcement across all workers |
| **Uncapped** | "Go as fast as the storage allows" | No pre-set limit; react to storage throttling signals (429/503) |

The system currently supports two copy scenarios:

| Scenario | Operations | Control requirement |
|----------|------------|---------------------|
| **Blob -> Blob** | Data transfer operations | Single-resource bandwidth control; transition to reactive only on sustained throttling (not momentary spikes) |
| **AzureFiles -> AzureFiles** | Data (`PutRangeFromURL`) + metadata (`GetProperties`) | Must honor both bandwidth and IOPS with dynamic mode switching: Proactive when GetShareStats shows no throttles, Reactive when throttle counters indicate sustained server-side exhaustion |

**Key architectural decision:** This design replaces Redis with **Cosmos DB** (already present in infrastructure) for coordination state. Workers poll Cosmos DB for job configuration and active worker count, then self-enforce bandwidth limits locally. There is no centralized atomic grant bucket — each worker independently computes and enforces its share.

**AzureFiles-specific decision:** For AzureFiles -> AzureFiles copy, workers dynamically switch between Proactive and Reactive modes based on `GetShareStats` API response every 30 seconds:
- **Proactive mode** when no new throttles are observed in the latest poll window (`deltaIopsThrottledRequestCount == 0` AND `deltaEgressThrottledBytes == 0`), using equal-share dual buckets (IOPS + Bandwidth) from API limits.
- **Reactive mode** when either per-poll throttle delta is non-zero, applying per-resource AIMD to converge toward storage capacity.

---

## 2. Design Modes

Mode selection is driven by `jobRate` for Blob jobs, and by both `jobRate` and `GetShareStats` throttle state for AzureFiles jobs.

```
Job Registration (Orchestrator writes to Cosmos DB)
     │
     ├── Scenario: Blob -> Blob?
     │   ├── jobRate > 0?
     │   │   └── YES → Mode A: Proactive Equal-Share
     │   │              • Worker polls Cosmos DB for: jobRate + activeWorkerCount
     │   │              • Local enforcement: workerShare = jobRate / activeWorkerCount
     │   │              • Local token bucket paces at workerShare
     │   │              • Transition to Reactive only on sustained throttling (30s window)
     │   │              • No centralized grant bucket — self-enforced
     │   │
     │   └── jobRate == 0 (or absent)?
     │       └── Mode B: Reactive AIMD
     │                • Worker runs uncapped until 429/503
     │                • On throttle: multiplicative decrease (÷2)
     │                • On success: additive increase (+step)
     │                • Release cap after sustained quiet period
     │
     └── Scenario: AzureFiles -> AzureFiles?
         └── Poll GetShareStats every 30s, check throttle counters
             ├── deltaIopsThrottledRequestCount == 0 AND deltaEgressThrottledBytes == 0?
             │   └── YES → Mode A: Proactive Dual-Bucket
             │              • Dual buckets: IOPS + Bandwidth (independent rates)
             │              • Use IopsLimit + BurstIopsAvailable → workerIopsShare
             │              • Use BandwidthLimitMiBps → workerBandwidthShare
             │              • Equal-share across active workers
             │
             └── Any per-poll throttle delta > 0?
                 └── YES → Mode R: Reactive AIMD (per-resource)
                            • Apply AIMD independently to IOPS and Bandwidth
                            • Exponential backoff: backoff = max(iopsBackoff, bwBackoff)
                            • Transition back to Proactive after quiet window (no counter deltas)
```

### 2.1 Mode Behaviors Across Scenarios

| Aspect | Blob (Capped, Mode A) | Blob (Uncapped, Mode B) | AzureFiles (Mode A → R) |
|---|---|---|---|
| **Control signal** | Cosmos DB: jobRate + worker count | HTTP 429/503 from API | GetShareStats throttle counters (every 30s) |
| **Transition trigger** | Sustained throttling window (30s, 12+ events, 20% ratio) | First 429/503 | `deltaIopsThrottledRequestCount > 0` OR `deltaEgressThrottledBytes > 0` |
| **Back to Proactive** | N/A (transition only via sustained gate) | 5 min quiet + jitter | No counter deltas for quiet window |
| **Resource control** | Bandwidth only (equal-share) | Bandwidth only (AIMD) | Bandwidth + IOPS (per-resource AIMD in reactive) |
| **Enforcement** | Local hard ceiling per share | Local soft cap (converges) | Dual buckets: hard ceiling in proactive, soft cap in reactive |
| **Overshoot risk** | Brief (~1 poll interval) | Until first 429 (mitigated by sustained gate) | N/A (proactive budgets from server limits) |

---

## 3. Architecture Overview

```
┌──────────────────────────────────────────────────────────────────────────┐
│                   Azure Container Apps Environment                         │
│                                                                           │
│  ┌──────────────────────────────┐                                         │
│  │  Orchestrator                │                                         │
│  │  ┌─────────────────────────┐ │                                         │
│  │  │  BandwidthManager       │ │  Write job doc (rate, config)           │
│  │  │  registerJob(rate|nil)  │ | ───────────────────────┐                │
│  │  │  completeJob()          │ │                        │                │
│  │  └─────────────────────────┘ │                        │                │
│  └──────────────────────────────┘                        │                │
│                                                          │                │
│  ┌──────────────────────────────────────────────────────┐│                │
│  │  Shared Worker Pool (auto-scaled)                    ││                │
│  │                                                      ││                │
│  │  Worker 1          Worker 2          Worker N        ││                │
│  │  ┌──────────┐     ┌──────────┐     ┌──────────┐      ││                │
│  │  │JP-07(A)  │     │JP-22(B)  │     │JP-88(A)  │      ││                │
│  │  │JP-12(A)  │     │JP-45(A)  │     │JP-46(B)  │      ││                │
│  │  │          │     │          │     │          │      ││                │
│  │  │RateLimiter│    │RateLimiter│    │RateLimiter│     ││                │
│  │  │ Job A(P) │     │ Job A(P) │     │ Job A(P) │      ││                │
│  │  │ Job B(R) │     │ Job B(R) │     │ Job B(R) │      ││                │
│  │  └────┬─────┘     └────┬─────┘     └────┬─────┘      ││                │
│  │       │                │                │            ││                │
│  └───────┼────────────────┼────────────────┼─────────── ┘│                │
│          │ Poll + Heartbeat│               │             │                │
│          └────────────────┼────────────────┘             │                │
│                           ▼                              │                │
│  ┌────────────────────────────────────────────────────────┐               │
│  │  Azure Cosmos DB (existing infrastructure)             │               │
│  │                                                        │               │
│  │  Container: bandwidth-management                       │               │
│  │  ┌────────────────────────────────────────────────┐    │               │
│  │  │  /jobs/{jobID}        — job config (rate, etc) │◄──┘               │
│  │  │  /workers/{jobID}     — worker heartbeat docs  │                    │
│  │  │  /throttle/{jobID}    — throttle events (TTL)  │                    │
│  │  │  /usage/{jobID}       — usage metrics (TTL)    │                    │
│  │  └────────────────────────────────────────────────┘                    │
│  └────────────────────────────────────────────────────────┘               │
└──────────────────────────────────────────────────────────────────────────┘

(P) = Proactive equal-share mode
(R) = Reactive AIMD mode
```

---

## 4. Cosmos DB Data Model

### 4.1 Container: `bandwidth-management`

**Partition key:** `/jobId`

All documents for a given job are co-located in the same logical partition, enabling efficient single-partition queries.

### 4.2 Document Types

#### Job Configuration Document

Written by Orchestrator at job registration. Workers read this to determine mode and rate.

```json
{
    "id": "job-config",
    "jobId": "job-12345",
    "type": "job-config",
    "rate": 104857600,          // bytes/s (0 or absent = uncapped)
    "maxWorkersPerJob": 50,
    "status": "ACTIVE",         // ACTIVE | THROTTLED | PAUSED | COMPLETED
    "createdAt": "2026-05-06T22:00:00Z",
    "updatedAt": "2026-05-06T22:00:00Z",
    "_etag": "..."
}
```

#### Worker Heartbeat Document

Written by each worker on every poll interval. TTL-based auto-expiry handles crash detection.

```json
{
    "id": "worker-{workerID}",
    "jobId": "job-12345",
    "type": "worker-heartbeat",
    "workerId": "worker-abc-001",
    "lastHeartbeat": "2026-05-06T22:30:00.200Z",
    "ttl": 30,                  // Cosmos DB TTL in seconds — auto-deleted after expiry
    "_etag": "..."
}
```

#### Throttle Event Document (Mode B, and Mode A storage throttling)

Written by workers on 429/503. TTL-based auto-expiry.

```json
{
    "id": "throttle-{workerID}-{timestamp}",
    "jobId": "job-12345",
    "type": "throttle-event",
    "workerId": "worker-abc-001",
    "statusCode": 429,
    "retryAfterMs": 1000,
    "timestamp": "2026-05-06T22:30:05.000Z",
    "ttl": 60                   // expires after 60s
}
```

#### Usage Metrics Document

Written by workers periodically for observability. TTL-based rolling window.

```json
{
    "id": "usage-{workerID}-{windowStart}",
    "jobId": "job-12345",
    "type": "usage-metric",
    "workerId": "worker-abc-001",
    "windowStartUtc": "2026-05-06T22:25:00Z",
    "windowDurationSec": 300,
    "bytesTransferred": 52428800000,
    "avgBps": 174762666,
    "ttl": 3600                 // keep for 1 hour
}
```

### 4.3 Indexing Strategy

| Index | Purpose | RU Impact |
|-------|---------|-----------|
| Partition key (`/jobId`) | All queries scoped to a single job | Optimal — single-partition reads |
| Composite: `type` + `lastHeartbeat` | Count active workers efficiently | Low — partition-scoped |
| TTL policy enabled on container | Auto-cleanup of heartbeats, throttle events, metrics | Zero — system-managed |

### 4.4 RU Cost Estimation

| Operation | Frequency | Estimated RU | Notes |
|-----------|-----------|-------------|-------|
| Worker heartbeat (upsert) | 1–5/s per worker | ~10 RU each | Point write (1KB doc) |
| Read active worker count | 1–5/s per worker | ~3 RU each | COUNT query, single partition |
| Read job config | Once per reservoir creation | ~1 RU | Point read by ID |
| Write throttle event | On 429/503 only | ~10 RU each | Infrequent in normal operation |
| Write usage metric | Every 5 min per worker | ~10 RU each | Negligible |

**At 50 workers, 1 Hz poll:** ~650 RU/s per job. At 5 Hz: ~3,250 RU/s per job. Configure poll interval based on RU budget.

---

## 5. Mode A: Proactive Equal-Share (Bandwidth-Capped Jobs)

### 5.1 Core Principle

Each worker self-enforces an **equal share** of the job's configured bandwidth:

$$
workerShare = \frac{jobRate}{activeWorkerCount}
$$

Workers poll Cosmos DB to discover `activeWorkerCount` and adjust their local token bucket rate accordingly. There is no centralized grant bucket — enforcement is purely local.

### 5.2 Why Equal-Share Instead of Demand-Based

| Aspect | Demand-based (V3 with Redis) | Equal-share (V4 with Cosmos DB) |
|--------|------------------------------|--------------------------------|
| New worker convergence | ~3s ramp via doubling | **Instant** — share recomputes on next poll |
| Heavy consumer squeeze | Organic via Lua (slow) | Immediate — share drops on worker join |
| Coordination | Atomic Lua scripts in Redis | None — each worker self-enforces |
| Infrastructure | Requires Redis | Uses existing Cosmos DB |
| Simplicity | Complex Lua + return logic | Simple: poll count, divide rate, pace locally |
| Fairness guarantee | Emergent | **By construction** |

### 5.3 Worker Share Computation

```go
func (p *ProactiveRateLimiter) computeShare() int64 {
    activeCount := p.getActiveWorkerCount() // query Cosmos DB
    if activeCount < 1 {
        activeCount = 1
    }
    return p.jobRate / int64(activeCount)
}
```

### 5.4 Local Token Bucket Enforcement

Once the share is computed, the worker paces all transfers for that job using a local token bucket:

```
Token bucket parameters:
    rate     = workerShare (bytes/s)
    capacity = workerShare × burstWindow (e.g., 0.2s = one poll interval worth)
```

The token bucket refills at `workerShare` bytes/s continuously. JP goroutines call `AcquireTokens(n)` which blocks if insufficient tokens are available. This ensures the worker never exceeds its share regardless of how many JPs it has for that job.

### 5.5 Polling Cosmos DB for Worker Count

```go
func (p *ProactiveRateLimiter) pollLoop() {
    ticker := time.NewTicker(p.pollInterval) // 200ms–1s, configurable
    defer ticker.Stop()

    for {
        select {
        case <-ticker.C:
            p.refreshHeartbeat()     // upsert own heartbeat doc
            count := p.queryActiveWorkerCount()
            p.updateShare(count)
        case <-p.stopCh:
            p.deregister()           // delete heartbeat doc
            return
        }
    }
}
```

### 5.6 Share Update — Immediate Effect

When the active worker count changes, the local token bucket rate adjusts immediately:

```go
func (p *ProactiveRateLimiter) updateShare(newCount int64) {
    p.mu.Lock()
    defer p.mu.Unlock()

    if newCount == p.activeWorkerCount {
        return // no change
    }

    p.activeWorkerCount = newCount
    p.currentShare = p.jobRate / newCount

    // Update local token bucket rate
    p.pacer.SetRate(p.currentShare)
    // Adjust burst capacity proportionally
    p.pacer.SetCapacity(p.currentShare / int64(PollsPerSecond))
}
```

### 5.7 Overshoot Analysis

Without a centralized atomic guard (Redis Lua), brief overshoot is possible during worker count transitions:

| Scenario | Overshoot window | Magnitude | Acceptable? |
|----------|-----------------|-----------|-------------|
| Worker joins, others haven't polled yet | 1 poll interval (200ms–1s) | `jobRate / (N-1)` instead of `jobRate / N` | ✓ Negligible |
| Worker crashes, heartbeat hasn't expired | Up to `workerTTL` (30s) | Under-utilization, not overshoot | ✓ Self-heals |
| N workers all start simultaneously | 1 poll interval | Up to `N × jobRate` | Rare; bucket on first poll |

**Worst-case overshoot:** One poll interval × `jobRate`. For a 100 MB/s job at 1s poll: 100MB one-time burst. For 200ms poll: 20MB. This is comparable to V3's `bucketCapacity` and is acceptable for bandwidth enforcement.

### 5.8 Timeline: Worker Joins with Equal-Share

```
Config: jobRate = 100 MB/s, pollInterval = 500ms, workerTTL = 30s

t=0s: W1 online → writes heartbeat → queries count → activeWorkers=1
      W1 share = 100/1 = 100 MB/s
      Local token bucket rate = 100 MB/s ✓

t=1s: W2 online → writes heartbeat → queries count → activeWorkers=2
      W2 share = 100/2 = 50 MB/s (W2 starts at correct rate immediately)

t=1.5s: W1 polls → sees activeWorkers=2
      W1 share drops to 50 MB/s → token bucket rate adjusted instantly
      ★ W1 was over-consuming for 0.5s (one poll interval) — brief overshoot

t=2s: Steady state
      W1 = 50 MB/s, W2 = 50 MB/s, Total = 100 MB/s ✓

t=5s: W3 online → queries → activeWorkers=3
      All converge to 33.3 MB/s within one poll interval

t=10s: W2 crashes — no deregistration
      W2's heartbeat TTL = 30s → expires at t=40s
      t=10–40s: W1, W3 still see count=3, share=33.3 MB/s (under-utilized)
      t=40s: W2 doc expires, count drops to 2, shares become 50 MB/s
```

### 5.9 AzureFiles -> AzureFiles: Proactive-Reactive Dual-Bucket Control

For AzureFiles -> AzureFiles copy, the worker maintains **two independent local token buckets** (IOPS and Bandwidth) and switches dynamically between proactive and reactive modes based on throttling signals from `GetShareStats`.

#### 5.9.1 GetShareStats Polling and Structure

Workers poll `GetShareStats` API every 30 seconds to detect throttling at the share level:

```json
{
    "ShareUsageBytes": 1099511627776,
    "ShareQuotaBytes": 1099511627776,
    "IopsLimit": 20000,
    "BurstIopsAvailable": 10000,
    "BandwidthLimitMiBps": 1000,
    "IopsThrottledRequestCount": 0,
    "EgressThrottledBytes": 0,
    "IngressThrottledBytes": 0
}
```

**Key throttle indicators:**
- `IopsThrottledRequestCount > 0`: Share-level IOPS exhaustion detected
- `EgressThrottledBytes > 0`: Egress (download) bandwidth exhaustion detected
- `IngressThrottledBytes > 0`: Ingress (upload) bandwidth exhaustion detected (for completeness, though Data Box Edge typically uploads)

#### 5.9.2 Mode Transition Logic

The worker operates in one of two modes, determined by per-poll throttle counter deltas from `GetShareStats`:

```
┌─────────────────────────────────────┐
│  GetShareStats Poll (every 30s)     │
│  Check deltas:                      │
│    deltaIopsThrottledRequestCount   │
│    deltaEgressThrottledBytes        │
└────────────┬────────────────────────┘
             │
             ├─ Both deltas == 0?
             │  YES → PROACTIVE MODE ────────────┐
             │                                   │
             │  NO → REACTIVE MODE (AIMD)        │
             │  (at least one delta > 0)         │
             │                                   │
             └───────────────────────────────────┘
                          │
                          ▼
                  ┌──────────────────┐
                  │ Token bucket     │
                  │ rates updated    │
                  │ per mode         │
                  └──────────────────┘
```

#### 5.9.3 Proactive Mode: Dual Buckets with Equal-Share

**Condition:** No new throttles in the latest poll window:
- `deltaIopsThrottledRequestCount == 0`
- `deltaEgressThrottledBytes == 0`

Use per-poll deltas (not absolute counters), because `GetShareStats` throttle counters may be cumulative:

$$
deltaIopsThrottledRequestCount = \max(currIopsThrottledRequestCount - prevIopsThrottledRequestCount, 0)
$$

$$
deltaEgressThrottledBytes = \max(currEgressThrottledBytes - prevEgressThrottledBytes, 0)
$$

In proactive mode, compute per-worker shares from `GetShareStats` limits:

$$
workerIopsShare = \frac{IopsLimit}{activeWorkerCount}
$$

$$
workerBandwidthShare = \frac{BandwidthLimitMiBps \times 1024 \times 1024}{activeWorkerCount} \text{ (bytes/s)}
$$

`BurstIopsAvailable` can be used as transient headroom, but should not be assumed as steady-state baseline.

Maintain two independent local token buckets (one per resource):

| Bucket | Rate | Capacity | Refill | Purpose |
|--------|------|----------|--------|---------|
| **IOPS** | `workerIopsShare` ops/s | `workerIopsShare × 0.2s` | Continuous (wall-clock) | Operations/second quota |
| **Bandwidth** | `workerBandwidthShare` bytes/s | `workerBandwidthShare × 0.2s` | Continuous (wall-clock) | Data transfer rate quota |

Operation accounting for AzureFiles -> AzureFiles:

| Operation | API | Bandwidth bucket | IOPS bucket |
|-----------|-----|------------------|-------------|
| Data copy | `PutRangeFromURL` | Consume `size` bytes | Consume 1 op |
| Metadata read | `GetProperties` | 0 | Consume 1 op |

Acquisition rule:
- `PutRangeFromURL(size)` must acquire **both** `size` bandwidth tokens **and** `1` IOPS token before proceeding.
- `GetProperties()` must acquire `1` IOPS token only.

Both buckets must grant tokens for the operation to proceed. If either is exhausted, the operation blocks.

Implementation note: dual-token acquisition should be atomic (reserve both or reserve none), or it must roll back the first reservation if the second fails.

#### 5.9.4 Reactive Mode: AIMD with Dual-Bucket Damping

**Condition:** `deltaIopsThrottledRequestCount > 0 OR deltaEgressThrottledBytes > 0`

On first detection of new throttling deltas, switch to reactive AIMD mode. Maintain separate target rates for IOPS and bandwidth, applying multiplicative decrease independently to each resource when its per-poll delta is non-zero:

**On throttle detection (next `GetShareStats` poll):**

```
If deltaIopsThrottledRequestCount > 0:
    targetIops_new = max(targetIops_old × 0.5, MinIops)
    iopsThrottleStreak++
    lastIopsDecreaseAt = now()

If deltaEgressThrottledBytes > 0:
    targetBandwidth_new = max(targetBandwidth_old × 0.5, MinBandwidth)
    bwThrottleStreak++
    lastBwDecreaseAt = now()

// Apply exponential backoff proportional to streak
backoffIops = BaseBackoff × 2^iopsThrottleStreak (capped at MaxBackoff)
backoffBw = BaseBackoff × 2^bwThrottleStreak (capped at MaxBackoff)
backoff = max(backoffIops, backoffBw)
sleep(backoff + jitter)
```

**On additive increase (no throttles for RecoveryQuietPeriod):**

```
if no IOPS throttles for RecoveryQuietPeriod:
    targetIops_new = min(targetIops_old + IopsStep, IopsLimit)
    iopsThrottleStreak = 0

if no Egress throttles for RecoveryQuietPeriod:
    targetBandwidth_new = min(targetBandwidth_old + BandwidthStep, BandwidthLimitMiBps × 1024 × 1024)
    bwThrottleStreak = 0
```

Backoff and multiplicative decrease are triggered by new server throttle evidence (counter deltas), not by local token exhaustion.

#### 5.9.5 Transition Back to Proactive

For cumulative counters, requiring absolute zero can prevent recovery forever. Instead, switch back to proactive when both resources remain quiet for a configurable window:
- `deltaIopsThrottledRequestCount == 0` **AND**
- `deltaEgressThrottledBytes == 0`
- for `QuietForProactiveReturn` (recommended 60s, i.e., two 30s polls)

On transition, reset AIMD state and immediately adopt the latest proactive equal-share rates from `GetShareStats`.

#### 5.9.6 Token Bucket Implementation

Implement dual buckets as separate objects, each maintaining its own rate and capacity:

```go
type DualResourceLimiter struct {
    // IOPS bucket
    iopsBucket   *LocalTokenBucket  // rate = workerIopsShare (ops/s)
    
    // Bandwidth bucket
    bwBucket     *LocalTokenBucket  // rate = workerBandwidthShare (bytes/s)
    
    // Mode state
    mode         string             // "proactive" | "reactive"
    targetIops   int64
    targetBw     int64
    
    // AIMD state (reactive mode only)
    iopsThrottleStreak    int
    bwThrottleStreak      int
    lastIopsDecreaseAt    time.Time
    lastBwDecreaseAt      time.Time
}

// Acquire both IOPS and bandwidth tokens for a data operation
func (d *DualResourceLimiter) AcquirePutRangeTokens(sizeBytes int64) (granted int64, err error) {
    iopsGrant := d.iopsBucket.Take(1)
    if iopsGrant == 0 {
        // IOPS starved — back off
        return 0, ErrIOPSExhausted
    }
    
    bwGrant := d.bwBucket.Take(sizeBytes)
    // Return the minimum of what we can send
    return bwGrant, nil
}

// Acquire only IOPS token for metadata operation
func (d *DualResourceLimiter) AcquireMetadataTokens() error {
    grant := d.iopsBucket.Take(1)
    if grant == 0 {
        return ErrIOPSExhausted
    }
    return nil
}
```

#### 5.9.7 GetShareStats Poll and Mode Update

Every 30 seconds, poll `GetShareStats` and update mode + bucket rates:

```go
func (d *DualResourceLimiter) refreshFromShareStats() {
    stats := d.getShareStats()  // Call Azure Files API

    deltaIops := max64(stats.IopsThrottledRequestCount-d.prevIopsThrottledCount, 0)
    deltaBw := max64(stats.EgressThrottledBytes-d.prevEgressThrottledBytes, 0)

    d.prevIopsThrottledCount = stats.IopsThrottledRequestCount
    d.prevEgressThrottledBytes = stats.EgressThrottledBytes

    if deltaIops > 0 || deltaBw > 0 {
        d.transitionToReactive(stats, deltaIops, deltaBw)
    } else {
        d.transitionToProactiveIfQuiet(stats)
    }
}

func (d *DualResourceLimiter) transitionToProactive(stats ShareStats) {
    d.mu.Lock()
    defer d.mu.Unlock()
    
    d.mode = "proactive"
    d.iopsThrottleStreak = 0
    d.bwThrottleStreak = 0
    
    // Set bucket rates from GetShareStats limits
    workerIopsShare := stats.IopsLimit / int64(d.activeWorkerCount)
    workerBwShare := (stats.BandwidthLimitMiBps * 1024 * 1024) / int64(d.activeWorkerCount)
    
    d.iopsBucket.SetRate(workerIopsShare)
    d.bwBucket.SetRate(workerBwShare)
}

func (d *DualResourceLimiter) transitionToReactive(stats ShareStats, deltaIops, deltaBw int64) {
    d.mu.Lock()
    defer d.mu.Unlock()
    
    d.mode = "reactive"
    
    // Initialize or apply multiplicative decrease for detected throttles
    if deltaIops > 0 {
        if d.targetIops == 0 {
            // First detection: seed from current proactive envelope
            d.targetIops = max64(stats.IopsLimit/2, MinIops)
        } else if time.Since(d.lastIopsDecreaseAt) >= d.computeDebounceWindow(d.iopsThrottleStreak) {
            d.targetIops = max64(d.targetIops/2, MinIops)
            d.iopsThrottleStreak++
            d.lastIopsDecreaseAt = time.Now()
        }
        d.iopsBucket.SetRate(d.targetIops)
    }
    
    if deltaBw > 0 {
        if d.targetBw == 0 {
            // First detection: seed from limit
            d.targetBw = max64((stats.BandwidthLimitMiBps*1024*1024)/2, MinBandwidth)
        } else if time.Since(d.lastBwDecreaseAt) >= d.computeDebounceWindow(d.bwThrottleStreak) {
            d.targetBw = max64(d.targetBw/2, MinBandwidth)
            d.bwThrottleStreak++
            d.lastBwDecreaseAt = time.Now()
        }
        d.bwBucket.SetRate(d.targetBw)
    }
}
```

---

## 6. Mode B: Reactive AIMD (Uncapped Jobs)

### 6.1 Core Principle

No pre-configured bandwidth limit. Workers run at maximum speed until the storage service signals overload via HTTP `429` or `503`. On throttle, the worker applies **AIMD (Additive Increase / Multiplicative Decrease)** congestion control locally.

### 6.2 State Machine

```
        ┌──────────────┐    first 429/503     ┌──────────────────┐
        │  UNCAPPED    │ ───────────────────▶ │  CAPPED (AIMD)   │
        │ targetRate=∞ │                      │ targetRate halved │
        │ no pacing    │                      │ + backoff sleep   │
        └──────────────┘                      └─────────┬────────┘
              ▲                                         │
              │   no throttle for                       │ each success
              │   UncapAfter (5 min)                    │ window: additive
              │                                        │ increase (+step)
              └─────────────────────────────────────────┘
```

### 6.3 Throttle Indicators

| Signal | HTTP Status | Response |
|--------|-------------|----------|
| **Too Many Requests** | 429 | Immediate AIMD decrease + backoff |
| **Service Unavailable / Server Busy** | 503 | Immediate AIMD decrease + backoff |

**Why both 429 and 503:** Storage services return `429` for per-account rate limiting and `503` for partition-level overload or maintenance. Both indicate the worker should reduce load.

### 6.4 AIMD Algorithm

**On throttle (429/503):**

```
// Debounce: treat all 429s within one backoff window as a single congestion event.
// Prevents N concurrent in-flight requests each halving independently.
debounceWindow = min(BaseBackoff × 2^throttleStreak, MaxBackoff)
if time.Since(lastDecreaseAt) >= debounceWindow:
    throttleStreak++
    lastDecreaseAt = now()
    targetRate_new = max(targetRate_old × 0.5, MinRate)
    pacer.SetRate(targetRate_new)

backoff = min(BaseBackoff × 2^throttleStreak, MaxBackoff)
if Retry-After header present: backoff = Retry-After value
sleep(backoff + jitter)
```

> **Why debounce?** With N concurrent in-flight requests, all returning 429 within the same
> RTT window, the naive implementation applies N independent halvings — collapsing `targetRate`
> to `MinRate` immediately and spiking `throttleStreak` to N. The debounce window ensures
> **one multiplicative decrease per congestion event** (per backoff cycle), matching TCP AIMD
> semantics. Goroutines that lose the debounce race still sleep their backoff but do not halve.

**On sustained success (no throttle for RecoveryQuietPeriod):**

```
targetRate_new = targetRate_old + AIMDStep    // unbounded; grows until next 429 or uncap
if no throttle for UncapAfter → return to UNCAPPED
```

### 6.5 Local Token Bucket Pacer

When capped, a local token bucket enforces `targetRate`:

```
Token bucket parameters:
    rate     = targetRate (bytes/s)
    capacity = targetRate × 1.0s (burst window)
    refill   = continuous (wall-clock based)
```

When UNCAPPED, the token bucket is disabled (returns requested amount immediately).

### 6.6 Cosmos DB Interaction for Uncapped Jobs

Minimal — Cosmos DB is used only for:

| Purpose | Mechanism |
|---------|-----------|
| Worker count (observability) | Heartbeat docs + COUNT query |
| Throttle propagation | Write throttle-event doc on 429/503 |
| Usage metrics | Periodic usage-metric doc for dashboards |

**No Cosmos DB is on the critical path for Mode B enforcement.** The AIMD loop is entirely local.

### 6.7 No Cross-Worker Coordination

Each worker reacts independently to throttle signals. This is correct because:
- Storage throttling affects all workers hitting the same endpoint — 429s arrive nearly simultaneously
- Independent AIMD converges to fair sharing (TCP-proven property)
- Cross-worker coordination adds latency without benefit for reactive control

---

## 7. Active Worker Tracking via Cosmos DB

### 7.1 Heartbeat Document Design

Each worker maintains a heartbeat document per job it's actively working on:

```json
{
    "id": "worker-{workerID}",
    "jobId": "{jobID}",
    "type": "worker-heartbeat",
    "workerId": "{workerID}",
    "lastHeartbeat": "2026-05-06T22:30:00.200Z",
    "ttl": 30
}
```

- **Partition key:** `jobId` — ensures worker count queries are single-partition
- **TTL:** 30 seconds — auto-deleted by Cosmos DB if worker crashes without deregistering
- **Upsert on every poll:** Refreshes `lastHeartbeat` and resets the TTL countdown

### 7.2 Worker Registration

```go
func (w *Worker) registerForJob(jobID string) error {
    doc := WorkerHeartbeat{
        ID:            "worker-" + w.workerID,
        JobID:         jobID,
        Type:          "worker-heartbeat",
        WorkerID:      w.workerID,
        LastHeartbeat: time.Now().UTC(),
        TTL:           WorkerTTLSeconds, // 30
    }
    // Upsert — creates or replaces
    _, err := w.cosmosContainer.UpsertItem(ctx, jobID, doc, nil)
    return err
}
```

### 7.3 Worker Deregistration (Graceful)

```go
func (w *Worker) deregisterFromJob(jobID string) error {
    docID := "worker-" + w.workerID
    _, err := w.cosmosContainer.DeleteItem(ctx, jobID, docID, nil)
    return err
}
```

### 7.4 Query Active Worker Count

```sql
SELECT VALUE COUNT(1)
FROM c
WHERE c.jobId = @jobId
  AND c.type = "worker-heartbeat"
```

This is a single-partition query (partitioned by `jobId`), costing ~3 RU.

**Note:** Cosmos DB TTL cleanup runs asynchronously (typically within seconds of expiry but not guaranteed to be instant). The count may include a recently-crashed worker for a brief period. This is acceptable — it means workers use a slightly larger denominator (smaller share), which is conservative (under-utilization, not overshoot).

### 7.5 Heartbeat Refresh

On every poll cycle (200ms–1s), the worker upserts its heartbeat doc:

```go
func (p *ProactiveRateLimiter) refreshHeartbeat() {
    doc := WorkerHeartbeat{
        ID:            "worker-" + p.workerID,
        JobID:         p.jobID,
        Type:          "worker-heartbeat",
        WorkerID:      p.workerID,
        LastHeartbeat: time.Now().UTC(),
        TTL:           WorkerTTLSeconds,
    }
    p.cosmosContainer.UpsertItem(ctx, p.jobID, doc, nil)
}
```

### 7.6 TTL vs Staleness Trade-off

| TTL (seconds) | Crash detection delay | Risk |
|---------------|----------------------|------|
| 10 | 10s under-utilization after crash | Worker misses 1-2 heartbeats during GC → prematurely pruned |
| **30** | **30s under-utilization after crash** | **Safe — worker must miss 30+ heartbeats** |
| 60 | 60s under-utilization | Too slow for dynamic scaling |

**Recommendation:** TTL = 30 seconds. A worker polling every 500ms would need to fail 60 consecutive heartbeats before being pruned. This is robust against transient network issues.

---

## 8. Throttle Detection & Signal Processing

### 8.1 Throttle Indicators

```go
func IsThrottleResponse(statusCode int) bool {
    return statusCode == 429 || statusCode == 503
}
```

### 8.2 Throttle Propagation to Control Plane

Both modes write throttle events to Cosmos DB for JobPlanner/Orchestrator visibility:

```go
func (rl *baseRateLimiter) publishThrottleEvent(statusCode int, retryAfterMs int64) {
    doc := ThrottleEvent{
        ID:           fmt.Sprintf("throttle-%s-%d", rl.workerID, time.Now().UnixMilli()),
        JobID:        rl.jobID,
        Type:         "throttle-event",
        WorkerID:     rl.workerID,
        StatusCode:   statusCode,
        RetryAfterMs: retryAfterMs,
        Timestamp:    time.Now().UTC(),
        TTL:          60, // auto-expires after 1 minute
    }
    rl.cosmosContainer.UpsertItem(ctx, rl.jobID, doc, nil)
}
```

### 8.3 JobPlanner Behavior

JobPlanner queries throttle events before dispatching new JPs:

```sql
SELECT VALUE COUNT(1)
FROM c
WHERE c.jobId = @jobId
  AND c.type = "throttle-event"
  AND c.timestamp > @recentWindow
```

| Condition | Action |
|-----------|--------|
| No throttle events | Dispatch JPs normally |
| 1–2 events in last 30s | Dispatch with caution (reduce batch) |
| 3+ events in last 30s | Pause new JP dispatch for `retryAfterMs` |

### 8.4 Orchestrator Escalation

| Pattern | Orchestrator response |
|---------|----------------------|
| Single short event | Observe only |
| Repeated events (>3 in 30s) | Mark job `THROTTLED`, reduce concurrency |
| Sustained (>1 min continuous) | Pause job; optionally introduce a `jobRate` (convert Mode B → Mode A) |

### 8.5 Scenario-Specific Throttle Semantics

| Scenario | Throttle detection | Mode transition | Recovery |
|----------|-------------------|-----------------|----------|
| Blob -> Blob (uncapped) | HTTP 429/503 from API | `UNCAPPED -> CAPPED` in Mode B | After 5 min quiet → `CAPPED -> UNCAPPED` |
| Blob -> Blob (capped) | HTTP 429/503 from API | `PROACTIVE -> REACTIVE` only after sustained throttling window | Sustained quiet window → `REACTIVE -> PROACTIVE` |
| **AzureFiles -> AzureFiles** | **GetShareStats counters sampled every 30s (delta-based)** | **PROACTIVE ↔ REACTIVE based on per-poll deltas** | **No deltas for quiet window → back to PROACTIVE** |

#### 8.5.1 AzureFiles Mode-Switching Details

For AzureFiles -> AzureFiles copy, mode is determined by per-poll deltas from `GetShareStats` counters:

| Condition | Mode | Action |
|-----------|------|--------|
| `deltaIopsThrottledRequestCount == 0 AND deltaEgressThrottledBytes == 0` | **PROACTIVE** | Use `IopsLimit` and `BandwidthLimitMiBps` to set bucket rates; equal-share across active workers |
| `deltaIopsThrottledRequestCount > 0` | **REACTIVE** | Apply AIMD to IOPS bucket; exponential backoff |
| `deltaEgressThrottledBytes > 0` | **REACTIVE** | Apply AIMD to Bandwidth bucket; exponential backoff |
| Either per-poll delta > 0 | **REACTIVE** | Apply AIMD independently to each throttled resource; backoff = max(iopsBackoff, bwBackoff) |

**Key difference from HTTP-based detection:** No `Retry-After` header parsing. Instead, throttle state is derived from `GetShareStats` counter deltas, enabling:
- Independent per-resource AIMD (IOPS and bandwidth can converge at different rates)
- Deterministic transition back to proactive after a quiet window with no deltas
- No false positives from transient 429s (server-level status is authoritative)

#### 8.5.2 Non-AzureFiles Sustained-Throttling Detector (Blob -> Blob Capped)

For Blob -> Blob jobs running in proactive mode (`jobRate > 0`), a few momentary 429/503 responses must not trigger mode switch. Transition to reactive mode happens only if throttling is sustained.

Detection policy (worker-local, sliding window):
- Track only throttle responses (`429` or `503`) in `SustainedThrottleWindow`.
- Compute:
  - `throttleCountWindow` = number of 429/503 in window
  - `throttleRatioWindow` = throttled responses / total responses in window
- Enter reactive mode only when both are true:
  - `throttleCountWindow >= SustainedThrottleMinEvents`
  - `throttleRatioWindow >= SustainedThrottleMinRatio`

Recommended defaults:
- `SustainedThrottleWindow = 30s`
- `SustainedThrottleMinEvents = 12`
- `SustainedThrottleMinRatio = 0.20`

Transition logic:

```go
if mode == "PROACTIVE" {
    if isThrottle(statusCode) {
        sustainedDetector.AddThrottle(now)
    } else {
        sustainedDetector.AddSuccess(now)
    }

    if sustainedDetector.IsSustainedThrottling() {
        mode = "REACTIVE"
        reactiveLimiter.SeedFromCurrentShare(currentShare)
    }
}

if mode == "REACTIVE" {
    // Existing AIMD (halve on throttle, additive increase on quiet)
    // plus transition back to PROACTIVE after sustained quiet.
    if sustainedDetector.IsSustainedQuiet() {
        mode = "PROACTIVE"
        pacer.SetRate(currentShare)
    }
}
```

This gate prevents oscillation caused by brief server-side bursts while still allowing robust fallback to reactive control when throttling is persistent.

---

## 9. Worker Design — Unified Rate Limiter

### 9.1 Unified Interface

Both modes implement the same interface, making JP goroutines mode-agnostic:

```go
type RateLimiter interface {
    // AcquireTokens returns the number of bytes the caller may transfer.
    // Blocks if necessary (Mode A: until tokens available, Mode B: until pacer allows).
    AcquireTokens(requested int64) int64

    // HandleResponse feeds transfer outcome back to the rate limiter.
    HandleResponse(statusCode int, responseTime time.Duration, retryAfterSec float64)

    // AddRef / Release for reference counting
    AddRef()
    Release()

    // Stop gracefully shuts down polling and deregisters.
    Stop()
}
```

### 9.2 Mode Selection at Reservoir Creation

```go
func (w *Worker) getOrCreateRateLimiter(jobID string, jobRate int64) RateLimiter {
    w.mu.Lock()
    defer w.mu.Unlock()

    rl, exists := w.rateLimiters[jobID]
    if exists {
        rl.AddRef()
        return rl
    }

    if jobRate > 0 {
        // Mode A: Proactive Equal-Share
        rl = NewProactiveRateLimiter(jobID, w.workerID, jobRate, w.cosmosClient)
    } else {
        // Mode B: Reactive AIMD
        rl = NewReactiveRateLimiter(jobID, w.workerID, w.cosmosClient)
    }

    w.rateLimiters[jobID] = rl
    return rl
}
```

### 9.3 JP Processing Loop (Mode-Agnostic)

```go
func (w *Worker) processJobPlan(jp JobPlan, rl RateLimiter) {
    for _, file := range jp.Files {
        for offset := int64(0); offset < file.Size; {
            size := min64(blockSize, file.Size-offset)

            // Acquire tokens (may block for pacing)
            granted := rl.AcquireTokens(size)
            if granted == 0 {
                time.Sleep(10 * time.Millisecond)
                continue
            }

            // Transfer block
            statusCode, respTime, retryAfter := transferBlock(file, offset, granted)

            // Feed result to rate limiter
            rl.HandleResponse(statusCode, respTime, retryAfter)

            if !IsThrottleResponse(statusCode) {
                offset += granted
            }
            // On throttle: HandleResponse already applied backoff/sleep;
            // retry same offset on next iteration
        }
    }
}
```

---

## 10. Multi-Worker Scaling Walkthrough

### 10.1 Mode A: 1→4 Workers, Equal-Share (No Redis)

```
Config: jobRate = 100 MB/s, pollInterval = 500ms, workerTTL = 30s

═══ Phase 1 — Solo Worker ═══

t=0.0s: W1 online
    • Writes heartbeat doc to Cosmos DB
    • Queries worker count → 1
    • workerShare = 100/1 = 100 MB/s
    • Local token bucket rate = 100 MB/s
    • W1 transfers at full job rate

═══ Phase 2 — W2 Joins ═══

t=1.0s: W2 online
    • Writes heartbeat → count query returns 2
    • W2 share = 50 MB/s (starts correctly immediately)
    • W1 hasn't polled yet → still at 100 MB/s

t=1.5s: W1 polls → count=2
    • W1 share drops to 50 MB/s instantly
    • Token bucket rate adjusted — no ramp needed
    ★ Total briefly exceeded during [t=1.0, t=1.5]: 100+50=150 MB/s for 0.5s
    ★ This is the accepted trade-off of no centralized bucket

═══ Phase 3 — W3 Joins ═══

t=2.0s: W3 online → count=3
    • All workers converge to 33.3 MB/s within one poll interval
    • Total = 3 × 33.3 = 100 MB/s ✓

═══ Phase 4 — W4 Joins, Steady State ═══

t=3.0s: W4 online → count=4
    • Each share = 25 MB/s
    • Total = 4 × 25 = 100 MB/s ✓

═══ Phase 5 — W2 Crashes ═══

t=5.0s: W2 process terminates (no graceful deregistration)
    • W2's heartbeat doc TTL = 30s → Cosmos DB deletes at ~t=35s
    • t=5–35s: W1, W3, W4 see count=4, share=25 MB/s
    • Total = 3 × 25 = 75 MB/s (under-utilized for ~30s)
    • t=35s: W2 doc expired → count=3, shares become 33.3 MB/s
    • Self-heals without intervention
```

### 10.2 Mode B: Reactive AIMD Scenario

```
Config: uncapped job, 3 workers, storage account limit ~200 MB/s

═══ Phase 1 — UNCAPPED ═══

t=0–5s: All workers running at max speed
    • Combined throughput: ~250 MB/s (exceeds storage capacity)
    • Storage returns 429 to all workers nearly simultaneously

═══ Phase 2 — FIRST THROTTLE ═══

t=5s: All workers enter CAPPED state
    • Each worker observed ~83 MB/s before throttle
    • targetRate = 83/2 ≈ 41 MB/s per worker
    • Combined: 3 × 41 = 123 MB/s (within storage capacity)
    • Backoff sleep: BaseBackoff × 2^1 = 500ms with jitter

═══ Phase 3 — ADDITIVE INCREASE ═══

t=6s onward: No 429s → increase +1 MB/s every RecoveryQuietPeriod
    • t=6s: 42 MB/s each → 126 total
    • t=7s: 43 MB/s each → 129 total
    • ...continues growing...
    • t=60s: ~96 MB/s each → 288 total → hits 429 again
    
═══ Phase 4 — OSCILLATION NEAR CAPACITY ═══

    • On 429: halve → ~48 MB/s each → 144 total
    • Recover → grow → oscillate near 200 MB/s
    • OR: after 5 min without throttle → return to UNCAPPED

═══ Phase 5 — UNCAP (optional) ═══

    • If UncapAfter (5 min) passes with no throttles → UNCAPPED
    • Workers blast again → cycle repeats if storage limit unchanged
    • If storage scaled up → workers stay uncapped at higher throughput
```

---

## 11. Cosmos DB Failure Mitigation

### 11.1 Mode A Failure Strategy

When Cosmos DB is unreachable, Mode A workers cannot query the active worker count. Fallback:

```go
func (p *ProactiveRateLimiter) onCosmosFailure() {
    // Use pessimistic cap: assume maximum possible workers
    p.mu.Lock()
    p.currentShare = p.jobRate / int64(MaxWorkersPerJob)
    p.pacer.SetRate(p.currentShare)
    p.mu.Unlock()
}
```

| Strategy | Behavior | Throughput |
|----------|----------|-----------|
| **Pessimistic** (default) | `jobRate / MaxWorkersPerJob` = 2 MB/s per worker | Safe — never exceeds jobRate |
| **Last-known** | Keep last-seen share until Cosmos recovers | Risk of overshoot if workers have left |
| **Fail-closed** | Stop all transfers | For strict compliance scenarios |

**Recommended:** Pessimistic for first 30s, then last-known with safety margin (`lastCount × 2`).

### 11.2 Mode B Failure Strategy

Cosmos DB failure has **zero impact** on Mode B enforcement:
- AIMD is entirely local — no external dependency
- Only throttle-event propagation and metrics reporting stop
- Workers continue in their current AIMD state
- JobPlanner loses visibility but workers still back off locally

### 11.3 Configuration

```go
type CosmosFailureConfig struct {
    Mode           string        // "pessimistic" | "last-known" | "fail-closed"
    MaxRetries     int           // 3
    RetryBackoff   time.Duration // 200ms exponential base
    FallbackWindow time.Duration // 30s before switching to last-known
}
```

---

## 12. Edge Cases & Starvation Fixes

### 12.1 Mode A: Stale Worker Count (Crash Without Deregistration)

**Problem:** Worker crashes. Its heartbeat doc takes up to `TTL=30s` to expire. Remaining workers under-utilize.

**Mitigation:** Acceptable trade-off. Under-utilization is conservative (safe). The 30s TTL is chosen to balance:
- Too short (5s): Normal GC pauses or network blips cause false deregistration
- Too long (60s): Extended under-utilization after crash

**Alternative:** Workers can also check heartbeat timestamps themselves and only count docs with `lastHeartbeat > now - 2×pollInterval`. This provides faster detection than TTL alone:

```sql
SELECT VALUE COUNT(1)
FROM c
WHERE c.jobId = @jobId
  AND c.type = "worker-heartbeat"
  AND c.lastHeartbeat > @cutoff
```

Where `@cutoff = now - 10s` (configurable). This catches stale workers faster than TTL expiry.

### 12.2 Mode A: Overshoot During Worker Join

**Problem:** New worker starts using its share before existing workers have reduced theirs.

**Analysis:** For one poll interval, total allocation = `N × (rate/N-1) + rate/N > rate`. Overshoot magnitude = `rate / (N × (N-1))`.

| Workers (N) | Overshoot per interval | At 100 MB/s, 500ms poll |
|---|---|---|
| 2→3 | 16.7% | 8.3 MB |
| 4→5 | 5% | 2.5 MB |
| 10→11 | 0.9% | 0.45 MB |

**Conclusion:** Acceptable. Brief, bounded, and self-correcting within one poll interval.

### 12.3 Mode A: Idle Worker Hogging Share

**Problem:** Worker registered but has no active JPs — its share is wasted.

**Fix:** Workers deregister from Cosmos DB when their last JP for a job completes (after `CleanupDelay=30s`):

```go
func (w *Worker) releaseRateLimiter(jobID string) {
    w.mu.Lock()
    rl := w.rateLimiters[jobID]
    w.mu.Unlock()

    rl.Release() // decrement refCount

    if rl.RefCount() == 0 {
        time.AfterFunc(CleanupDelay, func() {
            if rl.RefCount() == 0 {
                rl.Stop()  // deregisters from Cosmos DB
                w.mu.Lock()
                delete(w.rateLimiters, jobID)
                w.mu.Unlock()
            }
        })
    }
}
```

### 12.4 Mode B: Thundering Herd on Recovery

**Problem:** All workers reach `UncapAfter` simultaneously → all go UNCAPPED → all blast → all throttled.

**Fix:** Add jitter to uncap timing:

```go
uncapTime := UncapAfter + randomDuration(0, UncapJitter)
// UncapJitter = 30s → workers uncap over a 30s window, not simultaneously
```

### 12.5 Mode B: Asymmetric Throttling

**Problem:** One worker sees 429s (hitting a hot partition) while others don't.

**Behavior:** Correct by design. The throttled worker backs off locally. Others continue. If the condition is widespread, all workers will see 429s and converge independently. Throttle events propagated to Cosmos DB give JobPlanner visibility.

### 12.6 Job Completion Race

**Orchestrator** updates job-config doc: `status = "COMPLETED"`. Workers check job status on each poll and stop if completed:

```go
func (p *ProactiveRateLimiter) pollLoop() {
    for {
        select {
        case <-ticker.C:
            jobConfig := p.readJobConfig()
            if jobConfig.Status == "COMPLETED" || jobConfig.Status == "PAUSED" {
                p.Stop()
                return
            }
            p.refreshHeartbeat()
            count := p.queryActiveWorkerCount()
            p.updateShare(count)
        case <-p.stopCh:
            return
        }
    }
}
```

### 12.7 Storage Throttling on Mode A (Capped) Jobs

**Problem:** Storage throttles at 80 MB/s but job is configured at 100 MB/s.

**Response:**
1. Workers publish throttle events to Cosmos DB
2. Orchestrator observes sustained throttling
3. Orchestrator may: reduce `jobRate` (update job-config doc) or reduce `maxWorkersPerJob`
4. Workers pick up the new `jobRate` on next poll and adjust shares

For Blob -> Blob capped jobs, a worker should not leave proactive mode on momentary throttles. It transitions to reactive only when the sustained-throttling detector is tripped (window + count + ratio threshold).

For AzureFiles -> AzureFiles jobs, the same loop applies with one extension: workers also refresh `GetShareStats`-derived total bandwidth and total IOPS envelopes and recompute per-worker dual shares.

**Optional belt-and-suspenders:** Mode A workers can also apply a secondary local AIMD on top of the equal share when they personally receive 429s:

```go
func (p *ProactiveRateLimiter) HandleResponse(statusCode int, responseTime time.Duration, retryAfterSec float64) {
    if IsThrottleResponse(statusCode) {
        p.publishThrottleEvent(statusCode, int64(retryAfterSec*1000))

        // Secondary local backoff — reduce effective rate below equal share
        p.mu.Lock()
        p.localThrottleFactor = max(p.localThrottleFactor * 0.5, 0.1)
        effectiveRate := int64(float64(p.currentShare) * p.localThrottleFactor)
        p.pacer.SetRate(effectiveRate)
        p.mu.Unlock()

        time.Sleep(computeBackoff(retryAfterSec))
    } else if p.localThrottleFactor < 1.0 {
        // Additive recovery of the throttle factor
        p.mu.Lock()
        p.localThrottleFactor = min(p.localThrottleFactor + 0.1, 1.0)
        effectiveRate := int64(float64(p.currentShare) * p.localThrottleFactor)
        p.pacer.SetRate(effectiveRate)
        p.mu.Unlock()
    }
}
```

AzureFiles -> AzureFiles mode switching is controlled by `GetShareStats` counters. Blob -> Blob capped mode switching is controlled by sustained HTTP-throttling detection.

---

## 13. Metrics & Observability

### 13.1 Per-Job Metrics

| Metric | Source | Mode A | Mode B |
|--------|--------|--------|--------|
| `bw.job.actual_bps` | Cosmos usage-metric docs | ✓ | ✓ |
| `bw.job.actual_pct` | `bps / jobRate × 100` | ✓ | n/a |
| `bw.job.active_workers` | COUNT query on heartbeat docs | ✓ | ✓ |
| `bw.job.worker_share` | `jobRate / activeWorkers` | ✓ | n/a |
| `bw.job.throttle_events` | COUNT query on throttle-event docs | ✓ | ✓ |
| `bw.job.status` | Job-config doc | ✓ | ✓ |

### 13.2 Per-Worker Metrics (Local, emitted to telemetry)

| Metric | Source | Mode A | Mode B |
|--------|--------|--------|--------|
| `bw.worker.effective_rate` | local gauge | ✓ | ✓ (when capped) |
| `bw.worker.tokens_available` | local token bucket | ✓ | ✓ (when capped) |
| `bw.worker.consumed_bps` | local counter | ✓ | ✓ |
| `bw.worker.aimd_state` | "uncapped" / "capped" | n/a | ✓ |
| `bw.worker.target_rate` | local gauge | n/a | ✓ |
| `bw.worker.throttle_streak` | local counter | ✓ | ✓ |
| `bw.worker.backoff_ms` | local gauge | ✓ | ✓ |
| `bw.worker.poll_latency_ms` | Cosmos query RTT | ✓ | ✓ |
| `bw.worker.active_count_seen` | from poll | ✓ | ✓ |

### 13.3 System Metrics

| Metric | Source |
|--------|--------|
| `bw.cosmos.ru_consumed` | Cosmos DB diagnostics |
| `bw.cosmos.query_latency_ms` | Client-side |
| `bw.cosmos.failure_rate` | Client errors |
| `bw.active_jobs` | Job-config doc count where status=ACTIVE |
| `bw.total_active_workers` | Sum of worker counts across jobs |

---

## 14. Configuration Reference

### 14.1 Orchestrator Config

| Parameter | Description |
|-----------|-------------|
| `JobRate` | Per job. `> 0` → Mode A. `0` or absent → Mode B |
| `MaxWorkersPerJob` | Hard cap for scaling + pessimistic fallback |

### 14.2 Worker / Shared Config

| Parameter | Default | Description |
|-----------|---------|-------------|
| `PollInterval` | 500ms | How often worker polls Cosmos DB for count |
| `MaxGrantCap` | 4 MB | Max per AcquireTokens call (intra-worker fairness) |
| `MaxConcurrentJPs` | 4 | JP slots per worker |
| `CleanupDelay` | 30s | Delay before deregistering idle rate limiter |
| `WorkerTTLSeconds` | 30 | Cosmos DB TTL on heartbeat docs |
| `BurstWindow` | 0.2s | Local token bucket capacity = share × burstWindow |

### 14.3 Mode A Config (Proactive)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `MaxWorkersPerJob` | 50 | Used for pessimistic fallback on Cosmos failure |
| `ActiveCountCutoff` | 10s | Only count heartbeats newer than this (faster stale detection) |
| `SustainedThrottleWindow` | 30s | Blob->Blob capped only. Sliding window for evaluating sustained 429/503 |
| `SustainedThrottleMinEvents` | 12 | Blob->Blob capped only. Minimum throttled responses in window before mode switch |
| `SustainedThrottleMinRatio` | 0.20 | Blob->Blob capped only. Minimum throttled/total response ratio for proactive->reactive transition |
| `SustainedQuietWindow` | 60s | Blob->Blob capped only. Required quiet duration to transition reactive->proactive |

### 14.3.1 AzureFiles-AzureFiles Specific Config

| Parameter | Default | Description |
|-----------|---------|-------------|
| `GetShareStatsPollInterval` | 30s | How frequently to call `GetShareStats` API to refresh limits and detect throttles |
| `BurstWindowBandwidth` | 0.2s | Local token bucket burst capacity for bandwidth = `rate × window` |
| `BurstWindowIops` | 0.2s | Local token bucket burst capacity for IOPS = `rate × window` |
| `EnableDualResourceControl` | true | Enable separate IOPS and Bandwidth token buckets |
| `MinIops` | 100 ops/s | AIMD floor for IOPS in reactive mode |
| `MinBandwidth` | 1 MB/s | AIMD floor for bandwidth in reactive mode |
| `IopsStep` | 100 ops/s | Additive increase step for IOPS per `RecoveryQuietPeriod` |
| `BandwidthStep` | 1 MB/s | Additive increase step for bandwidth per `RecoveryQuietPeriod` |
| `RecoveryQuietPeriodAzureFiles` | 1s | Quiet time without throttle counters before additive increase |
| `BaseBackoffAzureFiles` | 250ms | Exponential backoff base for dual-bucket AIMD |
| `MaxBackoffAzureFiles` | 10s | Exponential backoff ceiling for dual-bucket AIMD |

### 14.4 Mode B Config (Reactive AIMD)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `DefaultUnmeteredRate` | 100 MB/s | Seed for first throttle if no throughput observed |
| `MinRate` | 1 MB/s | AIMD floor (multiplicative decrease never goes below this) |
| `AIMDStep` | 1 MB/s | Additive increase per success window |
| `RecoveryQuietPeriod` | 1s | Quiet time before each additive increase |
| `UncapAfter` | 5 min | Quiet time to release cap entirely |
| `UncapJitter` | 30s | Random jitter on uncap to prevent thundering herd |
| `BaseBackoff` | 250 ms | Exponential backoff base |
| `MaxBackoff` | 10s | Exponential backoff ceiling |
| `ThrottleDebounceWindow` | `BaseBackoff × 2^streak` (capped at `MaxBackoff`) | Suppresses redundant halvings from concurrent in-flight 429s within the same congestion event |

### 14.5 Cosmos DB Failure Config

| Parameter | Default | Description |
|-----------|---------|-------------|
| `FailureMode` | "pessimistic" | "pessimistic" / "last-known" / "fail-closed" |
| `MaxRetries` | 3 | Retry attempts before fallback |
| `RetryBackoff` | 200ms | Exponential backoff base for Cosmos retries |

### 14.6 Scaling Config

| Parameter | Default | Description |
|-----------|---------|-------------|
| `BacklogPerWorker` | 6 | KEDA scaling ratio |
| `MaxWorkersPerJob` | 50 | Hard cap |
| `MinWorkersPerJob` | 1 | Floor while job is active |

---

## 15. Go Implementation

### 15.1 Constants & Interfaces

```go
package bandwidth

import (
    "math"
    "sync"
    "time"
)

const (
    PollInterval       = 500 * time.Millisecond
    PollsPerSecond     = 2
    MaxGrantCap        = 4 * 1024 * 1024  // 4MB
    MaxConcurrentJPs   = 4
    CleanupDelay       = 30 * time.Second
    WorkerTTLSeconds   = 30
    BurstWindow        = 200 * time.Millisecond
    MaxWorkersPerJob   = 50

    // Mode B defaults
    DefaultUnmeteredRate = 100 * 1024 * 1024  // 100 MB/s
    MinRate              = 1 * 1024 * 1024     // 1 MB/s
    AIMDStep             = 1 * 1024 * 1024     // 1 MB/s
    RecoveryQuietPeriod  = 1 * time.Second
    UncapAfter           = 5 * time.Minute
    UncapJitter          = 30 * time.Second
    BaseBackoff          = 250 * time.Millisecond
    MaxBackoff           = 10 * time.Second
)

type RateLimiter interface {
    AcquireTokens(requested int64) int64
    HandleResponse(statusCode int, responseTime time.Duration, retryAfterSec float64)
    AddRef()
    Release()
    RefCount() int
    Stop()
}
```

### 15.2 Local Token Bucket (Shared by Both Modes)

```go
type LocalTokenBucket struct {
    mu       sync.Mutex
    tokens   float64
    rate     float64   // bytes/s
    capacity float64   // burst cap
    lastTime time.Time
}

func NewLocalTokenBucket(rate int64, burstWindow time.Duration) *LocalTokenBucket {
    return &LocalTokenBucket{
        tokens:   float64(rate) * burstWindow.Seconds(),
        rate:     float64(rate),
        capacity: float64(rate) * burstWindow.Seconds(),
        lastTime: time.Now(),
    }
}

func (tb *LocalTokenBucket) Take(requested int64) int64 {
    tb.mu.Lock()
    defer tb.mu.Unlock()

    now := time.Now()
    elapsed := now.Sub(tb.lastTime).Seconds()
    tb.lastTime = now

    // Refill
    tb.tokens = math.Min(tb.tokens+tb.rate*elapsed, tb.capacity)

    // Grant
    grant := math.Min(float64(requested), tb.tokens)
    if grant < 0 {
        grant = 0
    }
    tb.tokens -= grant
    return int64(grant)
}

func (tb *LocalTokenBucket) SetRate(rate int64) {
    tb.mu.Lock()
    tb.rate = float64(rate)
    tb.capacity = float64(rate) * BurstWindow.Seconds()
    tb.mu.Unlock()
}

func NewUnlimitedTokenBucket() *LocalTokenBucket {
    return &LocalTokenBucket{
        tokens:   math.MaxFloat64,
        rate:     math.MaxFloat64,
        capacity: math.MaxFloat64,
        lastTime: time.Now(),
    }
}
```

### 15.3 Mode A — ProactiveRateLimiter

```go
type ProactiveRateLimiter struct {
    mu sync.Mutex

    // State
    jobID              string
    workerID           string
    jobRate            int64
    activeWorkerCount  int64
    currentShare       int64
    localThrottleFactor float64  // 1.0 = no storage throttle, <1 = backed off

    // Local pacing
    pacer *LocalTokenBucket

    // Cosmos DB
    cosmosContainer CosmosContainer

    // Lifecycle
    refCount int
    stopCh   chan struct{}
}

func NewProactiveRateLimiter(jobID, workerID string, jobRate int64, cosmos CosmosContainer) *ProactiveRateLimiter {
    p := &ProactiveRateLimiter{
        jobID:               jobID,
        workerID:            workerID,
        jobRate:             jobRate,
        activeWorkerCount:   1,
        currentShare:        jobRate,
        localThrottleFactor: 1.0,
        pacer:               NewLocalTokenBucket(jobRate, BurstWindow),
        cosmosContainer:     cosmos,
        refCount:            1,
        stopCh:              make(chan struct{}),
    }

    // Initial registration + poll
    p.refreshHeartbeat()
    count := p.queryActiveWorkerCount()
    p.updateShareLocked(count)

    go p.pollLoop()
    return p
}

func (p *ProactiveRateLimiter) AcquireTokens(requested int64) int64 {
    grant := p.pacer.Take(min64(requested, MaxGrantCap))
    return grant
}

func (p *ProactiveRateLimiter) HandleResponse(statusCode int, responseTime time.Duration, retryAfterSec float64) {
    if IsThrottleResponse(statusCode) {
        p.publishThrottleEvent(statusCode, int64(retryAfterSec*1000))

        // Secondary local throttle — reduce below equal share
        p.mu.Lock()
        p.localThrottleFactor = math.Max(p.localThrottleFactor*0.5, 0.1)
        effectiveRate := int64(float64(p.currentShare) * p.localThrottleFactor)
        p.pacer.SetRate(effectiveRate)
        p.mu.Unlock()

        backoff := computeBackoff(1, retryAfterSec)
        time.Sleep(backoff)
    } else if p.localThrottleFactor < 1.0 {
        // Recover throttle factor
        p.mu.Lock()
        p.localThrottleFactor = math.Min(p.localThrottleFactor+0.1, 1.0)
        effectiveRate := int64(float64(p.currentShare) * p.localThrottleFactor)
        p.pacer.SetRate(effectiveRate)
        p.mu.Unlock()
    }
}

func (p *ProactiveRateLimiter) pollLoop() {
    ticker := time.NewTicker(PollInterval)
    defer ticker.Stop()

    for {
        select {
        case <-ticker.C:
            // Check job status
            config, err := p.readJobConfig()
            if err == nil && (config.Status == "COMPLETED" || config.Status == "PAUSED") {
                p.Stop()
                return
            }

            // Heartbeat + count
            p.refreshHeartbeat()
            count := p.queryActiveWorkerCount()
            p.updateShare(count)

        case <-p.stopCh:
            p.deregister()
            return
        }
    }
}

func (p *ProactiveRateLimiter) updateShare(count int64) {
    p.mu.Lock()
    defer p.mu.Unlock()
    p.updateShareLocked(count)
}

func (p *ProactiveRateLimiter) updateShareLocked(count int64) {
    if count < 1 {
        count = 1
    }
    p.activeWorkerCount = count
    p.currentShare = p.jobRate / count
    effectiveRate := int64(float64(p.currentShare) * p.localThrottleFactor)
    p.pacer.SetRate(effectiveRate)
}

func (p *ProactiveRateLimiter) onCosmosFailure() {
    // Pessimistic fallback
    p.mu.Lock()
    p.currentShare = p.jobRate / int64(MaxWorkersPerJob)
    p.pacer.SetRate(p.currentShare)
    p.mu.Unlock()
}

func (p *ProactiveRateLimiter) Stop() {
    close(p.stopCh)
}

func (p *ProactiveRateLimiter) AddRef()      { p.mu.Lock(); p.refCount++; p.mu.Unlock() }
func (p *ProactiveRateLimiter) Release()     { p.mu.Lock(); p.refCount--; p.mu.Unlock() }
func (p *ProactiveRateLimiter) RefCount() int { p.mu.Lock(); defer p.mu.Unlock(); return p.refCount }
```

### 15.4 Mode B — ReactiveRateLimiter

```go
type ReactiveRateLimiter struct {
    mu sync.Mutex

    // AIMD state
    capped          bool
    targetRate      int64
    throttleStreak  int
    lastThrottleAt  time.Time
    lastDecreaseAt  time.Time  // debounce: timestamp of last multiplicative decrease
    lastObservedBps int64

    // Local pacer
    pacer *LocalTokenBucket

    // Identity
    jobID    string
    workerID string

    // Cosmos DB (for metrics + throttle propagation only)
    cosmosContainer CosmosContainer

    // Lifecycle
    refCount int
    stopCh   chan struct{}
}

func NewReactiveRateLimiter(jobID, workerID string, cosmos CosmosContainer) *ReactiveRateLimiter {
    r := &ReactiveRateLimiter{
        jobID:           jobID,
        workerID:        workerID,
        capped:          false,
        pacer:           NewUnlimitedTokenBucket(),
        cosmosContainer: cosmos,
        refCount:        1,
        stopCh:          make(chan struct{}),
    }

    // Register worker heartbeat for observability
    r.refreshHeartbeat()
    go r.heartbeatLoop()

    return r
}

func (r *ReactiveRateLimiter) AcquireTokens(requested int64) int64 {
    return r.pacer.Take(min64(requested, MaxGrantCap))
}

func (r *ReactiveRateLimiter) HandleResponse(statusCode int, responseTime time.Duration, retryAfterSec float64) {
    if IsThrottleResponse(statusCode) {
        r.onThrottle(statusCode, retryAfterSec)
    } else {
        r.onSuccess(responseTime)
    }
}

func (r *ReactiveRateLimiter) onThrottle(statusCode int, retryAfterSec float64) {
    r.mu.Lock()

    r.lastThrottleAt = time.Now()

    // Debounce: only apply one multiplicative decrease per congestion event.
    // Multiple concurrent in-flight requests returning 429 within the same RTT
    // window represent ONE congestion signal, not N independent ones.
    debounceWindow := computeBackoff(r.throttleStreak, 0)
    if time.Since(r.lastDecreaseAt) >= debounceWindow {
        r.throttleStreak++
        r.lastDecreaseAt = time.Now()

        if !r.capped {
            // First throttle: snap a cap into existence from observed throughput.
            seed := r.lastObservedBps
            if seed == 0 {
                seed = DefaultUnmeteredRate
            }
            r.targetRate = max64(seed/2, MinRate)
            r.capped = true
        } else {
            // Multiplicative decrease — one halving per congestion event.
            r.targetRate = max64(r.targetRate/2, MinRate)
        }
        r.pacer.SetRate(r.targetRate)
    }
    // Goroutines that lost the debounce race still sleep their backoff
    // but do not modify targetRate or throttleStreak.

    backoff := computeBackoff(r.throttleStreak, retryAfterSec)
    r.mu.Unlock()

    // Publish to Cosmos DB (non-blocking, best-effort)
    go r.publishThrottleEvent(statusCode, backoff.Milliseconds())

    time.Sleep(backoff)
}

func (r *ReactiveRateLimiter) onSuccess(responseTime time.Duration) {
    r.mu.Lock()
    defer r.mu.Unlock()

    if !r.capped {
        return
    }

    timeSinceThrottle := time.Since(r.lastThrottleAt)

    // Additive increase
    if timeSinceThrottle >= RecoveryQuietPeriod {
        r.targetRate += AIMDStep
        r.throttleStreak = 0
        r.pacer.SetRate(r.targetRate)
    }

    // Release cap entirely after sustained quiet period + jitter
    uncapTime := UncapAfter + randomDuration(0, UncapJitter)
    if timeSinceThrottle >= uncapTime {
        r.capped = false
        r.targetRate = 0
        r.pacer.SetRate(math.MaxInt64)
    }
}

func (r *ReactiveRateLimiter) heartbeatLoop() {
    ticker := time.NewTicker(PollInterval)
    defer ticker.Stop()

    for {
        select {
        case <-ticker.C:
            r.refreshHeartbeat()
        case <-r.stopCh:
            r.deregister()
            return
        }
    }
}

func (r *ReactiveRateLimiter) Stop() {
    close(r.stopCh)
}

func (r *ReactiveRateLimiter) AddRef()      { r.mu.Lock(); r.refCount++; r.mu.Unlock() }
func (r *ReactiveRateLimiter) Release()     { r.mu.Lock(); r.refCount--; r.mu.Unlock() }
func (r *ReactiveRateLimiter) RefCount() int { r.mu.Lock(); defer r.mu.Unlock(); return r.refCount }
```

### 15.5 Helper Functions

```go
func IsThrottleResponse(statusCode int) bool {
    return statusCode == 429 || statusCode == 503
}

func computeBackoff(streak int, retryAfterSec float64) time.Duration {
    if retryAfterSec > 0 {
        return time.Duration(retryAfterSec * float64(time.Second))
    }
    backoff := BaseBackoff * time.Duration(1<<streak)
    if backoff > MaxBackoff {
        backoff = MaxBackoff
    }
    // Add ±20% jitter
    return addJitter(backoff, 0.2)
}

func addJitter(d time.Duration, fraction float64) time.Duration {
    jitter := time.Duration(float64(d) * fraction * (2*rand.Float64() - 1))
    return d + jitter
}

func randomDuration(min, max time.Duration) time.Duration {
    return min + time.Duration(rand.Int63n(int64(max-min)))
}

func min64(a, b int64) int64 {
    if a < b { return a }
    return b
}

func max64(a, b int64) int64 {
    if a > b { return a }
    return b
}
```

### 15.6 Worker Integration

```go
type Worker struct {
    mu           sync.Mutex
    workerID     string
    rateLimiters map[string]RateLimiter
    cosmosContainer CosmosContainer
}

func (w *Worker) getOrCreateRateLimiter(jobID string, jobRate int64) RateLimiter {
    w.mu.Lock()
    defer w.mu.Unlock()

    rl, exists := w.rateLimiters[jobID]
    if exists {
        rl.AddRef()
        return rl
    }

    if jobRate > 0 {
        rl = NewProactiveRateLimiter(jobID, w.workerID, jobRate, w.cosmosContainer)
    } else {
        rl = NewReactiveRateLimiter(jobID, w.workerID, w.cosmosContainer)
    }

    w.rateLimiters[jobID] = rl
    return rl
}

func (w *Worker) releaseRateLimiter(jobID string) {
    w.mu.Lock()
    rl := w.rateLimiters[jobID]
    w.mu.Unlock()

    rl.Release()

    if rl.RefCount() == 0 {
        time.AfterFunc(CleanupDelay, func() {
            if rl.RefCount() == 0 {
                rl.Stop()
                w.mu.Lock()
                delete(w.rateLimiters, jobID)
                w.mu.Unlock()
            }
        })
    }
}

func (w *Worker) processLoop(queue <-chan JobPlan) {
    sem := make(chan struct{}, MaxConcurrentJPs)

    for jp := range queue {
        sem <- struct{}{}
        go func(jp JobPlan) {
            defer func() { <-sem }()

            rl := w.getOrCreateRateLimiter(jp.JobID, jp.JobRate)
            defer w.releaseRateLimiter(jp.JobID)

            w.processJobPlan(jp, rl)
        }(jp)
    }
}

func (w *Worker) processJobPlan(jp JobPlan, rl RateLimiter) {
    for _, file := range jp.Files {
        for offset := int64(0); offset < file.Size; {
            size := min64(blockSize, file.Size-offset)

            granted := rl.AcquireTokens(size)
            if granted == 0 {
                time.Sleep(10 * time.Millisecond)
                continue
            }

            statusCode, respTime, retryAfter := transferBlock(file, offset, granted)
            rl.HandleResponse(statusCode, respTime, retryAfter)

            if !IsThrottleResponse(statusCode) {
                offset += granted
            }
        }
    }
}
```

---

## Key Differences from V3

| Aspect | V3 (Redis Grant Bucket) | V4 (Cosmos DB Dual-Mode) |
|--------|------------------------|--------------------------|
| Infrastructure | Redis (dedicated) | Cosmos DB (existing) |
| Coordination | Atomic Lua scripts | Self-enforced local pacing |
| Mode A sizing | Demand-based (`lastConsumed × 1.25`) | Equal share (`jobRate / activeCount`) |
| Mode A fairness | Emergent | Guaranteed by construction |
| Mode A convergence | ~3s ramp | Instant (next poll) |
| Mode B | Added as §12.8 | First-class design mode |
| Worker count | Not tracked | TTL heartbeat docs in Cosmos |
| Overshoot guard | Centralized bucket (atomic) | Accepted brief staleness (~1 poll interval) |
| Redis dependency | Critical for all operations | **Eliminated** |
| Cosmos dependency | None | Mode A: required. Mode B: observability only |
| RU cost | N/A (Redis ops) | ~650–3250 RU/s per job |
