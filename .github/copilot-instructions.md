# Streaming Merge-Join for Sync Orchestrator — Project Context

## Branch: `users/pbanakar/streaming-merge-join`

### What This Branch Does
Adds a streaming merge-join algorithm to the sync orchestrator for remote-to-remote sync (Blob→Blob, S3→Blob, BlobFS→BlobFS). Replaces the O(N) memory indexMap-based comparison with O(1) streaming comparison for sources that guarantee lexicographic listing order.

### Architecture

**Two code paths in `syncOneDir`** (in `syncOrchestrator.go`):
1. **Merge-join path** (Blob/S3/BlobFS sources): `useStreamingMergeJoin(fromTo)` returns true → calls `mergeJoinSyncDir()`
2. **IndexMap path** (local filesystem): Keeps existing store-all-then-compare flow

**Merge-join flow per directory:**
```
TreeCrawler (parallelism=500) → syncOneDir → mergeJoinSyncDir
  ├─ traverserToChannel(src) → srcCh (buffer=10,000)
  ├─ traverserToChannel(dst) → dstCh (buffer=10,000)
  └─ Lockstep merge-join:
       srcPath < dstPath → source-only → scheduleCopyTransfer
       srcPath > dstPath → dest-only → deleteDestination policy
       srcPath == dstPath → compare LMT → transfer if stale
```

**Key design decisions:**
- Source-driven crawl: TreeCrawler discovers directories from source listing, so dest-only subtrees are never visited (O(1) skip)
- Per-directory hierarchy listing (not flat listing) — preserves the ability to skip entire dest-only prefixes without iterating them
- Throttling disabled for merge-join: no ReadMemStats STW, no file/goroutine limits
- `EnumerationParallelism = 1`: inner blob traverser doesn't spawn 16 goroutines per flat directory
- Default parallelism: 500 (env var `AZCOPY_MERGE_JOIN_PARALLELISM` for override)

### Files Modified

| File | Change |
|------|--------|
| `cmd/syncMergeJoin.go` | **NEW** — core merge-join algorithm (327 lines) |
| `cmd/syncOrchestrator.go` | Fork syncOneDir into merge-join vs indexMap paths, CrawlWithStats, diagnostic logging |
| `cmd/syncThrottler.go` | Disable throttling for merge-join, `getMergeJoinParallelism()`, EnumerationParallelism=1 |
| `cmd/syncComparator.go` | Fix: both-zero change times no longer flags metadata changed |
| `common/parallel/TreeCrawler.go` | `CrawlWithStats()` with live ActiveWorkers/QueuedDirs counters |

### Performance Results (500K arcgis-cache dataset, 514,500 files)

| Metric | COPY | Sync: indexMap (ratio=1.0) | **Sync: Merge-join** |
|--------|------|----------------------------|---------------------|
| Files scanned | 514,500 | 514,500 | **514,500** |
| Scan rate | 1,476/sec | 282/sec | **~995/sec** |
| Scan duration | ~5.8 min | ~30.4 min | **~9.4 min** |
| Container runtime | 16.4 min | 36.4 min | **11 min** |
| Parallelism | N/A | 42 (auto) | **500** |

### Known Issues / Gaps
1. **`mergeJoinHandleBothExist` uses only LMT comparison** — does not call `processIfNecessaryWithOrchestrator` (size+changeTime checks). Fine for c2c blob sync, but not feature-complete vs indexMap path.
2. **Diagnostic logging is verbose** — `[STEP]` logs emit for every directory. Should be removed or gated behind debug level before production merge.
3. **5000 parallelism OOM'd** — at 5000 workers, goroutine+connection overhead exceeded 8GB Go heap. 500 is the safe default.

### Scaling Discussion (100B objects)
- **Current per-directory approach works** but at 10B directories → 20B API calls → 11.6 days at 20K req/sec throttle
- **Pure flat listing fails** for dest-only subtrees: must iterate all dest blobs even when not needed
- **Best approach for 100B**: Adaptive prefix-sharded flat listing with source-driven discovery
  - Use hierarchy listing to discover prefixes until ~500-1000 shards exist
  - Each shard does flat listing merge-join of its subtree
  - Source-only shards → schedule all, Dest-only shards → skip (or delete), Both → flat merge-join
  - Estimated: ~1-2 hours for 100B (vs 11.6 days with per-directory)

### Agent Repo (Storage-XDataMove-Agent-Linux)
- Branch: `users/pbanakar/streaming-merge-join` on Azure DevOps
- `xdatamoved/go.mod`: points to azcopy commit `9bcedfb746cf` (this branch)
- New test: `xdatamoved/cloud2cloud/e2etests/dataplane_merge_join_e2e_test.go`
- **For local dev**: uncomment line 18 in go.mod (`replace => ../azure-storage-azcopy`), run `go mod tidy`

### Deployment (c2cbiceptest2 environment)
- ACR: `c2ccontainerregistry3.azurecr.io/worker:latest`
- ACA job: `worker-sm-dp-test-westus2`, RG: `c2cbiceptest2`, Sub: `31347be8-d066-464e-9866-7e58d85027b7`
- LAW ID: `f89e271d-10af-46dc-b281-0ab2f7fa71c5`
- Source dataset: `a2asource/arcgis-cache-500k` — 514,500 files in ~348K virtual directories
- Dataset structure: `arcgiscache/layer_XX(70)/tile_XX(70)/chunk_XX(70)/tile_N.dat(1-2 files)`
- Container: 30GB memory, 7.5 CPU
- Docker build: `docker build -t worker -f xdatamoved/cloud2cloud/worker/Dockerfile .` from Agent root

### Build Tag
All syncOrchestrator code requires build tag `smslidingwindow`. The Docker build includes it.

### Pending Work
- [ ] Get a clean perf run with 500 parallelism in container (last run was poisoned by OOM retries)
- [ ] Consider adding size+changeTime comparison to `mergeJoinHandleBothExist`
- [ ] Remove/reduce verbose [STEP] diagnostic logging
- [ ] Test with `deleteDestination=true` scenario
- [ ] Investigate future optimization: batch listing at higher prefix levels for deep hierarchies
