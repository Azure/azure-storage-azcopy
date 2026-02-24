# InodeStore — How AzCopy Tracks Hardlinks

## What Problem Does It Solve?

On Linux, multiple file paths can point to the same data on disk. These are called
**hardlinks**. When you use `azcopy sync --hardlinks=preserve` (or `azcopy copy`),
AzCopy needs to detect these relationships at the source and recreate them at the
destination instead of copying the same data twice.

InodeStore is the component that makes this possible. It keeps a lightweight
registry of every inode (the OS-level identifier for a file's data) and which file
paths share it.

---

## How It Works — The Big Picture

```
   Source filesystem                InodeStore                 Destination (Azure Files NFS)
  ┌─────────────────┐         ┌──────────────────┐          ┌──────────────────────────┐
  │                  │         │                  │          │                          │
  │  photo.jpg ──┐   │  scan   │  inode 99001:    │  sync    │  photo.jpg  (file data)  │
  │              ├───┼────────►│    anchor =      │─────────►│  backup.jpg (hardlink)   │
  │  backup.jpg ─┘   │         │    "backup.jpg"  │          │                          │
  │                  │         │                  │          │  (CreateHardLink API)    │
  └─────────────────┘         └──────────────────┘          └──────────────────────────┘
```

1. AzCopy walks the source directory tree.
2. For each file with more than one link (`nlink > 1`), it reads the inode number
   and registers the path in InodeStore.
3. The **first** path seen for an inode is uploaded as a normal file.
4. Every subsequent path sharing that inode is transferred as a **hardlink
   operation** — no data is re-uploaded; Azure Files creates the link server-side.

---

## Architecture

InodeStore has two layers:

```
┌──────────────────────────────────────────────────┐
│                   InodeStore                     │
│                                                  │
│   ┌────────────────────┐   ┌──────────────────┐  │
│   │   In-Memory Index  │   │   Disk File      │  │
│   │                    │   │                  │  │
│   │  inode → location  │──►│  inode  path  anchor │
│   │  (~24 bytes each)  │   │  inode  path  anchor │
│   │                    │   │  ...             │  │
│   └────────────────────┘   └──────────────────┘  │
│                                                  │
│   Thread-safe (read-write lock)                  │
└──────────────────────────────────────────────────┘
```

- **In-memory index** — a small map that stores only the byte offset of each
  inode's record on disk (~24 bytes per inode). This keeps RAM usage minimal even
  for millions of files.
- **Disk file** — a flat file (`~/.azcopy/plans/inodeStore-<jobID>.txt`) where each
  inode gets one fixed-width record containing the inode number, the first path
  discovered, and the current **anchor** path.

### What Is an "Anchor"?

Among all file paths that share the same inode, InodeStore picks the
**lexicographically smallest** path as the *anchor*. This ensures the choice is
deterministic — no matter what order AzCopy discovers files, the anchor is always
the same.

The anchor serves two purposes:

| During copy | During sync |
|-------------|-------------|
| The anchor path is uploaded as a regular file; all other paths in the group become `CreateHardLink` calls pointing to it. | The anchor at source is compared to the anchor at destination to detect whether a hardlink's target has changed. |

---

## Lifecycle

```
 azcopy sync --hardlinks=preserve ...
       │
       ▼
 ┌─ Job starts ──────────────────────────────────┐
 │  InodeStore created (one per job)             │
 │  Backing file: ~/.azcopy/plans/inodeStore-*.txt│
 └───────────────────────────────────────────────┘
       │
       ▼
 ┌─ Source traversal ────────────────────────────┐
 │  For each file with nlink > 1:                │
 │    Register (inode, path) in InodeStore       │
 │    First path  → mark as regular file         │
 │    Later paths → mark as hardlink             │
 └───────────────────────────────────────────────┘
       │
       ▼
 ┌─ Comparison & transfer ──────────────────────┐
 │  Regular files: compared by last-modified time│
 │  Hardlinks: deferred until all files are known│
 │    then resolved by comparing anchors         │
 └───────────────────────────────────────────────┘
       │
       ▼
 ┌─ Cleanup ─────────────────────────────────────┐
 │  Backing file removed by `azcopy jobs clean`  │
 └───────────────────────────────────────────────┘
```

---

## How Hardlinks Are Compared During Sync

Hardlinks need special handling during sync because the relationship between paths
matters, not just individual file content. AzCopy defers hardlink comparison until
every path at both source and destination has been registered.

Then, for each destination hardlink:

```
Source has same path?
  │
  ├─ Yes ─► Do the anchors match?
  │           │
  │           ├─ Yes ─► Is source newer? ─► Transfer / Skip
  │           │
  │           └─ No ──► Delete old link, recreate with correct target
  │
  └─ No ──► Delete stale link from destination
```

**Example — anchor mismatch (retarget):**

Before sync:
- Source: `link.txt` is a hardlink to `new_target.txt` (anchor = `new_target.txt`)
- Dest:   `link.txt` is a hardlink to `old_target.txt` (anchor = `old_target.txt`)

After sync:
- AzCopy detects the anchor mismatch, deletes the destination `link.txt`, and
  recreates it as a hardlink to `new_target.txt`.

---

## Design Decisions

### Why disk-backed instead of purely in-memory?

Large directory trees can contain millions of hardlinked files. Storing full path
strings for every inode in memory would consume significant RAM. By keeping only a
24-byte pointer per inode in memory and storing the actual paths on disk, InodeStore
scales to millions of inodes with minimal memory overhead.

### Why fixed-width records with padding?

Each record is padded with 64 extra bytes. When the anchor path changes (a shorter
path is discovered), InodeStore can update the record in-place without rewriting
the entire file. This avoids expensive file compaction and keeps writes fast.

### Why lexicographic ordering for anchors?

Filesystem traversal order is non-deterministic and can vary between runs. By always
picking the lexicographically smallest path as the anchor, AzCopy guarantees that:

- Repeated syncs produce stable, predictable results.
- Source and destination anchor comparisons are meaningful even if files were
  discovered in different orders.

---

## Summary

| Aspect | Detail |
|--------|--------|
| **Purpose** | Track which file paths share the same data (hardlinks) |
| **Storage** | Flat file on disk + small in-memory index |
| **Memory per inode** | ~24 bytes |
| **Anchor selection** | Lexicographically smallest path (deterministic) |
| **Thread safety** | Read-write lock; concurrent reads, exclusive writes |
| **File location** | `~/.azcopy/plans/inodeStore-<jobID>.txt` |
| **Cleanup** | Automatic via `azcopy jobs clean` |
