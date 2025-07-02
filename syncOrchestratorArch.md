# SyncOrchestrator Architecture Documentation

## Table of Contents
1. [Overview](#overview)
2. [High-Level Architecture](#high-level-architecture)
3. [Core Components](#core-components)
4. [Data Flow](#data-flow)
5. [Concurrency Model](#concurrency-model)
6. [Throttling System](#throttling-system)
7. [Memory Management](#memory-management)
8. [Error Handling](#error-handling)
9. [Performance Optimizations](#performance-optimizations)
10. [Configuration](#configuration)
11. [Monitoring and Observability](#monitoring-and-observability)
12. [Diagrams](#diagrams)

## Overview

The SyncOrchestrator is a sophisticated file synchronization system designed for high-performance, large-scale directory synchronization operations. It implements a sliding window approach with intelligent throttling, dynamic resource management, and optimized enumeration strategies.

### Key Features
- **Sliding Window Processing**: Processes directories in parallel while managing memory usage
- **Dynamic Throttling**: Adaptive resource management based on system load
- **Intelligent Enumeration**: Optimized traversal with ctime-based skip optimizations
- **Multi-Source Support**: Handles Local, S3, and other storage systems
- **Resource-Aware Scaling**: Automatically adjusts concurrency based on available resources

## High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    SyncOrchestrator System                      │
├─────────────────────────────────────────────────────────────────┤
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐  │
│  │   Entry Point   │  │   Configuration │  │   Monitoring    │  │
│  │                 │  │                 │  │                 │  │
│  │ ┌─────────────┐ │  │ ┌─────────────┐ │  │ ┌─────────────┐ │  │
│  │ │ Custom Sync │ │  │ │ Orchestrator│ │  │ │ Stats       │ │  │
│  │ │ Handler     │ │  │ │ Options     │ │  │ │ Monitor     │ │  │
│  │ └─────────────┘ │  │ └─────────────┘ │  │ └─────────────┘ │  │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘  │
├─────────────────────────────────────────────────────────────────┤
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐  │
│  │   Core Engine   │  │   Throttling    │  │   Traversal     │  │
│  │                 │  │                 │  │                 │  │
│  │ ┌─────────────┐ │  │ ┌─────────────┐ │  │ ┌─────────────┐ │  │
│  │ │ Sync        │ │  │ │ Dir         │ │  │ │ Sync        │ │  │
│  │ │ Enumerator  │ │  │ │ Semaphore   │ │  │ │ Traverser   │ │  │
│  │ └─────────────┘ │  │ └─────────────┘ │  │ └─────────────┘ │  │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘  │
├─────────────────────────────────────────────────────────────────┤
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐  │
│  │   Data Layer    │  │   Error Mgmt    │  │   Optimization  │  │
│  │                 │  │                 │  │                 │  │
│  │ ┌─────────────┐ │  │ ┌─────────────┐ │  │ ┌─────────────┐ │  │
│  │ │ Object      │ │  │ │ Error       │ │  │ │ CTime       │ │  │
│  │ │ Indexer     │ │  │ │ Channels    │ │  │ │ Optimization│ │  │
│  │ └─────────────┘ │  │ └─────────────┘ │  │ └─────────────┘ │  │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

## Core Components

### 1. CustomSyncHandlerFunc
**Purpose**: Entry point for sync operations
**Responsibilities**:
- Initialize profiling and monitoring
- Set up resource limits based on system capabilities
- Delegate to the main orchestrator

```go
type CustomSyncHandlerFunc func(cca *cookedSyncCmdArgs, enumerator *syncEnumerator, ctx context.Context) error
```

### 2. SyncTraverser
**Purpose**: Manages traversal of individual directories
**Key Fields**:
- `enumerator`: Reference to main sync enumerator
- `comparator`: Processes destination objects for comparison
- `dir`: Current directory being processed
- `sub_dirs`: Discovered subdirectories for future processing

**Responsibilities**:
- Process source files and directories
- Compare with destination objects
- Schedule transfers
- Discover and queue subdirectories

### 3. DirSemaphore
**Purpose**: Controls directory processing concurrency
**Key Features**:
- Semaphore-based concurrency control
- Multi-factor throttling (files, memory, goroutines)
- Hysteresis to prevent oscillation
- Context-aware cancellation

### 4. StatsMonitor
**Purpose**: Dynamic performance monitoring and adjustment
**Key Features**:
- Continuous performance sampling
- Trend analysis with consistency requirements
- Dynamic limit adjustments
- Configurable thresholds and cooldown periods

### 5. Object Indexer
**Purpose**: Temporary storage for enumerated objects
**Key Features**:
- Thread-safe object storage
- Memory-efficient indexing
- Support for cleanup operations

## Data Flow

### Primary Data Flow

```
Source Location                    Destination Location
      │                                   │
      ▼                                   ▼
┌─────────────┐                    ┌─────────────┐
│  Traverser  │                    │  Traverser  │
│   (Source)  │                    │   (Dest)    │
└─────────────┘                    └─────────────┘
      │                                   │
      ▼                                   ▼
┌─────────────┐                    ┌─────────────┐
│   Processor │                    │ Comparator  │
│  (Source)   │                    │   (Dest)    │
└─────────────┘                    └─────────────┘
      │                                   │
      ▼                                   ▼
┌─────────────────────────────────────────────────┐
│              Object Indexer                     │
│  ┌─────────────┐  ┌─────────────┐  ┌──────────┐ │
│  │   Source    │  │Destination  │  │Comparison│ │
│  │   Objects   │  │   Objects   │  │ Results  │ │
│  └─────────────┘  └─────────────┘  └──────────┘ │
└─────────────────────────────────────────────────┘
      │
      ▼
┌─────────────┐
│  Transfer   │
│  Scheduler  │
└─────────────┘
```

### Directory Processing Flow

```
Root Directory
      │
      ▼
┌─────────────────┐
│ Initialize      │
│ SyncTraverser   │
└─────────────────┘
      │
      ▼
┌─────────────────┐     ┌─────────────────┐
│ Source          │────▶│ Object Indexer  │
│ Enumeration     │     │ (Store Objects) │
└─────────────────┘     └─────────────────┘
      │
      ▼
┌─────────────────┐     ┌─────────────────┐
│ Destination     │────▶│ Object          │
│ Enumeration     │     │ Comparator      │
└─────────────────┘     └─────────────────┘
      │
      ▼
┌─────────────────┐
│ Finalize &      │
│ Schedule        │
│ Transfers       │
└─────────────────┘
      │
      ▼
┌─────────────────┐
│ Enqueue         │
│ Subdirectories  │
└─────────────────┘
```

## Concurrency Model

### Parallel Directory Processing

```
┌─────────────────────────────────────────────────────────────┐
│                 Parallel Crawling System                    │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  Root Dir ────┐                                             │
│               │                                             │
│               ▼                                             │
│         ┌──────────┐    ┌──────────┐    ┌──────────┐       │
│         │   Dir    │    │   Dir    │    │   Dir    │       │
│         │    A     │    │    B     │    │    C     │       │
│         └──────────┘    └──────────┘    └──────────┘       │
│               │               │               │             │
│               ▼               ▼               ▼             │
│         ┌──────────┐    ┌──────────┐    ┌──────────┐       │
│         │SyncTrav A│    │SyncTrav B│    │SyncTrav C│       │
│         └──────────┘    └──────────┘    └──────────┘       │
│               │               │               │             │
│               ▼               ▼               ▼             │
│         ┌──────────┐    ┌──────────┐    ┌──────────┐       │
│         │Sub-dirs  │    │Sub-dirs  │    │Sub-dirs  │       │
│         │A1,A2,A3  │    │B1,B2     │    │C1,C2,C3,C4│      │
│         └──────────┘    └──────────┘    └──────────┘       │
│                                                             │
├─────────────────────────────────────────────────────────────┤
│                  Semaphore Control                         │
│                                                             │
│  Available Slots: [██████░░░░] 6/10                        │
│  Waiting Queue:   [Dir D1][Dir D2][Dir D3]                 │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### Thread Safety Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                   Thread Safety Model                       │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌─────────────┐     ┌─────────────┐     ┌─────────────┐   │
│  │ Goroutine 1 │     │ Goroutine 2 │     │ Goroutine N │   │
│  │             │     │             │     │             │   │
│  │┌───────────┐│     │┌───────────┐│     │┌───────────┐│   │
│  ││SyncTrav A ││     ││SyncTrav B ││     ││SyncTrav N ││   │
│  │└───────────┘│     │└───────────┘│     │└───────────┘│   │
│  └─────────────┘     └─────────────┘     └─────────────┘   │
│         │                     │                     │       │
│         ▼                     ▼                     ▼       │
│  ┌─────────────────────────────────────────────────────┐   │
│  │              syncMutex (Global Lock)               │   │
│  │                                                     │   │
│  │  ┌─────────────────────────────────────────────┐   │   │
│  │  │           Object Indexer                    │   │   │
│  │  │                                             │   │   │
│  │  │  Map[string]StoredObject                    │   │   │
│  │  │                                             │   │   │
│  │  │  ┌─────────┐ ┌─────────┐ ┌─────────────┐   │   │   │
│  │  │  │ Store() │ │Delete() │ │Counter Ops │   │   │   │
│  │  │  └─────────┘ └─────────┘ └─────────────┘   │   │   │
│  │  └─────────────────────────────────────────────┘   │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

## Throttling System

### Multi-Factor Throttling Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                  Throttling Control System                  │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
│  │   File      │  │   Memory    │  │    Goroutine        │  │
│  │ Throttling  │  │ Throttling  │  │   Throttling        │  │
│  │             │  │             │  │                     │  │
│  │ Current:    │  │ Current:    │  │ Current: 45,000     │  │
│  │ 8.5M files │  │ 78% usage   │  │ Limit:   50,000     │  │
│  │ Limit: 10M  │  │ Limit: 80%  │  │ Status:  OK         │  │
│  │ Status: OK  │  │ Status: OK  │  │                     │  │
│  └─────────────┘  └─────────────┘  └─────────────────────┘  │
│         │                │                      │            │
│         ▼                ▼                      ▼            │
│  ┌─────────────────────────────────────────────────────┐    │
│  │              Hysteresis Logic                       │    │
│  │                                                     │    │
│  │  Engage Threshold:  100% (files), 80% (memory)     │    │
│  │  Release Threshold:  85% (files), 70% (memory)     │    │
│  │                                                     │    │
│  │  Current State: [NORMAL] [NORMAL] [NORMAL]         │    │
│  └─────────────────────────────────────────────────────┘    │
│                           │                                  │
│                           ▼                                  │
│  ┌─────────────────────────────────────────────────────┐    │
│  │              Directory Semaphore                    │    │
│  │                                                     │    │
│  │  Available Slots: [██████████] 50/50               │    │
│  │                                                     │    │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐ │    │
│  │  │AcquireSlot()│  │ReleaseSlot()│  │shouldThrottle│ │    │
│  │  └─────────────┘  └─────────────┘  └─────────────┘ │    │
│  └─────────────────────────────────────────────────────┘    │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### Throttling State Machine

```
                    ┌─────────────┐
                    │   NORMAL    │
                    │  Operation  │
                    └─────────────┘
                           │
                Resource > Engage Threshold
                           │
                           ▼
    ┌─────────────────────────────────────────────────────┐
    │                 THROTTLED                           │
    │                                                     │
    │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐ │
    │  │    Files    │  │   Memory    │  │ Goroutines  │ │
    │  │   Active    │  │   Active    │  │   Active    │ │
    │  └─────────────┘  └─────────────┘  └─────────────┘ │
    │         │                │                │        │
    │         ▼                ▼                ▼        │
    │  Block Directory Acquisition Until                 │
    │  Resource < Release Threshold                      │
    └─────────────────────────────────────────────────────┘
                           │
                Resource < Release Threshold
                           │
                           ▼
                    ┌─────────────┐
                    │   NORMAL    │
                    │  Operation  │
                    └─────────────┘
```

## Memory Management

### Object Indexer Lifecycle

```
Directory Processing Lifecycle:

1. ENUMERATION PHASE
   ┌─────────────────┐
   │   Source        │
   │ Enumeration     │
   │                 │  Objects Added
   │ ┌─────────────┐ │ ────────────┐
   │ │   Files     │ │             │
   │ │Directories  │ │             ▼
   │ └─────────────┘ │  ┌─────────────────┐
   └─────────────────┘  │ Object Indexer  │
                        │                 │
   ┌─────────────────┐  │ ┌─────────────┐ │
   │  Destination    │  │ │  Map Store  │ │
   │ Enumeration     │  │ │             │ │
   │                 │  │ │ File1: SO1  │ │
   │ ┌─────────────┐ │  │ │ File2: SO2  │ │
   │ │Comparison   │ │ ─│▶│ Dir1:  SO3  │ │
   │ │Operations   │ │  │ │     ...     │ │
   │ └─────────────┘ │  │ └─────────────┘ │
   └─────────────────┘  └─────────────────┘

2. PROCESSING PHASE
   ┌─────────────────┐
   │  Finalize()     │
   │                 │  Objects Retrieved & Scheduled
   │ ┌─────────────┐ │ ────────────┐
   │ │ Schedule    │ │             │
   │ │ Transfers   │ │             ▼
   │ └─────────────┘ │  ┌─────────────────┐
   └─────────────────┘  │Transfer Scheduler│
                        └─────────────────┘

3. CLEANUP PHASE                    ▲
   ┌─────────────────┐             │
   │ Memory Cleanup  │ ────────────┘
   │                 │  Objects Removed
   │ ┌─────────────┐ │  From Indexer
   │ │   delete()  │ │
   │ │  operations │ │
   │ └─────────────┘ │
   └─────────────────┘
```

### Memory Optimization Strategies

```
┌─────────────────────────────────────────────────────────────┐
│                Memory Optimization Strategy                  │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  1. SLIDING WINDOW APPROACH                                 │
│  ┌─────────────────────────────────────────────────────┐   │
│  │  Directory A │ Directory B │ Directory C │ ...      │   │
│  │  [COMPLETE]  │ [ACTIVE]    │ [QUEUED]    │          │   │
│  │      │       │      │      │      │      │          │   │
│  │   Cleanup    │   Process   │    Wait     │          │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                             │
│  2. IMMEDIATE CLEANUP                                       │
│  ┌─────────────────────────────────────────────────────┐   │
│  │  Object Processed ────▶ Schedule Transfer ────▶     │   │
│  │                                             │       │   │
│  │                          Remove from   ◀───┘       │   │
│  │                           Indexer                  │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                             │
│  3. RESOURCE-BASED LIMITS                                  │
│  ┌─────────────────────────────────────────────────────┐   │
│  │  Available Memory: 32GB                             │   │
│  │  Files per GB:     500K                             │   │
│  │  Max Active Files: 16M                              │   │
│  │  Current Usage:    8.5M (53%)                       │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

## Error Handling

### Error Flow Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Error Handling System                    │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌─────────────────┐    ┌─────────────────┐               │
│  │  Source Error   │    │ Destination     │               │
│  │                 │    │ Error           │               │
│  │ ┌─────────────┐ │    │ ┌─────────────┐ │               │
│  │ │File Access │ │    │ │404 Response │ │               │
│  │ │Permission   │ │    │ │Network      │ │               │
│  │ │Network      │ │    │ │Timeout      │ │               │
│  │ └─────────────┘ │    │ └─────────────┘ │               │
│  └─────────────────┘    └─────────────────┘               │
│           │                       │                        │
│           ▼                       ▼                        │
│  ┌─────────────────────────────────────────────────────┐   │
│  │              Error Classification                   │   │
│  │                                                     │   │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐ │   │
│  │  │  Expected   │  │Recoverable  │  │   Fatal     │ │   │
│  │  │  Errors     │  │   Errors    │  │   Errors    │ │   │
│  │  │             │  │             │  │             │ │   │
│  │  │• 404 Resp   │  │• Temp Net   │  │• Perm Fail  │ │   │
│  │  │• Missing    │  │• Throttling │  │• Auth Fail  │ │   │
│  │  │  Files      │  │• Rate Limit │  │• Corruption │ │   │
│  │  └─────────────┘  └─────────────┘  └─────────────┘ │   │
│  └─────────────────────────────────────────────────────┘   │
│           │                       │              │         │
│           ▼                       ▼              ▼         │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐ │
│  │   Log &     │  │   Retry     │  │   Abort Operation   │ │
│  │  Continue   │  │ Mechanism   │  │   Report Fatal      │ │
│  └─────────────┘  └─────────────┘  └─────────────────────┘ │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### Error Channel Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                 Error Channel System                        │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  Multiple Goroutines                                        │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐         │
│  │Traverser 1  │  │Traverser 2  │  │Traverser N  │         │
│  │             │  │             │  │             │         │
│  │   Error ────┼──┼─Error ──────┼──┼─Error ──────┼──┐      │
│  └─────────────┘  └─────────────┘  └─────────────┘  │      │
│                                                     │      │
│                                                     ▼      │
│  ┌─────────────────────────────────────────────────────┐   │
│  │           Error Channel (Buffered)                 │   │
│  │                                                     │   │
│  │  [Error1][Error2][Error3][....][ErrorN]           │   │
│  │                                                     │   │
│  │  Non-blocking writes with fallback logging         │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                     │      │
│                                                     ▼      │
│  ┌─────────────────────────────────────────────────────┐   │
│  │              Error Consumer                         │   │
│  │                                                     │   │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐ │   │
│  │  │   Logging   │  │Aggregation  │  │  Reporting  │ │   │
│  │  └─────────────┘  └─────────────┘  └─────────────┘ │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

## Performance Optimizations

### CTime Optimization

```
┌─────────────────────────────────────────────────────────────┐
│                  CTime Optimization Flow                    │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  Directory Processing Decision Tree:                        │
│                                                             │
│  ┌─────────────────┐                                       │
│  │  Start Dir      │                                       │
│  │  Processing     │                                       │
│  └─────────────────┘                                       │
│           │                                                 │
│           ▼                                                 │
│  ┌─────────────────┐                                       │
│  │ Check CTime     │                                       │
│  │ Optimization    │                                       │
│  │ Conditions      │                                       │
│  └─────────────────┘                                       │
│           │                                                 │
│           ▼                                                 │
│    ┌─────────────┐           ┌─────────────────────────┐   │
│    │ Conditions  │───NO────▶ │  Full Enumeration       │   │
│    │    Met?     │           │                         │   │
│    └─────────────┘           │  • Source + Dest        │   │
│           │                  │  • Complete Comparison  │   │
│          YES                 └─────────────────────────┘   │
│           ▼                                                 │
│  ┌─────────────────┐                                       │
│  │ Check Parent    │                                       │
│  │ Dir CTime vs    │                                       │
│  │ Last Sync Time  │                                       │
│  └─────────────────┘                                       │
│           │                                                 │
│           ▼                                                 │
│    ┌─────────────┐           ┌─────────────────────────┐   │
│    │ Parent Dir  │───NO────▶ │  Check Individual       │   │
│    │ Changed?    │           │  File CTimes            │   │
│    └─────────────┘           └─────────────────────────┘   │
│           │                              │                 │
│          YES                            ▼                  │
│           │                  ┌─────────────────────────┐   │
│           │                  │   Any File Changed?     │   │
│           │                  └─────────────────────────┘   │
│           │                              │                 │
│           │                             YES                │
│           ▼                              ▼                 │
│  ┌─────────────────┐           ┌─────────────────────────┐ │
│  │  Full Target    │           │   Full Target           │ │
│  │  Enumeration    │           │   Enumeration           │ │
│  └─────────────────┘           └─────────────────────────┘ │
│                                             │               │
│                                            NO               │
│                                             ▼               │
│                                 ┌─────────────────────────┐ │
│                                 │   Skip Target           │ │
│                                 │   Enumeration           │ │
│                                 │                         │ │
│                                 │   Schedule Transfers    │ │
│                                 │   Without Comparison    │ │
│                                 └─────────────────────────┘ │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### Resource Utilization Curve

```
Performance vs Resource Usage:

 Performance
      ▲
  100%│     ╭─────╮ Optimal Zone
      │    ╱       ╲
   80%│   ╱         ╲
      │  ╱           ╲
   60%│ ╱             ╲
      │╱               ╲
   40%│                 ╲
      │                  ╲
   20%│                   ╲___
      │                       ╲___
    0%└─────────────────────────────╲──▶ Resource Usage
      0%   20%   40%   60%   80%  100%

Zone Analysis:
• 0-40%:   Underutilized - Can increase concurrency
• 40-80%:  Optimal - Best performance/resource ratio  
• 80-100%: Saturated - Risk of diminishing returns
• >100%:   Over-saturated - Performance degradation
```

## Configuration

### Configuration Hierarchy

```
┌─────────────────────────────────────────────────────────────┐
│                Configuration Hierarchy                      │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌─────────────────────────────────────────────────────┐   │
│  │                System Detection                     │   │
│  │                                                     │   │
│  │  • CPU Cores: runtime.NumCPU()                     │   │
│  │  • Memory: common.GetTotalPhysicalMemory()         │   │
│  │  • FromTo Configuration                             │   │
│  └─────────────────────────────────────────────────────┘   │
│                           │                                 │
│                           ▼                                 │
│  ┌─────────────────────────────────────────────────────┐   │
│  │              Dynamic Calculation                    │   │
│  │                                                     │   │
│  │  • CrawlParallelism = NumCPU × Multiplier          │   │
│  │  • MaxActiveFiles = MemoryGB × 500K                │   │
│  │  • Multipliers:                                     │   │
│  │    - Local: 4x                                      │   │
│  │    - S3: 8x                                         │   │
│  │    - Default: 2x                                    │   │
│  └─────────────────────────────────────────────────────┘   │
│                           │                                 │
│                           ▼                                 │
│  ┌─────────────────────────────────────────────────────┐   │
│  │              Runtime Adjustment                     │   │
│  │                                                     │   │
│  │  • StatsMonitor continuous optimization             │   │
│  │  • Throttling based on actual usage                │   │
│  │  • Hysteresis to prevent oscillation               │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### Configuration Parameters

| Parameter | Default | Description | Tuning Guidance |
|-----------|---------|-------------|-----------------|
| `crawlParallelism` | NumCPU × Multiplier | Directory processing concurrency | Increase for I/O bound, decrease for CPU bound |
| `maxActiveFiles` | MemoryGB × 500K | Maximum files in indexer | Monitor memory usage, adjust based on available RAM |
| `throttleEngageThreshold` | 100% | When to start throttling | Lower for more conservative resource usage |
| `throttleReleaseThreshold` | 85% | When to stop throttling | Higher values reduce oscillation |
| `consistencyThreshold` | 10 samples | Samples needed for trend analysis | Higher values increase stability |
| `adjustmentCooldown` | 2 minutes | Time between limit adjustments | Increase to reduce adjustment frequency |

## Monitoring and Observability

### Metrics Collection

```
┌─────────────────────────────────────────────────────────────┐
│                    Metrics Dashboard                        │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  Real-time Counters:                                       │
│  ┌─────────────────┐  ┌─────────────────┐                 │
│  │ Active Dirs     │  │ Files in Index  │                 │
│  │    1,247        │  │   8,542,391     │                 │
│  └─────────────────┘  └─────────────────┘                 │
│                                                             │
│  ┌─────────────────┐  ┌─────────────────┐                 │
│  │ Total Processed │  │ Memory Usage    │                 │
│  │   45,892        │  │    14.2 GB      │                 │
│  └─────────────────┘  └─────────────────┘                 │
│                                                             │
│  Performance Graphs:                                       │
│  ┌─────────────────────────────────────────────────────┐   │
│  │ Files/sec:  ████████████████▓▓▓▓ 15.2K/s           │   │
│  │ Dirs/sec:   ██████████▓▓▓▓▓▓▓▓▓▓ 245/s             │   │
│  │ Memory:     ████████████████████ 78% (↗)            │   │
│  │ CPU:        ████████████▓▓▓▓▓▓▓▓ 65% (→)            │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                             │
│  Throttling Status:                                        │
│  ┌─────────────────────────────────────────────────────┐   │
│  │ Files:      [NORMAL]  8.5M/10M (85%)               │   │
│  │ Memory:     [NORMAL]  78%/80%                       │   │
│  │ Goroutines: [NORMAL]  45K/50K (90%)                │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### Log Analysis Patterns

```
┌─────────────────────────────────────────────────────────────┐
│                     Log Analysis                            │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  Performance Indicators:                                    │
│  ┌─────────────────────────────────────────────────────┐   │
│  │ [INFO] Active Dirs: 1247 | Wait: 23 | Files: 8.5M  │   │
│  │ [INFO] Throughput: 15.2K files/sec, 245 dirs/sec   │   │
│  │ [INFO] Memory utilization: 78% (14.2GB/18GB)       │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                             │
│  Throttling Events:                                         │
│  ┌─────────────────────────────────────────────────────┐   │
│  │ [WARN] FILES: Engaging throttle at 100.0%          │   │
│  │ [INFO] THROTTLE ENGAGED: FILES, MEMORY             │   │
│  │ [INFO] FILES: Releasing throttle at 85.0%          │   │
│  │ [INFO] THROTTLE RELEASED: All resources normal     │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                             │
│  Optimization Events:                                       │
│  ┌─────────────────────────────────────────────────────┐   │
│  │ [INFO] ADJUSTMENT: UNDERUTILIZED - 6M -> 7.8M      │   │
│  │ [INFO] CTime optimization: Skipped 1,247 dirs      │   │
│  │ [INFO] Directory processing time: 2.4s avg         │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                             │
│  Error Patterns:                                           │
│  ┌─────────────────────────────────────────────────────┐   │
│  │ [WARN] Expected error (404): /path/not/found        │   │
│  │ [ERROR] Traversal failed: /problem/dir              │   │
│  │ [WARN] High contention: 45 dirs waiting             │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

## Diagrams

### System Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              SyncOrchestrator System                            │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  ┌─────────────┐    ┌─────────────────────────────────────┐    ┌─────────────┐ │
│  │   Source    │    │                                     │    │Destination  │ │
│  │  Location   │    │         Control Layer              │    │  Location   │ │
│  │             │    │                                     │    │             │ │
│  │ ┌─────────┐ │    │  ┌─────────────┐ ┌─────────────┐   │    │ ┌─────────┐ │ │
│  │ │Local/S3/│ │    │  │CustomSync   │ │Orchestrator │   │    │ │ Blob/   │ │ │
│  │ │  NFS    │ │    │  │Handler      │ │ Options     │   │    │ │ Files   │ │ │
│  │ └─────────┘ │    │  └─────────────┘ └─────────────┘   │    │ └─────────┘ │ │
│  └─────────────┘    └─────────────────────────────────────┘    └─────────────┘ │
│         │                              │                              │         │
│         │                              ▼                              │         │
│  ┌─────────────────────────────────────────────────────────────────────────────┐ │
│  │                           Processing Layer                                  │ │
│  │                                                                             │ │
│  │  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐ │ │
│  │  │   Sync      │    │    Dir      │    │   Stats     │    │   Object    │ │ │
│  │  │Enumerator   │    │ Semaphore   │    │  Monitor    │    │  Indexer    │ │ │
│  │  │             │    │             │    │             │    │             │ │ │
│  │  │┌─────────┐  │    │┌─────────┐  │    │┌─────────┐  │    │┌─────────┐  │ │ │
│  │  ││Primary  │  │    ││Throttle │  │    ││Dynamic  │  │    ││Temp     │  │ │ │
│  │  ││Traverser│  │    ││Control  │  │    ││Limits   │  │    ││Storage  │  │ │ │
│  │  │└─────────┘  │    │└─────────┘  │    │└─────────┘  │    │└─────────┘  │ │ │
│  │  │┌─────────┐  │    │┌─────────┐  │    │┌─────────┐  │    │┌─────────┐  │ │ │
│  │  ││Secondary│  │    ││Resource │  │    ││Trend    │  │    ││Cleanup  │  │ │ │
│  │  ││Traverser│  │    ││Monitor  │  │    ││Analysis │  │    ││Manager  │  │ │ │
│  │  │└─────────┘  │    │└─────────┘  │    │└─────────┘  │    │└─────────┘  │ │ │
│  │  └─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘ │ │
│  └─────────────────────────────────────────────────────────────────────────────┘ │
│                                        │                                         │
│                                        ▼                                         │
│  ┌─────────────────────────────────────────────────────────────────────────────┐ │
│  │                            Execution Layer                                  │ │
│  │                                                                             │ │
│  │     ┌─────────────┐      ┌─────────────┐      ┌─────────────┐             │ │
│  │     │SyncTraverser│      │SyncTraverser│      │SyncTraverser│    ...      │ │
│  │     │   (Dir A)   │      │   (Dir B)   │      │   (Dir N)   │             │ │
│  │     │             │      │             │      │             │             │ │
│  │     │┌─────────┐  │      │┌─────────┐  │      │┌─────────┐  │             │ │
│  │     ││Source   │  │      ││Source   │  │      ││Source   │  │             │ │
│  │     ││Enum     │  │      ││Enum     │  │      ││Enum     │  │             │ │
│  │     │└─────────┘  │      │└─────────┘  │      │└─────────┘  │             │ │
│  │     │┌─────────┐  │      │┌─────────┐  │      │┌─────────┐  │             │ │
│  │     ││Dest     │  │      ││Dest     │  │      ││Dest     │  │             │ │
│  │     ││Compare  │  │      ││Compare  │  │      ││Compare  │  │             │ │
│  │     │└─────────┘  │      │└─────────┘  │      │└─────────┘  │             │ │
│  │     │┌─────────┐  │      │┌─────────┐  │      │┌─────────┐  │             │ │
│  │     ││Transfer │  │      ││Transfer │  │      ││Transfer │  │             │ │
│  │     ││Schedule │  │      ││Schedule │  │      ││Schedule │  │             │ │
│  │     │└─────────┘  │      │└─────────┘  │      │└─────────┘  │             │ │
│  │     └─────────────┘      └─────────────┘      └─────────────┘             │ │
│  └─────────────────────────────────────────────────────────────────────────────┘ │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### Data Flow with Object Lifecycle

```
Object Lifecycle in SyncOrchestrator:

┌─────────────────────────────────────────────────────────────────────────────────┐
│                                Object Lifecycle                                 │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  Phase 1: DISCOVERY                                                             │
│  ┌─────────────────┐              ┌─────────────────┐                          │
│  │   Source File   │              │   Dest File     │                          │
│  │   "file1.txt"   │              │   "file1.txt"   │                          │
│  │                 │              │                 │                          │
│  │ ┌─────────────┐ │              │ ┌─────────────┐ │                          │
│  │ │Size: 1024   │ │              │ │Size: 1024   │ │                          │
│  │ │Modified:    │ │              │ │Modified:    │ │                          │
│  │ │ 2024-01-15  │ │              │ │ 2024-01-14  │ │                          │
│  │ │CTime:       │ │              │ │CTime:       │ │                          │
│  │ │ 2024-01-15  │ │              │ │ 2024-01-14  │ │                          │
│  │ └─────────────┘ │              │ └─────────────┘ │                          │
│  └─────────────────┘              └─────────────────┘                          │
│           │                                │                                    │
│           ▼                                ▼                                    │
│  ┌─────────────────┐              ┌─────────────────┐                          │
│  │   processor()   │              │ customComparator│                          │
│  │                 │              │      ()         │                          │
│  │ Creates:        │              │                 │                          │
│  │ StoredObject    │              │ Compares with   │                          │
│  │                 │              │ Indexed Object  │                          │
│  └─────────────────┘              └─────────────────┘                          │
│           │                                │                                    │
│           ▼                                ▼                                    │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │                        Object Indexer                                  │   │
│  │                                                                         │   │
│  │  Key: "dir1/file1.txt"                                                 │   │
│  │  Value: StoredObject {                                                 │   │
│  │    relativePath: "dir1/file1.txt"                                      │   │
│  │    size: 1024                                                          │   │
│  │    lastModified: 2024-01-15                                            │   │
│  │    changeTime: 2024-01-15                                              │   │
│  │    entityType: File                                                    │   │
│  │    transferQueued: false                                               │   │
│  │  }                                                                     │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                        │                                        │
│  Phase 2: COMPARISON & DECISION                                                 │
│                                        ▼                                        │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │                    Object Comparator                                    │   │
│  │                                                                         │   │
│  │  Decision Logic:                                                        │   │
│  │  • Source newer than destination? ✓ (2024-01-15 > 2024-01-14)         │   │
│  │  • Size different? ✗ (1024 == 1024)                                    │   │
│  │  • Content hash different? [Check if enabled]                          │   │
│  │                                                                         │   │
│  │  Result: TRANSFER_REQUIRED                                              │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                        │                                        │
│  Phase 3: SCHEDULING                                                            │
│                                        ▼                                        │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │                  Transfer Scheduler                                     │   │
│  │                                                                         │   │
│  │  • Queue transfer job for "dir1/file1.txt"                             │   │
│  │  • Mark object as transferQueued: true                                 │   │
│  │  • Add to copy transfer processor                                      │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                        │                                        │
│  Phase 4: CLEANUP                                                              │
│                                        ▼                                        │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │                   Memory Cleanup                                        │   │
│  │                                                                         │   │
│  │  • Remove "dir1/file1.txt" from Object Indexer                         │   │
│  │  • Decrement totalFilesInIndexer counter                               │   │
│  │  • Free memory for processed object                                    │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### Concurrent Processing Model

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                        Concurrent Processing Model                              │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  Directory Tree:                   Parallel Processing:                        │
│                                                                                 │
│        Root                        ┌─────────────────────────────────────────┐ │
│       /  |  \                      │          Goroutine Pool                 │ │
│      A   B   C                     │                                         │ │
│     /|  /|\  |\                    │  ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐      │ │
│    A1A2 B1B2B3 C1C2               │  │ G1  │ │ G2  │ │ G3  │ │ G4  │ ...  │ │
│                                    │  └─────┘ └─────┘ └─────┘ └─────┘      │ │
│  Processing Order:                 │     │      │      │      │           │ │
│  Time T0: Root                     │     ▼      ▼      ▼      ▼           │ │
│  Time T1: A, B, C (parallel)       │  ┌─────────────────────────────────┐   │ │
│  Time T2: A1,A2,B1,B2,B3,C1,C2     │  │      Directory Semaphore       │   │ │
│          (parallel, limited)       │  │                                 │   │ │
│                                    │  │   Available: [██████░░░░] 6/10  │   │ │
│  Concurrency Control:              │  │   Waiting:   [Dir][Dir][Dir]    │   │ │
│  • Semaphore limits active dirs    │  └─────────────────────────────────┘   │ │
│  • Throttling based on resources   │                                         │ │
│  • Memory-aware scheduling          │                                         │ │
│                                    └─────────────────────────────────────────┘ │
│                                                                                 │
│  Processing Flow per Directory:                                                 │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  1. Acquire Semaphore                                                   │   │
│  │     │                                                                   │   │
│  │     ▼                                                                   │   │
│  │  2. Create SyncTraverser                                                │   │
│  │     │                                                                   │   │
│  │     ▼                                                                   │   │
│  │  3. Source Enumeration ────────┐                                       │   │
│  │     │                          │                                       │   │
│  │     ▼                          ▼                                       │   │
│  │  4. Destination Enumeration    Store in Object Indexer                 │   │
│  │     │                          │                                       │   │
│  │     ▼                          ▼                                       │   │
│  │  5. Comparison & Scheduling    Process Objects                         │   │
│  │     │                          │                                       │   │
│  │     ▼                          ▼                                       │   │
│  │  6. Cleanup & Memory Release   Remove from Indexer                     │   │
│  │     │                                                                   │   │
│  │     ▼                                                                   │   │
│  │  7. Enqueue Subdirectories                                             │   │
│  │     │                                                                   │   │
│  │     ▼                                                                   │   │
│  │  8. Release Semaphore                                                  │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

This architecture document provides a comprehensive overview of the SyncOrchestrator system, covering all major components, their interactions, and the sophisticated resource management strategies employed. The system is designed for high-performance, large-scale synchronization operations with intelligent throttling and optimization capabilities.
