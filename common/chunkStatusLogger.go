// Copyright © 2017 Microsoft <wastore@microsoft.com>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package common

import (
	"bufio"
	"fmt"
	"os"
	"path"
	"sync/atomic"
	"time"
)

// Identifies a chunk. Always create with NewChunkID
type ChunkID struct {
	Name         string
	OffsetInFile int64

	// What is this chunk's progress currently waiting on?
	// Must be a pointer, because the ChunkID itself is a struct.
	// When chunkID is passed around, copies are made,
	// but because this is a pointer, all will point to the same
	// value for waitReasonIndex (so when we change it, all will see the change)
	waitReasonIndex *int32
}

func NewChunkID(name string, offsetInFile int64) ChunkID {
	dummyWaitReasonIndex := int32(0)
	return ChunkID{
		Name:            name,
		OffsetInFile:    offsetInFile,
		waitReasonIndex: &dummyWaitReasonIndex, // must initialize, so don't get nil pointer on usage
	}
}

var EWaitReason = WaitReason{0, ""}

type WaitReason struct {
	index int32
	Name  string
}

// Upload chunks go through these states:
// RAM
// GR
// Body
// Disk
// Done/Cancelled

// Download chunks go through a superset, as follows
// RAM
// GR
// Head (can easily separate out head/body for uploads)
// Body
// (possibly) BodyReRead-*
// Writer
// Prior
// Disk
// Done/Cancelled

// Head (below) has index between GB and Body, just so the ordering is numerical ascending during typical chunk lifetime for both upload and download
func (WaitReason) Nothing() WaitReason              { return WaitReason{0, "Nothing"} }            // not waiting for anything
func (WaitReason) RAMToSchedule() WaitReason        { return WaitReason{1, "RAM"} }                // waiting for enough RAM to schedule the chunk
func (WaitReason) WorkerGR() WaitReason             { return WaitReason{2, "GR"} }                 // waiting for a goroutine to start running our chunkfunc
func (WaitReason) HeaderResponse() WaitReason       { return WaitReason{3, "Head"} }               // waiting to finish downloading the HEAD
func (WaitReason) Body() WaitReason                 { return WaitReason{4, "Body"} }               // waiting to finish sending/receiving the BODY
func (WaitReason) BodyReReadDueToMem() WaitReason   { return WaitReason{5, "BodyReRead-LowRam"} }  //waiting to re-read the body after a forced-retry due to low RAM
func (WaitReason) BodyReReadDueToSpeed() WaitReason { return WaitReason{6, "BodyReRead-TooSlow"} } // waiting to re-read the body after a forced-retry due to a slow chunk read (without low RAM)
func (WaitReason) WriterChannel() WaitReason        { return WaitReason{7, "Writer"} }             // waiting for the writer routine, in chunkedFileWriter, to pick up this chunk
func (WaitReason) PriorChunk() WaitReason           { return WaitReason{8, "Prior"} }              // waiting on a prior chunk to arrive (before this one can be saved)
func (WaitReason) Disk() WaitReason                 { return WaitReason{9, "Disk"} }               // waiting on disk read/write to complete
func (WaitReason) ChunkDone() WaitReason            { return WaitReason{10, "Done"} }              // not waiting on anything. Chunk is done.
func (WaitReason) Cancelled() WaitReason            { return WaitReason{11, "Cancelled"} }         // transfer was cancelled.  All chunks end with either Done or Cancelled.

func (wr WaitReason) String() string {
	return string(wr.Name) // avoiding reflection here, for speed, since will be called a lot
}

type ChunkStatusLogger interface {
	LogChunkStatus(id ChunkID, reason WaitReason)
}

type chunkStatusCount struct {
	WaitReason WaitReason
	Count      int64
}

type ChunkStatusLoggerCloser interface {
	ChunkStatusLogger
	GetCounts() []chunkStatusCount
	CloseLog()
}

type chunkStatusLogger struct {
	counts         []int64
	outputEnabled  bool
	unsavedEntries chan chunkWaitState
}

func NewChunkStatusLogger(jobID JobID, logFileFolder string, enableOutput bool) ChunkStatusLoggerCloser {
	logger := &chunkStatusLogger{
		counts:         make([]int64, numWaitReasons()),
		outputEnabled:  enableOutput,
		unsavedEntries: make(chan chunkWaitState, 1000000),
	}
	if enableOutput {
		chunkLogPath := path.Join(logFileFolder, jobID.String()+"-chunks.log") // its a CSV, but using log extension for consistency with other files in the directory
		go logger.main(chunkLogPath)
	}
	return logger
}

func numWaitReasons() int32 {
	return EWaitReason.Cancelled().index + 1 // assume this is the last wait reason
}

type chunkWaitState struct {
	ChunkID
	reason    WaitReason
	waitStart time.Time
}

// We maintain running totals of how many chunks are in each state.
// To do so, we must determine the new state (which is simply a parameter) and the old state.
// We obtain and track the old state within the chunkID itself. The alternative, of having a threadsafe
// map in the chunkStatusLogger, to track and look up the states, is considered a risk for performance.
func (csl *chunkStatusLogger) countStateTransition(id ChunkID, newReason WaitReason) {

	// Flip the chunk's state to indicate the new thing that it's waiting for now
	oldReasonIndex := atomic.SwapInt32(id.waitReasonIndex, newReason.index)

	// Update the counts
	// There's no need to lock the array itself. Instead just do atomic operations on the contents.
	// (See https://groups.google.com/forum/#!topic/Golang-nuts/Ud4Dqin2Shc)
	if oldReasonIndex > 0 && oldReasonIndex < int32(len(csl.counts)) {
		atomic.AddInt64(&csl.counts[oldReasonIndex], -1)
	}
	if newReason.index < int32(len(csl.counts)) {
		atomic.AddInt64(&csl.counts[newReason.index], 1)
	}
}

// Gets the current counts of chunks in each wait state
// Intended for performance diagnostics and reporting
func (csl *chunkStatusLogger) GetCounts() []chunkStatusCount {
	// get list of all the reasons we want to output
	// Rare and not-useful ones are excluded
	allReasons := []WaitReason{
		//EWaitReason.Nothing(),
		EWaitReason.RAMToSchedule(),
		EWaitReason.WorkerGR(),
		EWaitReason.HeaderResponse(),
		EWaitReason.Body(),
		//EWaitReason.BodyReReadDueToMem(),
		//EWaitReason.BodyReReadDueToSpeed(),
		EWaitReason.WriterChannel(),
		EWaitReason.PriorChunk(),
		EWaitReason.Disk(),
		EWaitReason.ChunkDone(),
		//EWaitReason.Cancelled(),
	}
	result := make([]chunkStatusCount, len(allReasons))
	for i, reason := range allReasons {
		result[i] = chunkStatusCount{reason, atomic.LoadInt64(&csl.counts[reason.index])}
	}
	return result
}

func (csl *chunkStatusLogger) LogChunkStatus(id ChunkID, reason WaitReason) {
	// always update the in-memory stats, even if output is disabled
	csl.countStateTransition(id, reason)

	if !csl.outputEnabled {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			// recover panic from writing to closed channel
			// May happen in early exit of app, when Close is called before last call to this routine
		}
	}()

	csl.unsavedEntries <- chunkWaitState{ChunkID: id, reason: reason, waitStart: time.Now()}
}

func (csl *chunkStatusLogger) CloseLog() {
	if !csl.outputEnabled {
		return
	}
	close(csl.unsavedEntries)
	for len(csl.unsavedEntries) > 0 {
		time.Sleep(100 * time.Millisecond)
	}
}

func (csl *chunkStatusLogger) main(chunkLogPath string) {
	f, err := os.Create(chunkLogPath)
	if err != nil {
		panic(err.Error())
	}
	defer func() { _ = f.Close() }()

	w := bufio.NewWriter(f)
	defer func() { _ = w.Flush() }()

	_, _ = w.WriteString("Name,Offset,State,StateStartTime\n")

	for x := range csl.unsavedEntries {
		_, _ = w.WriteString(fmt.Sprintf("%s,%d,%s,%s\n", x.Name, x.OffsetInFile, x.reason, x.waitStart))
	}
}

/* LinqPad query used to analyze/visualize the CSV as is follows:
   Needs CSV driver for LinqPad to open the CSV - e.g. https://github.com/dobrou/CsvLINQPadDriver

var data = chunkwaitlog_noForcedRetries;

const int assumedMBPerChunk = 8;

DateTime? ParseStart(string s)
{
	const string format = "yyyy-MM-dd HH:mm:ss.fff";
	var s2 = s.Substring(0, format.Length);
	try
	{
		return DateTime.ParseExact(s2, format, CultureInfo.CurrentCulture);
	}
	catch
	{
		return null;
	}
}

// convert to real datetime (default unparseable ones to a fixed value, simply to avoid needing to deal with nulls below, and because all valid records should be parseable. Only exception would be something partially written a time of a crash)
var parsed = data.Select(d => new { d.Name, d.Offset, d.State, StateStartTime = ParseStart(d.StateStartTime) ?? DateTime.MaxValue}).ToList();

var grouped = parsed.GroupBy(c => new {c.Name, c.Offset});

var statesForOffset = grouped.Select(g => new
{
	g.Key,
	States = g.Select(x => new { x.State, x.StateStartTime }).OrderBy(x => x.StateStartTime).ToList()
}).ToList();

var withStatesOfInterest = (from sfo in statesForOffset
let states = sfo.States
let lastIndex = states.Count - 1
let statesWithDurations = states.Select((s, i) => new{ s.State, s.StateStartTime, Duration = ( i == lastIndex ? new TimeSpan(0) : states[i+1].StateStartTime - s.StateStartTime) })
let hasLongBodyRead = statesWithDurations.Any(x => (x.State == "Body" && x.Duration.TotalSeconds > 30)  // detect slowness in tests where we turn off the forced restarts
|| x.State.StartsWith("BodyReRead"))                       // detect slowness where we solved it by a forced restart
select new {sfo.Key, States = statesWithDurations, HasLongBodyRead = hasLongBodyRead})
.ToList();

var filesWithLongBodyReads = withStatesOfInterest.Where(x => x.HasLongBodyRead).Select(x => x.Key.Name).Distinct().ToList();

filesWithLongBodyReads.Count().Dump("Number of files with at least one long chunk read");

var final = (from wsi in withStatesOfInterest
join f in filesWithLongBodyReads on wsi.Key.Name equals f
select new
{
ChunkID = wsi.Key,
wsi.HasLongBodyRead,
wsi.States

})
.GroupBy(f => f.ChunkID.Name)
.Select(g => new {
Name = g.Key,
Chunks = g.Select(x => new {
OffsetNumber = (int)(long.Parse(x.ChunkID.Offset)/(assumedMBPerChunk*1024*1024)),
OffsetValue = x.HasLongBodyRead ? Util.Highlight(x.ChunkID.Offset) : x.ChunkID.Offset, States = x.States}
).OrderBy(x => x.OffsetNumber)
})
.OrderBy(x => x.Name);

final.Dump();

*/
