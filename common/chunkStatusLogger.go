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
	"time"
)

type ChunkID struct {
	Name         string
	OffsetInFile int64
}

var EWaitReason = WaitReason(0)

type WaitReason string

func (WaitReason) RAMToSchedule() WaitReason        { return WaitReason("RAM") }  // waiting for enough RAM to schedule the chunk
func (WaitReason) WorkerGR() WaitReason             { return WaitReason("GR") }   // waiting for a goroutine to start running our chunkfunc
func (WaitReason) HeaderResponse() WaitReason       { return WaitReason("Head") } // waiting to finish downloading the HEAD
func (WaitReason) BodyResponse() WaitReason         { return WaitReason("Body") } // waiting to finish downloading the BODY
func (WaitReason) BodyReReadDueToMem() WaitReason   { return WaitReason("BodyReRead-LowRam") }  //waiting to re-read the body after a forced-retry due to low RAM
func (WaitReason) BodyReReadDueToSpeed() WaitReason { return WaitReason("BodyReRead-TooSlow") } // waiting to re-read the body after a forced-retry due to a slow chunk read (without low RAM)
func (WaitReason) WriterChannel() WaitReason        { return WaitReason("Writer") } // waiting for the writer routine, in chunkedFileWriter, to pick up this chunk
func (WaitReason) PriorChunk() WaitReason           { return WaitReason("Prior") }  // waiting on a prior chunk to arrive (before this one can be saved)
func (WaitReason) Disk() WaitReason                 { return WaitReason("Disk") }   // waiting on disk write to complete
func (WaitReason) ChunkDone() WaitReason            { return WaitReason("Done") }   // not waiting on anything. Chunk is done.
func (WaitReason) Cancelled() WaitReason            { return WaitReason("Cancelled") } // transfer was cancelled.  All chunks end with either Done or Cancelled.

func (wr WaitReason) String() string {
	return string(wr) // avoiding reflection here, for speed, since will be called a lot
}

type ChunkStatusLogger interface {
	LogChunkStatus(id ChunkID, reason WaitReason)
}

type ChunkStatusLoggerCloser interface {
	ChunkStatusLogger
	CloseLog()
}

type chunkStatusLogger struct {
	enabled        bool
	unsavedEntries chan chunkWaitState
}

func NewChunkStatusLogger(jobID JobID, logFileFolder string, enable bool) ChunkStatusLoggerCloser {
	logger := &chunkStatusLogger{
		enabled:        enable,
		unsavedEntries: make(chan chunkWaitState, 1000000),
	}
	if enable {
		chunkLogPath := path.Join(logFileFolder, jobID.String()+"-chunks.log") // its a CSV, but using log extension for consistency with other files in the directory
		go logger.main(chunkLogPath)
	}
	return logger
}

type chunkWaitState struct {
	ChunkID
	reason    WaitReason
	waitStart time.Time
}

func (csl *chunkStatusLogger) LogChunkStatus(id ChunkID, reason WaitReason) {
	if !csl.enabled {
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
	if !csl.enabled {
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
