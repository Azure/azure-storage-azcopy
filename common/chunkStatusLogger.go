
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
	"path/filepath"
	"time"
)

type ChunkID struct {
	Name string
	OffsetInFile int64
}

var EWaitReason = WaitReason(0)

type WaitReason string

func (WaitReason) RAMToSchedule() WaitReason    { return WaitReason("RAM") }
func (WaitReason) WorkerGR() WaitReason         { return WaitReason("GR") }
func (WaitReason) HeaderResponse() WaitReason   { return WaitReason("Head") }
func (WaitReason) BodyResponse() WaitReason     { return WaitReason("Body") }
func (WaitReason) BodyReReadDueToMem() WaitReason     { return WaitReason("BodyReRead-LowRam") }
func (WaitReason) BodyReReadDueToSpeed() WaitReason   { return WaitReason("BodyReRead-TooSlow") }
func (WaitReason) WriterChannel() WaitReason 	{ return WaitReason("Writer") }
func (WaitReason) PriorChunk() WaitReason 		{ return WaitReason("Prior") }
func (WaitReason) Disk() WaitReason 			{ return WaitReason("Disk") }
func (WaitReason) ChunkDone() WaitReason 		{ return WaitReason("Done") }
func (WaitReason) Cancelled() WaitReason 		{ return WaitReason("Cancelled") }

func (wr WaitReason) String() string{
	return string(wr)   // avoiding reflection here, for speed, since will be called a lot
}

// TODO: stop this using globals
var cw chan chunkWait
const chunkLogEnabled = true  // TODO make this controllable by command line parameter

type chunkWait struct {
	ChunkID
	reason WaitReason
	waitStart time.Time
}


func LogChunkWaitReason(id ChunkID, reason WaitReason){
	if !chunkLogEnabled {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			// recover panic from writing to closed channel
			// May happen in early exit of app, when StopChunkWaitLogger is called before last call to this routine
		}
	}()

	cw <- chunkWait{ChunkID: id, reason: reason, waitStart:time.Now() }
}

func StartChunkWaitLogger(azCopyLogFolder string){
	if !chunkLogEnabled {
		return
	}
	cw = make(chan chunkWait, 1000000)
	go chunkWaitLogger(azCopyLogFolder)
}

func StopChunkWaitLogger(){
	if !chunkLogEnabled {
		return
	}
	close(cw)
	for len(cw) > 0 {
		time.Sleep(time.Second)
	}
}

func chunkWaitLogger(azCopyLogFolder string){
	f, err := os.Create(filepath.Join(azCopyLogFolder, "chunkwaitlog.csv"))  // only saves the latest run, at present...
	if err != nil {
		panic(err.Error())
	}
	defer func () { _ = f.Close()}()

	w := bufio.NewWriter(f)
	defer func () { _ = w.Flush()}()

	_,_ = w.WriteString("Name,Offset,State,StateStartTime\n")

	for x := range cw {
		_,_ = w.WriteString(fmt.Sprintf("%s,%d,%s,%s\n", x.Name, x.OffsetInFile, x.reason, x.waitStart))
	}
}

/* LinqPad query used to analyze/visualize the CSV as is follows:

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