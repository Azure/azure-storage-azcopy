
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

func (WaitReason) RAMToSchedule() WaitReason   { return WaitReason("RAM") }
func (WaitReason) WorkerGR() WaitReason        { return WaitReason("GR") }
func (WaitReason) HeaderResponse() WaitReason  { return WaitReason("Head") }
func (WaitReason) BodyResponse() WaitReason    { return WaitReason("Body") }
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

type chunkWait struct {
	ChunkID
	reason WaitReason
	waitStart time.Time
}


func LogChunkWaitReason(id ChunkID, reason WaitReason){
	defer func() {
		if r := recover(); r != nil {
			// recover panic from writing to closed channel
			// May happen in early exit of app, when StopChunkWaitLogger is called before last call to this routine
		}
	}()

	cw <- chunkWait{ChunkID: id, reason: reason, waitStart:time.Now() }
}

func StartChunkWaitLogger(azCopyLogFolder string){
	cw = make(chan chunkWait, 1000000)
	go chunkWaitLogger(azCopyLogFolder)
}

func StopChunkWaitLogger(){
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

var data = chunkwaitlog_bigFreeze;

const int assumedMBPerChunk = 8;

data.Max(c => c.StateStartTime).Dump();

var allChunkIds = data.Select(c => new {c.Name, c.Offset}).Distinct();
var doneChunkIds = data.Where(c => c.State == "Done" || c.State == "Cancelled").Select(c => new {c.Name, c.Offset}).Distinct();
var unfinishedChunkIds = allChunkIds.Except(doneChunkIds).ToList();

unfinishedChunkIds.Count().Dump();

var rawDataWithUnfinishedChunks = (from c in data
								  join u in unfinishedChunkIds on new {c.Name, c.Offset} equals u
								  select c);

var unfinishedGrouped = rawDataWithUnfinishedChunks.GroupBy(c => new {c.Name, c.Offset});

var statesForOffset = unfinishedGrouped.Select(g => new
{
	g.Key,
	States = g.Select(x => new { x.State, x.StateStartTime }).OrderBy(x => x.StateStartTime)
}).ToList();

var countsForFile = statesForOffset.GroupBy(sfo => sfo.Key.Name).Select(g => new { Name = g.Key, Count = g.Count()}).ToList();

var final = (from sfo in statesForOffset
	        join cff in countsForFile on sfo.Key.Name equals cff.Name
			select new {
				ChunkID = sfo.Key,
				LiveChunkCountForFile = cff.Count,
				ChunkStates = sfo.States
			})
.GroupBy(f => f.ChunkID.Name)
.Select(g => new {Name = g.Key, CountForFile = g.First().LiveChunkCountForFile, Chuncks = g.Select(x => new {OffsetNumber = (int)(long.Parse(x.ChunkID.Offset)/(assumedMBPerChunk*1024*1024)), OffsetValue = x.ChunkID.Offset, States = x.ChunkStates}).OrderBy(x => x.OffsetNumber)})
.OrderBy(x => x.Name);


final.Dump();

 */