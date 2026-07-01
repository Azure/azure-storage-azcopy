// Copyright © Microsoft <wastore@microsoft.com>
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

package ste

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBuildSourceGridPlan_AssignsContiguousOffsets(t *testing.T) {
	a := assert.New(t)

	plan, err := buildSourceGridPlan([]rawCommittedBlock{
		{Name: "b0", Size: 100},
		{Name: "b1", Size: 250},
		{Name: "b2", Size: 50},
	})
	a.NoError(err)
	a.EqualValues(400, plan.TotalSize)
	a.Len(plan.Blocks, 3)

	a.EqualValues(0, plan.Blocks[0].Offset)
	a.EqualValues(100, plan.Blocks[0].Size)
	a.Equal("b0", plan.Blocks[0].SrcBlockName)

	a.EqualValues(100, plan.Blocks[1].Offset)
	a.EqualValues(250, plan.Blocks[1].Size)

	a.EqualValues(350, plan.Blocks[2].Offset)
	a.EqualValues(50, plan.Blocks[2].Size)
}

func TestBuildSourceGridPlan_Empty(t *testing.T) {
	a := assert.New(t)

	plan, err := buildSourceGridPlan(nil)
	a.NoError(err)
	a.EqualValues(0, plan.TotalSize)
	a.Empty(plan.Blocks)
}

func TestBuildSourceGridPlan_NegativeSizeIsError(t *testing.T) {
	a := assert.New(t)

	_, err := buildSourceGridPlan([]rawCommittedBlock{
		{Name: "ok", Size: 10},
		{Name: "bad", Size: -1},
	})
	a.Error(err)
}

func TestAlignment_UniformBlocksAllMatch(t *testing.T) {
	a := assert.New(t)

	// Four blocks of exactly the uniform chunk size -> every block matches a uniform chunk.
	plan, err := buildSourceGridPlan([]rawCommittedBlock{
		{Name: "b0", Size: 1024},
		{Name: "b1", Size: 1024},
		{Name: "b2", Size: 1024},
		{Name: "b3", Size: 1024},
	})
	a.NoError(err)

	stats := plan.AlignmentToUniformGrid(1024)
	a.Equal(4, stats.SourceBlockCount)
	a.EqualValues(4, stats.UniformChunkCount)
	a.Equal(4, stats.AlignedStarts)
	a.Equal(4, stats.ExactChunkMatches)
}

func TestAlignment_FinalShortBlockMatches(t *testing.T) {
	a := assert.New(t)

	// Two full chunks plus a short final block that exactly equals the final (short) uniform chunk.
	plan, err := buildSourceGridPlan([]rawCommittedBlock{
		{Name: "b0", Size: 1024},
		{Name: "b1", Size: 1024},
		{Name: "b2", Size: 300},
	})
	a.NoError(err)

	stats := plan.AlignmentToUniformGrid(1024)
	a.EqualValues(2348, stats.TotalSize)
	a.EqualValues(3, stats.UniformChunkCount) // ceil(2348/1024)
	a.Equal(3, stats.AlignedStarts)
	a.Equal(3, stats.ExactChunkMatches) // final 300-byte block == final 300-byte uniform chunk
}

func TestAlignment_MisalignedBlocksDoNotMatch(t *testing.T) {
	a := assert.New(t)

	// Blocks half the uniform size: starts alternate aligned/unaligned, and none span a whole chunk.
	plan, err := buildSourceGridPlan([]rawCommittedBlock{
		{Name: "b0", Size: 512},
		{Name: "b1", Size: 512},
		{Name: "b2", Size: 512},
		{Name: "b3", Size: 512},
	})
	a.NoError(err)

	stats := plan.AlignmentToUniformGrid(1024)
	a.Equal(4, stats.SourceBlockCount)
	// b0@0 and b2@1024 start on a 1024 boundary; b1@512 and b3@1536 do not.
	a.Equal(2, stats.AlignedStarts)
	// No block is a full 1024 chunk (and the file ends exactly on a boundary, so no short final chunk).
	a.Equal(0, stats.ExactChunkMatches)
}

func TestAlignment_SingleBlockWholeFile(t *testing.T) {
	a := assert.New(t)

	// One committed block holding the whole blob, smaller than the chunk size -> it equals the single
	// (short) uniform chunk that spans the whole file.
	plan, err := buildSourceGridPlan([]rawCommittedBlock{
		{Name: "only", Size: 4000},
	})
	a.NoError(err)

	stats := plan.AlignmentToUniformGrid(8 * 1024 * 1024)
	a.Equal(1, stats.SourceBlockCount)
	a.EqualValues(1, stats.UniformChunkCount)
	a.Equal(1, stats.AlignedStarts)
	a.Equal(1, stats.ExactChunkMatches)
}

func TestAlignment_NonPositiveChunkSizeIsSafe(t *testing.T) {
	a := assert.New(t)

	plan, err := buildSourceGridPlan([]rawCommittedBlock{
		{Name: "b0", Size: 100},
		{Name: "b1", Size: 100},
	})
	a.NoError(err)

	stats := plan.AlignmentToUniformGrid(0)
	a.Equal(2, stats.SourceBlockCount)
	a.EqualValues(0, stats.UniformChunkCount)
	a.Equal(0, stats.AlignedStarts)
	a.Equal(0, stats.ExactChunkMatches)
}

func TestGridAlignmentStats_StringMentionsPercentage(t *testing.T) {
	a := assert.New(t)

	plan, err := buildSourceGridPlan([]rawCommittedBlock{
		{Name: "b0", Size: 1024},
		{Name: "b1", Size: 512},
	})
	a.NoError(err)

	s := plan.AlignmentToUniformGrid(1024).String()
	a.Contains(s, "dedupe-observe:")
	a.Contains(s, "exactChunkMatches=")
}
