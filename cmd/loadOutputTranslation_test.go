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

package cmd

import (
	"strings"

	chk "gopkg.in/check.v1"
)

type clfsTestSuite struct{}

var _ = chk.Suite(&clfsTestSuite{})

func (s *clfsTestSuite) TestParsingInit(c *chk.C) {
	sampleInfo := `AZCOPY-CONFIG:{"compression":"LZ4","new":true,"preserve_hardlinks":false,"version":"1.0.15"}`

	// set up the mock lcm so that we can intercept the parsed message
	mockedLcm := mockedLifecycleManager{infoLog: make(chan string, 50)}
	parser := newClfsExtensionOutputParser(&mockedLcm)

	// invoke the processing func
	parser.processInitialization(sampleInfo)

	// validate the parsed output
	select {
	case msg := <-mockedLcm.infoLog:
		// just check that the version got printed, the other info are less important
		c.Check(strings.Contains(msg, "1.0.15"), chk.Equals, true)
	default:
		// nothing parsed, disappointing
		c.Fail()
	}
}

func (s *clfsTestSuite) TestParsingInfo(c *chk.C) {
	sampleInfo := `AZCOPY-INFO:{"phase":"Init"}`

	// set up the mock lcm so that we can intercept the parsed message
	mockedLcm := mockedLifecycleManager{infoLog: make(chan string, 50)}
	parser := newClfsExtensionOutputParser(&mockedLcm)

	// invoke the processing func
	parser.processInfo(sampleInfo)

	// validate the parsed output
	select {
	case msg := <-mockedLcm.infoLog:
		c.Check(strings.Contains(msg, "Init"), chk.Equals, true)
	default:
		// nothing parsed, disappointing
		c.Fail()
	}
}

func (s *clfsTestSuite) TestParsingError(c *chk.C) {
	sampleError := "2019-10-01 23:27:36,232 ERR './state6' already exists"

	// set up the mock lcm so that we can intercept the parsed message
	mockedLcm := mockedLifecycleManager{infoLog: make(chan string, 50)}
	parser := newClfsExtensionOutputParser(&mockedLcm)

	// invoke the processing func
	parser.processError(sampleError)

	// validate the parsed output
	select {
	case msg := <-mockedLcm.infoLog:
		c.Check(msg, chk.Equals, "'./state6' already exists")
	default:
		// nothing parsed, disappointing
		c.Fail()
	}
}

func (s *clfsTestSuite) TestParsingProgress(c *chk.C) {
	// fabricated numbers to validate parsing
	sampleProgress := `AZCOPY:{"elapsed":12.009643077850342,"files_completed_failed":2,"files_completed_success":1,"files_pending":3,"files_skipped":4,"files_total":5,"pct_complete":6.7,"phase":"Transfer","throughput_Mbps":8.999,"throughput_delta_secs":2.000314235687256,"time_eta":null,"time_report":1569972866.0099542}`

	// set up the mock lcm so that we can intercept the parsed message
	mockedLcm := mockedLifecycleManager{progressLog: make(chan string, 50)}
	parser := newClfsExtensionOutputParser(&mockedLcm)

	// invoke the processing func
	parser.processProgress(sampleProgress)

	// validate the parsed output
	select {
	case msg := <-mockedLcm.progressLog:
		c.Check(msg, chk.Equals, "6.7 %, 1 Done, 2 Failed, 3 Pending, 4 Skipped, 5 Total, Throughput (Mb/s): 8.999")
	default:
		// nothing parsed, disappointing
		c.Fail()
	}
}

func (s *clfsTestSuite) TestParsingSummary(c *chk.C) {
	// fabricated numbers to validate parsing
	sampleProgress := `{"elapsed":199.999,"files_completed_success":10,"files_completed_failed":20,"files_skipped":30,"files_total":40,"final_job_status":"Completed","total_bytes_transferred":500}`

	// set up the mock lcm so that we can intercept the parsed message
	mockedLcm := mockedLifecycleManager{exitLog: make(chan string, 50)}
	parser := newClfsExtensionOutputParser(&mockedLcm)

	// invoke the processing func
	parser.processEndOfJob(sampleProgress)

	// validate the parsed output
	select {
	case msg := <-mockedLcm.exitLog:
		c.Check(msg, chk.Equals, `
Elapsed Time (Minutes): 3.3333
Total Number Of Transfers: 40
Number of Transfers Completed: 10
Number of Transfers Failed: 20
Number of Transfers Skipped: 30
TotalBytesTransferred: 500
Final Job Status: Completed
`)
	default:
		// nothing parsed, disappointing
		c.Fail()
	}
}

// a very crude way of testing our parsing logic
// TODO consider moving this string to a file or somewhere later
const sampleOutput = `2019-10-09 06:55:16,623 INF CLFSLoad version 1.0.15
2019-10-09 06:55:17,561 INF begin phase init
AZCOPY-INFO:{"phase":"Init"}
2019-10-09 06:55:17,686 INF preserve_hardlinks 0
2019-10-09 06:55:17,687 INF compression LZ4 (1)
2019-10-09 06:55:17,824 INF azcopy:
AZCOPY-CONFIG:{"compression":"LZ4","new":true,"preserve_hardlinks":false,"version":"1.0.15"}
2019-10-09 06:55:17,826 INF perform dry run on ./test-data
2019-10-09 06:55:17,830 INF dry run result count=87 GB=1.000718 elapsed=0.003576
2019-10-09 06:55:17,870 INF begin phase transfer
AZCOPY-INFO:{"phase":"Transfer"}
2019-10-09 06:55:17,945 INF transfer 0+0/8 elapsed=0.000000 recent(fps/mbps)=0.0/0.00 cumulative=0.0/0.00 db_queue_len=0/0.000 blobs=0 written=0 (dir/nondir=0/0) (0.0%) GB=0.000000 (0.0%)
AZCOPY:{"elapsed":0.009233713150024414,"files_completed_failed":0,"files_completed_success":0,"files_pending":87,"files_skipped":0,"files_total":87,"pct_complete":0.0,"phase":"Transfer","throughput_Mbps":0.0,"throughput_delta_secs":0.009233713150024414,"time_eta":null,"time_report":1570604117.9448159}
2019-10-09 06:55:19,945 INF transfer 7+0/8 elapsed=2.000123 recent(fps/mbps)=43.0/211.28 cumulative=43.0/211.28 db_queue_len=1/0.008 blobs=151 written=86 (dir/nondir=2/84) (98.9%) GB=0.422593 (42.2%)
AZCOPY:{"elapsed":2.0093564987182617,"files_completed_failed":0,"files_completed_success":86,"files_pending":1,"files_skipped":0,"files_total":87,"pct_complete":42.228963106750896,"phase":"Transfer","throughput_Mbps":1730.833497613517,"throughput_delta_secs":2.0001227855682373,"time_eta":null,"time_report":1570604119.9449387}
2019-10-09 06:55:21,778 INF transfer 8+0/8 elapsed=3.833536 recent(fps/mbps)=0.5/315.33 cumulative=22.7/261.04 db_queue_len=0/0.000 blobs=226 written=87 (dir/nondir=2/85) (100.0%) GB=1.000718 (100.0%)
2019-10-09 06:55:21,806 INF phase transfer complete, elapsed=3.861348
2019-10-09 06:55:21,807 INF transfer 0+8/8 elapsed=3.861348 recent(fps/mbps)=0.5/310.62 cumulative=22.5/259.16 db_queue_len=0/0.000 blobs=226 written=87 (dir/nondir=2/85) (100.0%) GB=1.000718 (100.0%)
stats:
{'check_existing': 2,
 'compress_blob': 226,
 'count_dir': 2,
 'count_nondir': 85,
 'dbc_claim_miss': 23,
 'error_count': 0,
 'fh_regenerate': 0,
 'found_dir': 1,
 'found_nondir': 85,
 'gb_write': 1.0007177144289017,
 'idle': 17,
 'wait': 23,
 'write_blob_bytes': 4658622,
 'write_blob_count': 226,
 'write_blob_fastpath': 0,
 'write_blob_secs': 11.67502760887146,
 'xfer_threads_busy_max': 1}
timers:
{'_count': 8,
 '_elapsed': 3.8199939727783203,
 'blob_compress': '1.223299026 (4.00%)',
 'blob_write': '11.675027609 (38.20%)',
 'dir_generate': '0.192975760 (0.63%)',
 'dir_generate_update_state': '0.000295401 (0.00%)',
 'dir_generate_writeback2': '0.000150204 (0.00%)',
 'idle': '21.273998022 (69.61%)',
 'read_entries': '0.000053167 (0.00%)',
 'read_mt': '0.095147610 (0.31%)',
 'read_st': '0.002938271 (0.01%)',
 'work_item_claim': '0.009269476 (0.03%)',
 'work_item_execute': '9.011045694 (29.49%)',
 'work_item_wait': '21.272152424 (69.61%)',
 'wps.directory_write': '0.073803186 (0.24%)',
 'wps.writer.transfer': '8.418346405 (27.55%)',
 'writing_dir': '0.081565619 (0.27%)',
 'writing_nondir': '8.733993769 (28.58%)',
 'writing_update_state': '0.006951332 (0.02%)'}
DB stats:
{'error_count_entity': 0,
 'flush_count': 6,
 'flush_seconds': 0.08766508102416992,
 'flush_update_tobj_deferred': 1,
 'flush_upsert_tobj_slowpath': 0,
 'snap0_flush_count': 1,
 'snap0_flush_seconds': 0.04831981658935547,
 'snap1_flush_count': 5,
 'snap1_flush_seconds': 0.03934526443481445,
 'snap2_flush_count': 0,
 'snap2_flush_seconds': 0.0}
DB timers:
{'_count': 0,
 '_elapsed': 3.9364240169525146,
 'flush': '0.087665081 (2.23%)',
 'flush_01_compute_upsert': '0.000585079 (0.01%)',
 'flush_02_upsert': '0.015451193 (0.39%)',
 'flush_03_compute_update': '0.011325836 (0.29%)',
 'flush_04_update': '0.004318476 (0.11%)',
 'flush_06_flush': '0.000125408 (0.00%)',
 'flush_07_update_state': '0.003791332 (0.10%)',
 'flush_08_update_state_deferred': '0.000058651 (0.00%)',
 'flush_09_flush': '0.019524574 (0.50%)',
 'flush_10_reap': '0.030148745 (0.77%)',
 'preclaim_more': '0.000038385 (0.00%)'}
2019-10-09 06:55:21,835 INF begin phase finalize
AZCOPY-INFO:{"phase":"Finalize"}
2019-10-09 06:55:22,499 INF all phases complete
2019-10-09 06:55:22,560 INF CLFSLoad succeeded
2019-10-09 06:55:22,569 INF azcopy:
AZCOPY-FINAL:{"elapsed":6.173717021942139,"files_completed_success":87,"files_completed_failed":0,"files_skipped":0,"files_total":87,"final_job_status":"Completed","total_bytes_transferred":1074512464}
2019-10-09 06:55:22,572 INF elapsed=6.174 GB=1.000718 count=87 14.092 files/sec 0.162093 GB/s`

func (s *clfsTestSuite) TestParsingSampleOutput(c *chk.C) {
	// set up the mock lcm so that we can intercept the parsed message
	mockedLcm := mockedLifecycleManager{
		infoLog:     make(chan string, 50),
		exitLog:     make(chan string, 50),
		errorLog:    make(chan string, 50),
		progressLog: make(chan string, 50),
	}

	// invoke parsing in a serial way
	parser := newClfsExtensionOutputParser(&mockedLcm)
	inputReader := strings.NewReader(sampleOutput)
	parser.startParsing(inputReader)

	allInfo := mockedLcm.GatherAllLogs(mockedLcm.infoLog)
	allError := mockedLcm.GatherAllLogs(mockedLcm.errorLog)
	allProgress := mockedLcm.GatherAllLogs(mockedLcm.progressLog)
	allExit := mockedLcm.GatherAllLogs(mockedLcm.exitLog)

	c.Check(len(allError), chk.Equals, 0)
	c.Check(len(allInfo) > 0, chk.Equals, true)
	c.Check(len(allProgress) > 0, chk.Equals, true)
	c.Check(len(allExit), chk.Equals, 1)
}

const sampleErrorOutput = `2019-10-09 07:34:05,205 INF CLFSLoad version 1.0.15
2019-10-09 07:34:05,394 ERR storage_account='zemaintest' container='clfs' is not empty
2019-10-09 07:34:05,434 INF azcopy:
AZCOPY-FINAL:{"elapsed":0.715491533279419,"files_completed_success":null,"files_completed_failed":null,"files_skipped":0,"files_total":null,"final_job_status":"Failed","total_bytes_transferred":null}`

func (s *clfsTestSuite) TestParsingSampleErrorOutput(c *chk.C) {
	// set up the mock lcm so that we can intercept the parsed message
	mockedLcm := mockedLifecycleManager{
		infoLog:     make(chan string, 50),
		exitLog:     make(chan string, 50),
		errorLog:    make(chan string, 50),
		progressLog: make(chan string, 50),
	}

	// invoke parsing in a serial way
	parser := newClfsExtensionOutputParser(&mockedLcm)
	parser.startParsing(strings.NewReader(sampleErrorOutput))

	allInfo := mockedLcm.GatherAllLogs(mockedLcm.infoLog)
	allError := mockedLcm.GatherAllLogs(mockedLcm.errorLog)
	allProgress := mockedLcm.GatherAllLogs(mockedLcm.progressLog)
	allExit := mockedLcm.GatherAllLogs(mockedLcm.exitLog)

	c.Check(len(allError), chk.Equals, 0)
	c.Check(len(allInfo), chk.Equals, 1)
	c.Check(len(allProgress), chk.Equals, 0)
	c.Check(len(allExit), chk.Equals, 1)
}
