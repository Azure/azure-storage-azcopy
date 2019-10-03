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

func (s *clfsTestSuite) TestParsingInfo(c *chk.C) {
	sampleInfo := "2019-10-01 23:34:13,753 INF compression LZ4 (1)"

	// set up the mock lcm so that we can intercept the parsed message
	mockedLcm := mockedLifecycleManager{infoLog: make(chan string, 50)}
	parser := newClfsExtensionOutputParser(&mockedLcm)

	// invoke the processing func
	parser.processInfo(sampleInfo)

	// validate the parsed output
	select {
	case msg := <-mockedLcm.infoLog:
		c.Check(msg, chk.Equals, "compression LZ4 (1)")
	default:
		// nothing parsed, disappointing
		c.Fail()
	}
}

func (s *clfsTestSuite) TestParsingError(c *chk.C) {
	sampleError := "2019-10-01 23:27:36,232 ERR './state6' already exists"

	// set up the mock lcm so that we can intercept the parsed message
	mockedLcm := mockedLifecycleManager{errorLog: make(chan string, 50)}
	parser := newClfsExtensionOutputParser(&mockedLcm)

	// invoke the processing func
	parser.processError(sampleError)

	// validate the parsed output
	select {
	case msg := <-mockedLcm.errorLog:
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
const sampleOutput = `2019-10-01 23:34:12,620 INF CLFSLoad version 1.0.11
2019-10-01 23:34:13,753 INF preserve_hardlinks 0
2019-10-01 23:34:13,753 INF compression LZ4 (1)
2019-10-01 23:34:13,940 INF begin phase transfer
2019-10-01 23:34:14,009 INF transfer 0+0/8 elapsed=0.000000 recent(fps/mbps)=0.0/0.00 cumulative=0.0/0.00 db_queue_len=0/0.000 blobs=0 written=0 (dir/nondir=0/0) GB=0.000000
AZCOPY:{"elapsed":0.00913548469543457,"files_completed_failed":0,"files_completed_success":0,"files_pending":null,"files_skipped":0,"files_total":null,"pct_complete":null,"phase":"Transfer","throughput_Mbps":0.0,"throughput_delta_secs":0.00913548469543457,"time_eta":null,"time_report":1569972854.0094466}
2019-10-01 23:34:16,010 INF transfer 0+0/8 elapsed=2.000168 recent(fps/mbps)=58.0/210.87 cumulative=58.0/210.87 db_queue_len=1/0.014 blobs=202 written=116 (dir/nondir=2/114) GB=0.421780
AZCOPY:{"elapsed":2.0093038082122803,"files_completed_failed":0,"files_completed_success":116,"files_pending":null,"files_skipped":0,"files_total":null,"pct_complete":null,"phase":"Transfer","throughput_Mbps":1727.466982878457,"throughput_delta_secs":2.0001683235168457,"time_eta":null,"time_report":1569972856.009615}
2019-10-01 23:34:18,009 INF transfer 7+0/8 elapsed=4.000156 recent(fps/mbps)=48.5/263.91 cumulative=53.2/237.39 db_queue_len=1/0.014 blobs=367 written=213 (dir/nondir=25/188) GB=0.949602
AZCOPY:{"elapsed":4.009291887283325,"files_completed_failed":0,"files_completed_success":213,"files_pending":null,"files_skipped":0,"files_total":null,"pct_complete":null,"phase":"Transfer","throughput_Mbps":3889.5945720349077,"throughput_delta_secs":1.999988079071045,"time_eta":null,"time_report":1569972858.009603}
2019-10-01 23:34:20,010 INF transfer 7+0/8 elapsed=6.000211 recent(fps/mbps)=0.0/273.43 cumulative=35.5/249.40 db_queue_len=1/0.014 blobs=437 written=213 (dir/nondir=25/188) GB=1.496477
AZCOPY:{"elapsed":6.0093467235565186,"files_completed_failed":0,"files_completed_success":213,"files_pending":null,"files_skipped":0,"files_total":null,"pct_complete":null,"phase":"Transfer","throughput_Mbps":6129.403331426832,"throughput_delta_secs":2.0000548362731934,"time_eta":null,"time_report":1569972860.0096579}
2019-10-01 23:34:22,009 INF transfer 7+0/8 elapsed=8.000175 recent(fps/mbps)=0.0/285.16 cumulative=26.6/258.34 db_queue_len=1/0.014 blobs=510 written=213 (dir/nondir=25/188) GB=2.066790
AZCOPY:{"elapsed":8.009310960769653,"files_completed_failed":0,"files_completed_success":213,"files_pending":null,"files_skipped":0,"files_total":null,"pct_complete":null,"phase":"Transfer","throughput_Mbps":8465.72276716412,"throughput_delta_secs":1.9999642372131348,"time_eta":null,"time_report":1569972862.009622}
2019-10-01 23:34:24,010 INF transfer 7+0/8 elapsed=10.000193 recent(fps/mbps)=0.0/269.53 cumulative=21.3/260.58 db_queue_len=1/0.014 blobs=579 written=213 (dir/nondir=25/188) GB=2.605852
AZCOPY:{"elapsed":10.009328842163086,"files_completed_failed":0,"files_completed_success":213,"files_pending":null,"files_skipped":0,"files_total":null,"pct_complete":null,"phase":"Transfer","throughput_Mbps":10673.475959933163,"throughput_delta_secs":2.0000178813934326,"time_eta":null,"time_report":1569972864.00964}
2019-10-01 23:34:26,011 INF transfer 7+0/8 elapsed=12.000508 recent(fps/mbps)=0.0/265.58 cumulative=17.7/261.41 db_queue_len=1/0.014 blobs=647 written=213 (dir/nondir=25/188) GB=3.137102
AZCOPY:{"elapsed":12.009643077850342,"files_completed_failed":0,"files_completed_success":213,"files_pending":null,"files_skipped":0,"files_total":null,"pct_complete":null,"phase":"Transfer","throughput_Mbps":12847.55280845147,"throughput_delta_secs":2.000314235687256,"time_eta":null,"time_report":1569972866.0099542}
2019-10-01 23:34:28,010 INF transfer 7+0/8 elapsed=14.000225 recent(fps/mbps)=0.0/289.10 cumulative=15.2/265.37 db_queue_len=1/0.014 blobs=721 written=213 (dir/nondir=25/188) GB=3.715227
AZCOPY:{"elapsed":14.009360074996948,"files_completed_failed":0,"files_completed_success":213,"files_pending":null,"files_skipped":0,"files_total":null,"pct_complete":null,"phase":"Transfer","throughput_Mbps":15219.72500104621,"throughput_delta_secs":1.9997169971466064,"time_eta":null,"time_report":1569972868.0096712}
2019-10-01 23:34:29,376 INF transfer 8+0/8 elapsed=15.366947 recent(fps/mbps)=0.7/234.37 cumulative=13.9/262.61 db_queue_len=0/0.000 blobs=763 written=214 (dir/nondir=25/189) GB=4.035540
2019-10-01 23:34:29,394 INF phase transfer complete, elapsed=15.384553
2019-10-01 23:34:29,395 INF transfer 0+8/8 elapsed=15.384553 recent(fps/mbps)=0.0/0.00 cumulative=13.9/262.31 db_queue_len=0/0.000 blobs=763 written=214 (dir/nondir=25/189) GB=4.035540
stats:
{'check_existing': 1,
 'compress_blob': 762,
 'count_dir': 25,
 'count_nondir': 189,
 'dbc_claim_miss': 84,
 'dbc_upsert_toc_check': 78,
 'dbc_upsert_toc_found': 78,
 'error_count': 0,
 'fh_regenerate': 0,
 'found_dir': 24,
 'found_nondir': 189,
 'gb_write': 4.035539889708161,
 'idle': 17,
 'wait': 84,
 'write_blob_bytes': 33696649,
 'write_blob_count': 763,
 'write_blob_fastpath': 0,
 'write_blob_secs': 43.1188223361969,
 'xfer_threads_busy_max': 1}
timers:
{'_count': 8,
 '_elapsed': 15.35014033317566,
 'blob_compress': '3.073975086 (2.50%)',
 'blob_write': '43.118822336 (35.11%)',
 'dir_generate': '1.090559244 (0.89%)',
 'dir_generate_update_state': '0.003031492 (0.00%)',
 'dir_generate_writeback2': '0.010361433 (0.01%)',
 'idle': '90.259510994 (73.50%)',
 'read_entries': '0.000687599 (0.00%)',
 'read_mt': '0.172951698 (0.14%)',
 'read_st': '0.004790306 (0.00%)',
 'work_item_claim': '0.026149988 (0.02%)',
 'work_item_execute': '32.401925802 (26.39%)',
 'work_item_wait': '90.247925520 (73.49%)',
 'wps.directory_write': '1.500050545 (1.22%)',
 'wps.writer.transfer': '29.463580847 (23.99%)',
 'writing_dir': '1.533771753 (1.25%)',
 'writing_nondir': '29.771774530 (24.24%)',
 'writing_update_state': '0.015924931 (0.01%)'}
DB stats:
{'error_count_entity': 0,
 'flush_count': 14,
 'flush_seconds': 0.3016519546508789,
 'flush_update_tobj_deferred': 16,
 'flush_upsert_tobj_slowpath': 0,
 'snap0_flush_count': 1,
 'snap0_flush_seconds': 0.11725616455078125,
 'snap1_flush_count': 13,
 'snap1_flush_seconds': 0.18439579010009766,
 'snap2_flush_count': 13,
 'snap2_flush_seconds': 0.18439579010009766}
DB timers:
{'_count': 0,
 '_elapsed': 15.454240083694458,
 'flush': '0.301651955 (1.95%)',
 'flush_01_compute_upsert': '0.003489971 (0.02%)',
 'flush_02_upsert': '0.035498142 (0.23%)',
 'flush_03_compute_update': '0.075634241 (0.49%)',
 'flush_04_update': '0.016016245 (0.10%)',
 'flush_06_flush': '0.000135183 (0.00%)',
 'flush_07_update_state': '0.057182074 (0.37%)',
 'flush_08_update_state_deferred': '0.000396729 (0.00%)',
 'flush_09_flush': '0.013414860 (0.09%)',
 'flush_10_reap': '0.080657005 (0.52%)',
 'preclaim_more': '0.000114918 (0.00%)'}
2019-10-01 23:34:29,429 INF begin phase finalize
2019-10-01 23:34:30,363 INF all phases complete
2019-10-01 23:34:30,425 INF CLFSLoad succeeded
2019-10-01 23:34:30,439 INF azcopy:
AZCOPY-FINAL:{"elapsed":18.100342273712158,"files_completed_success":25,"files_completed_failed":0,"files_skipped":0,"files_total":25,"final_job_status":"Completed","total_bytes_transferred":4333127962}
2019-10-01 23:34:30,441 INF elapsed=18.100 GB=4.035540 count=214 11.823 files/sec 0.222954 GB/s`

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

const sampleErrorOutput = `2019-10-01 23:33:51,188 INF CLFSLoad version 1.0.11
2019-10-01 23:33:51,744 ERR storage_account='zemaintest2' container='cfff' is not empty
2019-10-01 23:33:51,786 INF azcopy:
AZCOPY-FINAL:{"elapsed":1.5416233539581299,"files_completed_success":null,"files_completed_failed":null,"files_skipped":0,"files_total":null,"final_job_status":"Failed","total_bytes_transferred":null}`

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

	c.Check(len(allError), chk.Equals, 1)
	c.Check(len(allInfo) > 0, chk.Equals, true)
	c.Check(len(allProgress), chk.Equals, 0)
	c.Check(len(allExit), chk.Equals, 1)
}
