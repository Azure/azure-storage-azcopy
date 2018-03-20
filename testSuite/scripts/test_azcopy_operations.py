from scripts.utility import *

import time
def test_cancel_job():
    file_name = "test_cancel_file.txt"
    file_path = create_test_file(file_name, 1024*1024*1024)

    output = Command("copy").add_arguments(file_path).add_flags("Logging", "5").add_flags("recursive", "true").add_flags("background-op", "true").execute_azcopy_copy_command_get_output()
    if output is None:
        print("error copy file ", file_name, " in background mode")
        print("test_cancel_job test failed")
        return

    output_split = output.split(" ")
    jobId = output_split[len(output_split)-1]

    output = Command("cancel").add_arguments(jobId).execute_azcopy_operation_get_output()
    if output is None:
        print("error cancelling job with JobId ", jobId)
        return

    output = Command("list").add_arguments(jobId).execute_azcopy_operation_get_output()
    if output is not None:
        print("error cancelling the job")
        print("test_cancel_job test failed")
        return

    print("test_cancel_job test successfully passed")

def test_pause_resume_job_20Mb_file():
    file_name = "test_pause_resume_file.txt"
    file_path = create_test_file(file_name, 20*1024*1024)

    output = Command("copy").add_arguments(file_path).add_flags("Logging", "5").add_flags("recursive", "true").add_flags("background-op", "true").execute_azcopy_copy_command_get_output()
    if output is None:
        print("error copy file ", file_name, " in background mode")
        print("test_cancel_job test failed")
        return

    output_split = output.split(" ")
    jobId = output_split[len(output_split)-1]
    print("jobid ", jobId)
    output = Command("pause").add_arguments(jobId).execute_azcopy_operation_get_output()
    if output is None:
        print("error while pausing job with JobId ", jobId)
        return
    print("job with JobId ", jobId," paused")

    output = Command("resume").add_arguments(jobId).execute_azcopy_operation_get_output()
    if output is None:
        print("error while resuming job with JobId ", jobId)
        return
    print("job with JobId ", jobId," resumed")

    retry_count = 10
    for x in range (0, retry_count):
        result = Command("testBlob").add_arguments(file_name).execute_azcopy_verify()
        if not result:
           if x == (retry_count-1):
               print("the job could not resume successfully. test_pause_resume_job failed")
               return
           time.sleep(20)
        else:
            break
    print("test_pause_resume_job passed successfully")

def test_pause_resume_job_200Mb_file():
    file_name = "test_pause_resume_200mb_file.txt"
    file_path = create_test_file(file_name, 200*1024*1024)

    output = Command("copy").add_arguments(file_path).add_flags("Logging", "5").add_flags("recursive", "true").add_flags("background-op", "true").execute_azcopy_copy_command_get_output()
    if output is None:
        print("error copy file ", file_name, " in background mode")
        print("test_cancel_job test failed")
        return

    output_split = output.split(" ")
    jobId = output_split[len(output_split)-1]
    output = Command("pause").add_arguments(jobId).execute_azcopy_operation_get_output()
    if output is None:
        print("error while pausing job with JobId ", jobId)
        return
    print("job with JobId ", jobId," paused")
    output = Command("resume").add_arguments(jobId).execute_azcopy_operation_get_output()
    if output is None:
        print("error while resuming job with JobId ", jobId)
        return
    print("job with JobId ", jobId," resumed")
    retry_count = 5
    for x in range (0, retry_count):
        result = Command("testBlob").add_arguments(file_name).execute_azcopy_verify()
        if not result:
            if x == (retry_count-1):
                print("the job could not resume successfully. test_pause_resume_job failed")
                return
            time.sleep(60)
        else:
            break
    print("test_pause_resume_job passed successfully")


