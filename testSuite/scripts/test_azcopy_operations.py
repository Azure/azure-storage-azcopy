from scripts.utility import *

import time

# test_cancel_job verifies the cancel functionality of azcopy
def test_cancel_job():
    # create test file.
    file_name = "test_cancel_file.txt"
    file_path = create_test_file(file_name, 1024*1024*1024)

    # execute the azcopy upload job in background.
    output = Command("copy").add_arguments(file_path).add_flags("Logging", "5").add_flags("recursive", "true").add_flags("background-op", "true").execute_azcopy_copy_command_get_output()
    if output is None:
        print("error copy file ", file_name, " in background mode")
        print("test_cancel_job test failed")
        return

    # get the job Id of new job started by parsing the azcopy console output.
    output_split = output.split(" ")
    jobId = output_split[len(output_split)-1]

    # execute azcopy cancel job.
    output = Command("cancel").add_arguments(jobId).execute_azcopy_operation_get_output()
    if output is None:
        print("error cancelling job with JobId ", jobId)
        return

    # execute list job progress summary.
    # expected behavior is it should fail.
    output = Command("list").add_arguments(jobId).execute_azcopy_operation_get_output()
    if output is not None:
        print("error cancelling the job")
        print("test_cancel_job test failed")
        return
    print("test_cancel_job test successfully passed")

# test_pause_resume_job_20Mb_file verifies the azcopy pause and resume functionality.
def test_pause_resume_job_20Mb_file():
    # create test file of 20 MB
    file_name = "test_pause_resume_file.txt"
    file_path = create_test_file(file_name, 20*1024*1024)

    # execute azcopy file upload in background.
    output = Command("copy").add_arguments(file_path).add_flags("Logging", "5").add_flags("recursive", "true").add_flags("background-op", "true").execute_azcopy_copy_command_get_output()
    if output is None:
        print("error copy file ", file_name, " in background mode")
        print("test_cancel_job test failed")
        return

    # get the job Id of new job started by parsing the azcopy console output.
    output_split = output.split(" ")
    jobId = output_split[len(output_split)-1]

    # execute azcopy pause job with jobId.
    output = Command("pause").add_arguments(jobId).execute_azcopy_operation_get_output()
    if output is None:
        print("error while pausing job with JobId ", jobId)
        return

    # execute azcopy resume job with JobId.
    output = Command("resume").add_arguments(jobId).execute_azcopy_operation_get_output()
    if output is None:
        print("error while resuming job with JobId ", jobId)
        return

    # execute azcopy validator for the verifying the blob uploaded.
    # since blob upload will take time after it has resumed, it is
    # validated in loop with sleep of 1 min after each try.
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

# test_pause_resume_job_200Mb_file verifies the azcopy pause and resume functionality.
def test_pause_resume_job_200Mb_file():
    # create test file of 20 MB
    file_name = "test_pause_resume_file.txt"
    file_path = create_test_file(file_name, 200*1024*1024)

    # execute azcopy file upload in background.
    output = Command("copy").add_arguments(file_path).add_flags("Logging", "5").add_flags("recursive", "true").add_flags("background-op", "true").execute_azcopy_copy_command_get_output()
    if output is None:
        print("error copy file ", file_name, " in background mode")
        print("test_cancel_job test failed")
        return

    # get the job Id of new job started by parsing the azcopy console output.
    output_split = output.split(" ")
    jobId = output_split[len(output_split)-1]

    # execute azcopy pause job with jobId.
    output = Command("pause").add_arguments(jobId).execute_azcopy_operation_get_output()
    if output is None:
        print("error while pausing job with JobId ", jobId)
        return

    # execute azcopy resume job with JobId.
    output = Command("resume").add_arguments(jobId).execute_azcopy_operation_get_output()
    if output is None:
        print("error while resuming job with JobId ", jobId)
        return

    # execute azcopy validator for the verifying the blob uploaded.
    # since blob upload will take time after it has resumed, it is
    # validated in loop with sleep of 1 min after each try.
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


