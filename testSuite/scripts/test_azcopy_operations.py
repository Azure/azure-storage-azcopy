import utility as util

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
def test_pause_resume_job_95Mb_file():
    # create test file of 20 MB
    file_name = "test_pause_resume_file_95.txt"
    file_path = create_test_file(file_name, 95*1024*1024)

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

# test_remove_virtual_directory  creates a virtual directory, removes the virtual directory created
# and then verifies the contents of virtual directory.
def test_remove_virtual_directory():
    # create dir dir_10_files and 1 kb files inside the dir.
    dir_name = "dir_"+str(10)+"_files_rm"
    dir_n_files_path = util.create_test_n_files(1024, 10, dir_name)

    # execute azcopy command
    result = util.Command("copy").add_arguments(dir_n_files_path).add_arguments(util.test_container_url). \
        add_flags("recursive", "true").add_flags("Logging", "5").execute_azcopy_copy_command()
    if not result:
        print("test_remove_virtual_directory failed while uploading ", dir_n_files_path, " files to the container")
        return

    destination = util.get_resource_sas(dir_name)
    result = util.Command("rm").add_arguments(destination).add_flags("recursive", "true").execute_azcopy_copy_command()
    if not result:
        print("test_remove_virtual_directory failed while removing ", dir_n_files_path, " files to the container")
        return

    result = util.Command("list").add_arguments(destination).add_flags("resource-num", "0").execute_azcopy_verify()
    if not result:
        print("test_remove_virtual_directory failed while listing ", destination)
        return
    print("test_remove_virtual_directory passed")







