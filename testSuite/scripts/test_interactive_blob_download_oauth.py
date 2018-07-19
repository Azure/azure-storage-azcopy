import os
import shutil
import time

import utility as util

# test_download_1kb_blob verifies the download of 1Kb blob using azcopy.
def test_download_1kb_blob_oauth(
    forceOAuthLogin=False,
    tenantID="",
    aadEndpoint=""):
    # create file of size 1KB.
    filename = "test_1kb_blob_upload.txt"
    file_path = util.create_test_file(filename, 1024)

    # Upload 1KB file using azcopy.
    src = file_path
    dest = util.test_oauth_container_url
    cmd = util.Command("copy").add_arguments(src).add_arguments(dest). \
        add_flags("log-level", "info").add_flags("recursive", "true")
    util.process_oauth_command(
        cmd,
        "",
        forceOAuthLogin,
        tenantID,
        aadEndpoint)
    if forceOAuthLogin:
        result = cmd.execute_azcopy_command_interactive()
    else:
        result = cmd.execute_azcopy_copy_command()
    if not result:
        print("failed uploading 1KB file to the container")
        return

    # Verifying the uploaded blob.
    # the resource local path should be the first argument for the azcopy validator.
    # the resource sas should be the second argument for azcopy validator.
    dest_validate = util.get_resource_from_oauth_container_validate(filename)
    result = util.Command("testBlob").add_arguments(file_path).add_arguments(dest_validate).execute_azcopy_verify()
    if not result:
        print("test_1kb_file test failed")
        return

    time.sleep(5)

    # downloading the uploaded file
    src = util.get_resource_from_oauth_container(filename)
    src_validate = util.get_resource_from_oauth_container_validate(filename)
    dest = util.test_directory_path + "/test_1kb_blob_download.txt"
    cmd = util.Command("copy").add_arguments(src).add_arguments(dest).add_flags("log-level", "info")
    util.process_oauth_command(
        cmd,
        "",
        forceOAuthLogin,
        tenantID,
        aadEndpoint)
    if forceOAuthLogin:
        result = cmd.execute_azcopy_command_interactive()
    else:
        result = cmd.execute_azcopy_copy_command()
    if not result:
        print("test_download_1kb_blob test case failed")
        return

    # Verifying the downloaded blob
    result = util.Command("testBlob").add_arguments(dest).add_arguments(src_validate).execute_azcopy_verify()
    if not result:
        print("test_download_1kb_blob test case failed")
        return

    print("test_download_1kb_blob successfully passed")

# test_recursive_download_blob downloads a directory recursively from container through azcopy
def test_recursive_download_blob_oauth(
    forceOAuthLogin=False,
    tenantID="",
    aadEndpoint=""):
    # create directory and 5 files of 1KB inside that directory.
    dir_name = "dir_" + str(10) + "_files"
    dir1_path = util.create_test_n_files(1024, 5, dir_name)

    dest = util.test_oauth_container_url
    # upload the directory to container through azcopy with recursive set to true.
    cmd = util.Command("copy").add_arguments(dir1_path).add_arguments(dest).add_flags("log-level", "info") \
        .add_flags("recursive", "true")
    util.process_oauth_command(
        cmd,
        "",
        forceOAuthLogin,
        tenantID,
        aadEndpoint)
    if forceOAuthLogin:
        result = cmd.execute_azcopy_command_interactive()
    else:
        result = cmd.execute_azcopy_copy_command()
    if not result:
        print("error uploading recursive dir ", dir1_path)
        return

    # verify the uploaded file.
    dest_validate = util.get_resource_from_oauth_container_validate(dir_name)
    result = util.Command("testBlob").add_arguments(dir1_path).add_arguments(dest_validate).add_flags("is-object-dir",
                                                                                                        "true").execute_azcopy_verify()
    if not result:
        print("error verify the recursive dir ", dir1_path, " upload")
        return

    try:
        shutil.rmtree(dir1_path)
    except OSError as e:
        print("error removing the uploaded files. ", e)
        return

    src_download = util.get_resource_from_oauth_container(dir_name)
    # downloading the directory created from container through azcopy with recursive flag to true.
    cmd = util.Command("copy").add_arguments(src_download).add_arguments(util.test_directory_path).add_flags(
        "log-level", "info").add_flags("recursive", "true")
    util.process_oauth_command(
        cmd,
        "",
        forceOAuthLogin,
        tenantID,
        aadEndpoint)
    if forceOAuthLogin:
        result = cmd.execute_azcopy_command_interactive()
    else:
        result = cmd.execute_azcopy_copy_command()
    if not result:
        print("error download recursive dir ", dir1_path)
        return

    # verify downloaded blob.
    result = util.Command("testBlob").add_arguments(dir1_path).add_arguments(dest_validate).add_flags("is-object-dir",
                                                                                                        "true").execute_azcopy_verify()
    if not result:
        print("error verifying the recursive download ")
        return
    print("test_recursive_download_blob successfully passed")
