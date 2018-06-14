import json
import os
import shutil
import time
import urllib
from collections import namedtuple
import sys
import utility as util


# test_download_1kb_blob verifies the download of 1Kb blob using azcopy.
def test_download_1kb_blob():
    # create file of size 1KB.
    filename = "test_1kb_blob_upload.txt"
    file_path = util.create_test_file(filename, 1024)

    # Upload 1KB file using azcopy.
    src = file_path
    dest = util.test_container_url
    result = util.Command("copy").add_arguments(src).add_arguments(dest). \
        add_flags("log-level", "info").add_flags("recursive", "true").execute_azcopy_copy_command()
    if not result:
        print("failed uploading 1KB file to the container")
        sys.exit(1)

    # Verifying the uploaded blob.
    # the resource local path should be the first argument for the azcopy validator.
    # the resource sas should be the second argument for azcopy validator.
    resource_url = util.get_resource_sas(filename)
    result = util.Command("testBlob").add_arguments(file_path).add_arguments(resource_url).execute_azcopy_verify()
    if not result:
        print("test_1kb_file test failed")
        sys.exit(1)

    time.sleep(5)

    # downloading the uploaded file
    src = util.get_resource_sas(filename)
    dest = util.test_directory_path + "/test_1kb_blob_download.txt"
    result = util.Command("copy").add_arguments(src).add_arguments(dest).add_flags("log-level",
                                                                                   "info").execute_azcopy_copy_command()

    if not result:
        print("test_download_1kb_blob test case failed")
        sys.exit(1)

    # Verifying the downloaded blob
    result = util.Command("testBlob").add_arguments(dest).add_arguments(src).execute_azcopy_verify()
    if not result:
        print("test_download_1kb_blob test case failed")
        sys.exit(1)

    print("test_download_1kb_blob successfully passed")


# test_download_perserve_last_modified_time verifies the azcopy downloaded file
# and its modified time preserved locally on disk
def test_blob_download_preserve_last_modified_time():
    # create a file of 2KB
    filename = "test_upload_preserve_last_mtime.txt"
    file_path = util.create_test_file(filename, 2048)

    # upload file through azcopy.
    destination_sas = util.get_resource_sas(filename)
    result = util.Command("copy").add_arguments(file_path).add_arguments(destination_sas). \
        add_flags("log-level", "info").add_flags("recursive", "true").execute_azcopy_copy_command()
    if not result:
        print("failed uploading 1KB file to the container")
        sys.exit(1)

    # Verifying the uploaded blob
    result = util.Command("testBlob").add_arguments(file_path).add_arguments(destination_sas).execute_azcopy_verify()
    if not result:
        print("test_download_perserve_last_modified_time test failed")
        sys.exit(1)

    time.sleep(5)

    # download file through azcopy with flag preserve-last-modified-time set to true
    download_file_name = util.test_directory_path + "/test_download_preserve_last_mtime.txt"
    result = util.Command("copy").add_arguments(destination_sas).add_arguments(download_file_name).add_flags("log-level",
                                                                                                             "info").add_flags(
        "preserve-last-modified-time", "true").execute_azcopy_copy_command()
    if not result:
        print("test_download_perserve_last_modified_time test case failed")
        sys.exit(1)

    # Verifying the downloaded blob and its modified with the modified time of blob.
    result = util.Command("testBlob").add_arguments(download_file_name).add_arguments(destination_sas).add_flags(
        "preserve-last-modified-time", "true").execute_azcopy_verify()
    if not result:
        print("test_download_perserve_last_modified_time test case failed")
        sys.exit(1)

    print("test_download_perserve_last_modified_time successfully passed")


# test_blob_download_63mb_in_4mb downloads 63mb file in block of 4mb through azcopy
def test_blob_download_63mb_in_4mb():
    # create file of 63mb
    file_name = "test_63mb_in4mb_upload.txt"
    file_path = util.create_test_file(file_name, 63 * 1024 * 1024)

    # uploading file through azcopy with flag block-size set to 4194304 i.e 4mb
    destination_sas = util.get_resource_sas(file_name)
    result = util.Command("copy").add_arguments(file_path).add_arguments(destination_sas).add_flags("log-level",
                                                                                                    "info").add_flags(
        "block-size", "4194304").execute_azcopy_copy_command()
    if not result:
        print("error uploading 63 mb file. test_blob_download_63mb_in_4mb test case failed")
        sys.exit(1)

    # verify the uploaded file.
    result = util.Command("testBlob").add_arguments(file_path).add_arguments(destination_sas).execute_azcopy_verify()
    if not result:
        print("error verifying the 63mb upload. test_blob_download_63mb_in_4mb test case failed")
        sys.exit(1)

    # downloading the created parallely in blocks of 4mb file through azcopy.
    download_file = util.test_directory_path + "/test_63mb_in4mb_download.txt"
    result = util.Command("copy").add_arguments(destination_sas).add_arguments(download_file).add_flags("log-level",
                                                                                                        "info").add_flags(
        "block-size", "4194304").execute_azcopy_copy_command()
    if not result:
        print("error downloading the 63mb file. test_blob_download_63mb_in_4mb test case failed")
        sys.exit(1)

    # verify the downloaded file
    result = util.Command("testBlob").add_arguments(download_file).add_arguments(
        destination_sas).execute_azcopy_verify()
    if not result:
        print("test_blob_download_63mb_in_4mb test case failed.")
        sys.exit(1)

    print("test_blob_download_63mb_in_4mb test case successfully passed")


# test_recursive_download_blob downloads a directory recursively from container through azcopy
def test_recursive_download_blob():
    # create directory and 5 files of 1KB inside that directory.
    dir_name = "dir_" + str(10) + "_files"
    dir1_path = util.create_test_n_files(1024, 5, dir_name)

    # upload the directory to container through azcopy with recursive set to true.
    result = util.Command("copy").add_arguments(dir1_path).add_arguments(util.test_container_url).add_flags("log-level",
                                                                                                            "info").add_flags(
        "recursive", "true").execute_azcopy_copy_command()
    if not result:
        print("error uploading recursive dir ", dir1_path)
        sys.exit(1)

    # verify the uploaded file.
    destination_sas = util.get_resource_sas(dir_name)
    result = util.Command("testBlob").add_arguments(dir1_path).add_arguments(destination_sas).add_flags("is-object-dir",
                                                                                                        "true").execute_azcopy_verify()
    if not result:
        print("error verify the recursive dir ", dir1_path, " upload")
        sys.exit(1)

    try:
        shutil.rmtree(dir1_path)
    except OSError as e:
        print("error removing the uploaded files. ", e)
        sys.exit(1)

    # downloading the directory created from container through azcopy with recursive flag to true.
    result = util.Command("copy").add_arguments(destination_sas).add_arguments(util.test_directory_path).add_flags(
        "log-level", "info").add_flags("recursive", "true").execute_azcopy_copy_command()
    if not result:
        print("error download recursive dir ", dir1_path)
        sys.exit(1)

    # verify downloaded blob.
    result = util.Command("testBlob").add_arguments(dir1_path).add_arguments(destination_sas).add_flags("is-object-dir",
                                                                                                        "true").execute_azcopy_verify()
    if not result:
        print("error verifying the recursive download ")
        sys.exit(1)
    print("test_recursive_download_blob successfully passed")


def test_blob_download_with_special_characters():
    filename_special_characters = "abc|>rd*"
    resource_url = util.get_resource_sas(filename_special_characters)
    # creating the file with random characters and with file name having special characters.
    result = util.Command("create").add_arguments(resource_url).add_flags("resourceType", "blob").add_flags(
        "isResourceABucket", "false").add_flags("blob-size", "1024").execute_azcopy_verify()
    if not result:
        print("error creating blob ", filename_special_characters, " with special characters")
        sys.exit(1)
    # downloading the blob created above.
    result = util.Command("copy").add_arguments(resource_url).add_arguments(util.test_directory_path).add_flags(
        "log-level", "info").execute_azcopy_copy_command()
    if not result:
        print("error downloading the file with special characters ", filename_special_characters)
        sys.exit(1)
    expected_filename = urllib.parse.quote_plus(filename_special_characters)
    # verify if the downloaded file exists or not.
    filepath = util.test_directory_path + "/" + expected_filename
    if not os.path.isfile(filepath):
        print("file not downloaded with expected file name")
        sys.exit(1)
    # verify the downloaded blob.
    result = util.Command("testBlob").add_arguments(filepath).add_arguments(resource_url).execute_azcopy_verify()
    if not result:
        print("error verifying the download of file ", filepath)
        sys.exit(1)
    print("test_download_file_with_special_characters successfully passed")


def test_sync_blob_download_without_wildcards():
    # created a directory and created 10 files inside the directory
    dir_name = "sync_download_without_wildcards"
    dir_n_files_path = util.create_test_n_files(1024, 10, dir_name)

    # upload the directory
    # execute azcopy command
    result = util.Command("copy").add_arguments(dir_n_files_path).add_arguments(util.test_container_url). \
        add_flags("recursive", "true").add_flags("log-level", "info").execute_azcopy_copy_command()
    if not result:
        print("test_sync_blob_download_without_wildcards failed while uploading ", 10, " files to the container")
        sys.exit(1)

    # execute the validator.
    dir_sas = util.get_resource_sas(dir_name)
    result = util.Command("testBlob").add_arguments(dir_n_files_path).add_arguments(dir_sas). \
        add_flags("is-object-dir", "true").execute_azcopy_verify()
    if not result:
        print(
            "test_sync_blob_download_without_wildcards test case failed validating the upload dir sync_download_without_wildcards on the container")
        sys.exit(1)
    # download the destination to the source to match the last modified time
    result = util.Command("copy").add_arguments(dir_sas).add_arguments(util.test_directory_path). \
        add_flags("log-level", "info").add_flags("recursive", "true").add_flags("output-json", "true"). \
        add_flags("preserve-last-modified-time", "true").execute_azcopy_copy_command_get_output()
    if not result:
        print("test_sync_blob_download_without_wildcards failed downloading the source ", dir_sas,
              " to the destination ", dir_n_files_path)
        sys.exit(1)
    # execute the validator and verify the downloaded dir
    result = util.Command("testBlob").add_arguments(dir_n_files_path).add_arguments(dir_sas). \
        add_flags("is-object-dir", "true").execute_azcopy_verify()
    if not result:
        print(
            "test_sync_blob_download_without_wildcards test case failed validating the downloaded dir sync_download_without_wildcards ")
        sys.exit(1)
    # sync the source and destination
    result = util.Command("sync").add_arguments(dir_sas).add_arguments(dir_n_files_path). \
        add_flags("log-level", "info").add_flags("recursive", "true").execute_azcopy_copy_command()
    if result:
        print("test_sync_blob_download_without_wildcards failed performing a sync between source ", dir_sas, " and ",
              dir_n_files_path)
        sys.exit(1)
    print("test_sync_blob_download_without_wildcards successfully passed ")


def test_sync_blob_download_with_wildcards():
    # created a directory and created 10 files inside the directory
    dir_name = "sync_download_with_wildcards"
    dir_n_files_path = util.create_test_n_files(1024, 10, dir_name)

    # upload the directory
    # execute azcopy command
    result = util.Command("copy").add_arguments(dir_n_files_path).add_arguments(util.test_container_url). \
        add_flags("recursive", "true").add_flags("log-level", "info").execute_azcopy_copy_command()
    if not result:
        print("test_sync_blob_download_with_wildcards failed while uploading ", 10, " files to the container")
        sys.exit(1)

    # execute the validator.
    dir_sas = util.get_resource_sas(dir_name)
    result = util.Command("testBlob").add_arguments(dir_n_files_path).add_arguments(dir_sas). \
        add_flags("is-object-dir", "true").execute_azcopy_verify()
    if not result:
        print(
            "test_sync_blob_download_with_wildcards test case failed validating the upload dir sync_download_with_wildcards on the container")
        sys.exit(1)
    # download the destination to the source to match the last modified time
    result = util.Command("copy").add_arguments(dir_sas).add_arguments(util.test_directory_path). \
        add_flags("log-level", "info").add_flags("recursive", "true").add_flags("output-json", "true"). \
        add_flags("preserve-last-modified-time", "true").execute_azcopy_copy_command_get_output()
    if not result:
        print("test_sync_blob_download_with_wildcards failed downloading the source ", dir_sas, " to the destination ",
              dir_n_files_path)
        sys.exit(1)
    # execute the validator and verify the downloaded dir
    result = util.Command("testBlob").add_arguments(dir_n_files_path).add_arguments(dir_sas). \
        add_flags("is-object-dir", "true").execute_azcopy_verify()
    if not result:
        print(
            "test_sync_blob_download_with_wildcards test case failed validating the downloaded dir sync_download_with_wildcards ")
        sys.exit(1)

    # add "*" at the end of dir sas
    # since both the source and destination are in sync, it will fail
    dir_sas = util.append_text_path_resource_sas(dir_sas, "*")
    # sync the source and destination
    result = util.Command("sync").add_arguments(dir_sas).add_arguments(dir_n_files_path). \
        add_flags("log-level", "info").add_flags("recursive", "true").execute_azcopy_copy_command()
    if result:
        print("test_sync_blob_download_without_wildcards failed performing a sync between source ", dir_sas, " and ",
              dir_n_files_path)
        sys.exit(1)
    subdir1 = os.path.join(dir_name, "subdir1")
    subdir1_file_path = util.create_test_n_files(1024, 10, subdir1)

    subdir2 = os.path.join(dir_name, "subdir2")
    subdir2_file_path = util.create_test_n_files(1024, 10, subdir2)

    # upload the directory
    # execute azcopy command
    result = util.Command("copy").add_arguments(dir_n_files_path).add_arguments(util.test_container_url). \
        add_flags("recursive", "true").add_flags("log-level", "info").execute_azcopy_copy_command()
    if not result:
        print("test_sync_blob_download_with_wildcards failed while uploading ", 30, " files to the container")
        sys.exit(1)

    # execute the validator.
    dir_sas = util.get_resource_sas(dir_name)
    result = util.Command("testBlob").add_arguments(dir_n_files_path).add_arguments(dir_sas). \
        add_flags("is-object-dir", "true").execute_azcopy_verify()
    if not result:
        print(
            "test_sync_blob_download_with_wildcards test case failed validating the upload dir with sub-dirs sync_download_with_wildcards on the container")
        sys.exit(1)

    # Download the directory to match the blob modified time
    result = util.Command("copy").add_arguments(dir_sas).add_arguments(util.test_directory_path). \
        add_flags("log-level", "Info").add_flags("recursive", "true").execute_azcopy_copy_command()
    if not result:
        print("execute_azcopy_copy_command failed downloading the directory ", dir_sas, " locally")
        sys.exit(1)
    # sync the source and destination
    # add extra wildcards
    dir_sas = util.append_text_path_resource_sas(dir_sas, "*/*.txt")
    result = util.Command("sync").add_arguments(dir_sas).add_arguments(dir_n_files_path). \
        add_flags("log-level", "info").add_flags("recursive", "true").execute_azcopy_copy_command()
    if result:
        print("test_sync_blob_download_without_wildcards failed performing a sync between source ", dir_sas, " and ",
              dir_n_files_path)
        sys.exit(1)

    # delete 5 files inside each sub-directories locally
    for r in range(5, 9):
        filename = "test101024_" + str(r) + ".txt"
        filepath = os.path.join(subdir1_file_path, filename)
        try:
            os.remove(filepath)
        except:
            print("test_sync_blob_download_with_wildcards while deleting file ", filepath)
            sys.exit(1)
        filepath = os.path.join(subdir2_file_path, filename)
        try:
            os.remove(filepath)
        except:
            print("test_sync_blob_download_with_wildcards while deleting file ", filepath)
            sys.exit(1)
    # 10 files have been deleted inside the sub-dir
    # sync remote to local
    # 10 files will be downloaded
    result = util.Command("sync").add_arguments(dir_sas).add_arguments(dir_n_files_path). \
        add_flags("log-level", "info").add_flags("recursive", "true").add_flags("output-json",
                                                                              "true").execute_azcopy_copy_command_get_output()
    # parse the result to get the last job progress summary
    result = util.parseAzcopyOutput(result)
    # parse the Json Output
    x = json.loads(result, object_hook=lambda d: namedtuple('X', d.keys())(*d.values()))
    # Number of Expected Transfer should be 10 since 10 files were deleted
    if x.TransfersCompleted is not 10 and x.TransfersFailed is not 0:
        print(
            "test_sync_blob_download_with_wildcards failed with difference in the number of failed and successful transfers")
        sys.exit(1)
    print("test_sync_blob_download_with_wildcards successfully passed ")
