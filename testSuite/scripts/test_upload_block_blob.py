import json
import os
import shutil
from collections import namedtuple
from stat import *

import utility as util


# test_1kb_blob_upload verifies the 1KB blob upload by azcopy.
def test_1kb_blob_upload():
    # Creating a single File Of size 1 KB
    filename = "test1KB.txt"
    file_path = util.create_test_file(filename, 1024)

    # executing the azcopy command to upload the 1KB file.
    src = file_path
    dest = util.get_resource_sas(filename)
    result = util.Command("copy").add_arguments(src).add_arguments(dest). \
        add_flags("log-level", "info").add_flags("recursive", "true").execute_azcopy_copy_command()
    if not result:
        print("failed uploading 1KB file to the container")
        return

    # Verifying the uploaded blob.
    # the resource local path should be the first argument for the azcopy validator.
    # the resource sas should be the second argument for azcopy validator.
    result = util.Command("testBlob").add_arguments(file_path).add_arguments(dest).execute_azcopy_verify()
    if not result:
        print("test_1kb_file test failed")
    else:
        print("test_1kb_file successfully passed")


# test_63mb_blob_upload verifies the azcopy upload of 63mb blob upload.
def test_63mb_blob_upload():
    # creating file of 63mb size.
    filename = "test63Mb_blob.txt"
    file_path = util.create_test_file(filename, 8 * 1024 * 1024)

    # execute azcopy copy upload.
    dest = util.get_resource_sas(filename)
    result = util.Command("copy").add_arguments(file_path).add_arguments(dest) \
        .add_flags("log-level", "info").add_flags("block-size", "104857600").add_flags("recursive", "true"). \
        execute_azcopy_copy_command()
    if not result:
        print("failed uploading file", filename, " to the container")
        return

    # Verifying the uploaded blob
    # calling the testBlob validator to verify whether blob has been successfully uploaded or not
    result = util.Command("testBlob").add_arguments(file_path).add_arguments(dest).execute_azcopy_verify()
    if not result:
        print("test_63MB_file test failed")
    else:
        print("test_63MB_file successfully passed")


# test_n_1kb_blob_upload verifies the upload of n 1kb blob to the container.
def test_n_1kb_blob_upload(number_of_files):
    # create dir dir_n_files and 1 kb files inside the dir.
    dir_name = "dir_" + str(number_of_files) + "_files"
    dir_n_files_path = util.create_test_n_files(1024, number_of_files, dir_name)

    # execute azcopy command
    result = util.Command("copy").add_arguments(dir_n_files_path).add_arguments(util.test_container_url). \
        add_flags("recursive", "true").add_flags("log-level", "info").execute_azcopy_copy_command()
    if not result:
        print("test_n_1kb_blob_upload failed while uploading ", number_of_files, " files to the container")
        return

    # execute the validator.
    destination = util.get_resource_sas(dir_name)
    result = util.Command("testBlob").add_arguments(dir_n_files_path).add_arguments(destination). \
        add_flags("is-object-dir", "true").execute_azcopy_verify()
    if not result:
        print("test_n_1kb_blob_upload test case failed")
    else:
        print("test_n_1kb_blob_upload passed successfully")


# test_metaData_content_encoding_content_type verifies the meta data, content type,
# content encoding of 2kb upload to container through azcopy.
def test_blob_metaData_content_encoding_content_type():
    # create 2kb file test_mcect.txt
    filename = "test_mcect.txt"
    file_path = util.create_test_file(filename, 2048)

    # execute azcopy upload command.
    destination_sas = util.get_resource_sas(filename)
    result = util.Command("copy").add_arguments(file_path).add_arguments(destination_sas). \
        add_flags("log-level", "info").add_flags("recursive", "true").add_flags("metadata",
                                                                              "author=prjain;viewport=width;description=test file"). \
        add_flags("content-type", "testctype").add_flags("content-encoding", "testenc").add_flags("no-guess-mime-type",
                                                                                                  "true").execute_azcopy_copy_command()
    if not result:
        print("uploading 2KB file with metadata, content type and content-encoding failed")
        return

    # execute azcopy validate order.
    # adding the source in validator as first argument.
    # adding the destination in validator as second argument.
    result = util.Command("testBlob").add_arguments(file_path).add_arguments(destination_sas).add_flags("metadata",
                                                                                                        "author=prjain;viewport=width;description=test file"). \
        add_flags("content-type", "testctype").add_flags("content-encoding", "testenc").add_flags("no-guess-mime-type",
                                                                                                  "true").execute_azcopy_verify()
    if not result:
        print("test_metaData_content_encoding_content_type failed")
    else:
        print("test_metaData_content_encoding_content_type successfully passed")


# test_1G_blob_upload verifies the azcopy upload of 1Gb blob upload in blocks of 100 Mb
def test_1GB_blob_upload():
    # create 1Gb file
    filename = "test_1G_blob.txt"
    file_path = util.create_test_file(filename, 1 * 1024 * 1024 * 1024)

    # execute azcopy upload.
    destination_sas = util.get_resource_sas(filename)
    result = util.Command("copy").add_arguments(file_path).add_arguments(destination_sas).add_flags("log-level", "info"). \
        add_flags("block-size", "104857600").add_flags("recursive", "true").execute_azcopy_copy_command()
    if not result:
        print("failed uploading 1G file", filename, " to the container")
        return

    # Verifying the uploaded blob.
    # adding local file path as first argument.
    # adding file sas as local argument.
    # calling the testBlob validator to verify whether blob has been successfully uploaded or not.
    result = util.Command("testBlob").add_arguments(file_path).add_arguments(destination_sas).execute_azcopy_verify()
    if not result:
        print("test_1GB_blob_upload test failed")
        return
    print("test_1GB_blob_upload successfully passed")


# test_block_size verifies azcopy upload of blob in blocks of given block-size
# performs the upload, verify the blob and number of blocks.
def test_block_size(block_size):
    # create file of size 63 Mb
    filename = "test63Mb_blob.txt"
    file_path = util.create_test_file(filename, 63 * 1024 * 1024)

    # execute azcopy upload of 63 Mb file.
    destination_sas = util.get_resource_sas(filename)
    result = util.Command("copy").add_arguments(file_path).add_arguments(destination_sas).add_flags("log-level", "info"). \
        add_flags("block-size", str(block_size)).add_flags("recursive", "true").execute_azcopy_copy_command()
    if not result:
        print("failed uploading file", filename, " with block size 4MB to the container")
        return

    # Verifying the uploaded blob
    # calling the testBlob validator to verify whether blob has been successfully uploaded or not
    if (63 * 1024 * 1024) % block_size == 0:
        number_of_blocks = int(63 * 1024 * 1024 / block_size)
    else:
        number_of_blocks = int(63 * 1024 * 1024 / block_size) + 1
    result = util.Command("testBlob").add_arguments(file_path).add_arguments(destination_sas).add_flags(
        "verify-block-size", "true").add_flags("number-blocks-or-pages", str(number_of_blocks)).execute_azcopy_verify()
    if not result:
        print("test_block_size test failed")
        return
    print("test_block_size successfully passed")


# test_guess_mime_type verifies the mime type detection by azcopy while uploading the blob
def test_guess_mime_type():
    # create a test html file
    filename = "test_guessmimetype.html"
    file_path = util.create_test_html_file(filename)

    # execute azcopy upload of html file.
    destination_sas = util.get_resource_sas(filename)
    result = util.Command("copy").add_arguments(file_path).add_arguments(destination_sas).add_flags("log-level", "info"). \
        add_flags("recursive", "true").execute_azcopy_copy_command()
    if not result:
        print("uploading file ", filename, " failed")
        return

    # execute the validator to verify the content-type.
    result = util.Command("testBlob").add_arguments(file_path).add_arguments(destination_sas).add_flags("log-level",
                                                                                                        "info"). \
        add_flags("recursive", "true")
    if not result:
        print("test_guess_mime_type test failed")
    else:
        print("test_guess_mime_type successfully passed")


def test_set_block_blob_tier():
    # create a file file_hot_block_blob_tier
    filename = "test_hot_block_blob_tier.txt"
    file_path = util.create_test_file(filename, 10 * 1024)

    # uploading the file file_hot_block_blob_tier using azcopy and setting the block-blob-tier to Hot
    destination_sas = util.get_resource_sas(filename)
    result = util.Command("copy").add_arguments(file_path).add_arguments(destination_sas). \
        add_flags("log-level", "info").add_flags("block-blob-tier", "Hot").execute_azcopy_copy_command()
    if not result:
        print("uploading file with block-blob-tier set to Hot failed. ")
        return
    # execute azcopy validate order.
    # added the expected blob-tier "Hot"
    result = util.Command("testBlob").add_arguments(file_path).add_arguments(destination_sas).add_flags("blob-tier",
                                                                                                        "Hot").execute_azcopy_verify()
    if not result:
        print("test_set_block_blob_tier failed for Hot access Tier Type")
        return

    # create file to upload with block blob tier set to "Cool".
    filename = "test_cool_block_blob_tier.txt"
    file_path = util.create_test_file(filename, 10 * 1024)

    # uploading the file file_cool_block_blob_tier using azcopy and setting the block-blob-tier to Cool.
    destination_sas = util.get_resource_sas(filename)
    result = util.Command("copy").add_arguments(file_path).add_arguments(destination_sas). \
        add_flags("log-level", "info").add_flags("block-blob-tier", "Cool").execute_azcopy_copy_command()
    if not result:
        print("uploading file with block-blob-tier set to Cool failed.")
        return
    # execute azcopy validate order.
    # added the expected blob-tier "Cool"
    result = util.Command("testBlob").add_arguments(file_path).add_arguments(destination_sas).add_flags("blob-tier",
                                                                                                        "Cool").execute_azcopy_verify()
    if not result:
        print("test_set_block_blob_tier failed for Cool access Tier Type")
        return

    # create file to upload with block blob tier set to "Archive".
    filename = "test_archive_block_blob_tier.txt"
    file_path = util.create_test_file(filename, 10 * 1024)

    # uploading the file file_archive_block_blob_tier using azcopy and setting the block-blob-tier to Archive.
    destination_sas = util.get_resource_sas(filename)
    result = util.Command("copy").add_arguments(file_path).add_arguments(destination_sas). \
        add_flags("log-level", "info").add_flags("block-blob-tier", "archive").execute_azcopy_copy_command()
    if not result:
        print("uploading file with block-blob-tier set to Cool failed.")
        return
    # execute azcopy validate order.
    # added the expected blob-tier "Archive"
    result = util.Command("testBlob").add_arguments(file_path).add_arguments(destination_sas).add_flags("blob-tier",
                                                                                                        "Archive").execute_azcopy_verify()
    if not result:
        print("test_set_block_blob_tier failed for Archive access Tier Type")
        return
    print("test_set_block_blob_tier successfully passed")


def test_force_flag_set_to_false_upload():
    # creating directory with 20 files in it.
    dir_name = "dir_force_flag_set_upload"
    dir_n_files_path = util.create_test_n_files(1024, 20, dir_name)
    # uploading the directory with 20 files in it.
    result = util.Command("copy").add_arguments(dir_n_files_path).add_arguments(util.test_container_url). \
        add_flags("recursive", "true").add_flags("log-level", "info").execute_azcopy_copy_command()
    if not result:
        print("test_force_flag_set_to_false_upload failed while uploading ", 20,
              "files in to dir_force_flag_set_upload the container")
        return
    # execute the validator and validating the uploaded directory.
    destination = util.get_resource_sas(dir_name)
    result = util.Command("testBlob").add_arguments(dir_n_files_path).add_arguments(destination). \
        add_flags("is-object-dir", "true").execute_azcopy_verify()
    if not result:
        print("test_force_flag_set_to_false_upload test case failed while validating the directory uploaded")

    # uploading the directory again with force flag set to false.
    result = util.Command("copy").add_arguments(dir_n_files_path).add_arguments(util.test_container_url). \
        add_flags("recursive", "true").add_flags("force", "false").add_flags("log-level", "info"). \
        add_flags("output-json", "true").execute_azcopy_copy_command_get_output()
    if not result:
        print("test_force_flag_set_to_false_upload failed while uploading ", 20,
              "files in to dir_force_flag_set_upload the container with force flag set to false")
        return

    # parsing the json and comparing the number of failed and successful transfers.
    result = util.parseAzcopyOutput(result)
    x = json.loads(result, object_hook=lambda d: namedtuple('X', d.keys())(*d.values()))
    if x.TransfersFailed is not 20 and x.TransfersCompleted is not 0:
        print(
            "test_force_flag_set_to_false_upload failed with difference in the number of failed and successful transfers")

    # uploading a sub-directory inside the above dir with 20 files inside the sub-directory.
    # total number of file inside the dir is 40
    sub_dir_name = os.path.join(dir_name + "/sub_dir_force_flag_set_upload")
    sub_dir_n_files_path = util.create_test_n_files(1024, 20, sub_dir_name)

    result = util.Command("copy").add_arguments(dir_n_files_path).add_arguments(util.test_container_url). \
        add_flags("recursive", "true").add_flags("log-level", "info").execute_azcopy_copy_command()
    if not result:
        print("test_force_flag_set_to_false_upload failed while uploading ", 20, "files in " + sub_dir_force_flag_set)
        return

    # execute the validator and verifying the uploaded sub directory.
    sub_directory_resource_sas = util.get_resource_sas(sub_dir_name)

    result = util.Command("testBlob").add_arguments(sub_dir_n_files_path).add_arguments(sub_directory_resource_sas). \
        add_flags("is-object-dir", "true").execute_azcopy_verify()
    if not result:
        print("test_force_flag_set_to_false_upload test case failed while validating the directory uploaded")

    # removing the sub directory.
    result = util.Command("rm").add_arguments(sub_directory_resource_sas). \
        add_flags("log-level", "info").add_flags("recursive", "true").execute_azcopy_copy_command()
    if not result:
        print("test_force_flag_set_to_false_upload failed removing ", sub_dir_n_files_path)
        return

    # uploading the directory again with force flag set to false.
    result = util.Command("copy").add_arguments(dir_n_files_path).add_arguments(util.test_container_url). \
        add_flags("recursive", "true").add_flags("force", "false").add_flags("log-level", "info"). \
        add_flags("output-json", "true").execute_azcopy_copy_command_get_output()
    if not result:
        print("test_force_flag_set_to_false_upload failed while uploading ", 20,
              "files in to dir_force_flag_set the container with force flag set to false")
        return

    # parsing the json and comparing the number of failed and successful transfers.
    # Number of failed transfers should be 20 and number of successful transfer should be 20.
    result = util.parseAzcopyOutput(result)
    x = json.loads(result, object_hook=lambda d: namedtuple('X', d.keys())(*d.values()))
    if x.TransfersFailed is not 20 and x.TransfersCompleted is not 20:
        print(
            "test_force_flag_set_to_false_upload failed with difference in the number of failed and successful transfers")

    print("test_force_flag_set_to_false_upload successfully passed.")


def test_force_flag_set_to_false_download():
    # creating directory with 20 files in it.
    dir_name = "dir_force_flag_set_download"
    dir_n_files_path = util.create_test_n_files(1024, 20, dir_name)
    # uploading the directory with 20 files in it.
    result = util.Command("copy").add_arguments(dir_n_files_path).add_arguments(util.test_container_url). \
        add_flags("recursive", "true").add_flags("log-level", "info").execute_azcopy_copy_command()
    if not result:
        print("test_force_flag_set_to_false_download failed while uploading ", 20,
              "files in to dir_force_flag_set_download the container")
        return
    # execute the validator and validating the uploaded directory.
    destination = util.get_resource_sas(dir_name)
    result = util.Command("testBlob").add_arguments(dir_n_files_path).add_arguments(destination). \
        add_flags("is-object-dir", "true").execute_azcopy_verify()
    if not result:
        print("test_force_flag_set_to_false_download test case failed while validating the directory uploaded")

    # removing the directory dir_force_flag_set_download
    try:
        shutil.rmtree(dir_n_files_path)
    except:
        print("test_force_flag_set_to_false_download failed error removing the directory ", dir_n_files_path)
        return

    # downloading the directory created from container through azcopy with recursive flag to true.
    result = util.Command("copy").add_arguments(destination).add_arguments(util.test_directory_path).add_flags(
        "log-level", "info").add_flags("recursive", "true").execute_azcopy_copy_command()
    if not result:
        print("test_force_flag_set_to_false_download failed downloading dir ", dir_name)
        return

    # verify downloaded blob.
    result = util.Command("testBlob").add_arguments(dir_n_files_path).add_arguments(destination).add_flags(
        "is-object-dir", "true").execute_azcopy_verify()
    if not result:
        print("test_force_flag_set_to_false_download failed validating downloaded dir ", dir_name)
        return

    # downloading the directory created from container through azcopy with recursive flag to true and force flag set to false.
    result = util.Command("copy").add_arguments(destination).add_arguments(util.test_directory_path).add_flags(
        "log-level", "info"). \
        add_flags("recursive", "true").add_flags("force", "false").add_flags("output-json",
                                                                             "true").execute_azcopy_copy_command_get_output()
    result = util.parseAzcopyOutput(result)
    x = json.loads(result, object_hook=lambda d: namedtuple('X', d.keys())(*d.values()))
    if x.TransfersFailed is not 20 and x.TransfersCompleted is not 0:
        print(
            "test_force_flag_set_to_false_download failed with difference in the number of failed and successful transfers")
        return

    # removing 5 files with suffix from 10 to 14
    for index in range(10, 15):
        file_path_remove = dir_n_files_path + os.sep + "test201024" + "_" + str(index) + ".txt"
        try:
            os.remove(file_path_remove)
        except:
            print("test_force_flag_set_to_false_download error removing the file ", file_path_remove)
            return

    # downloading the directory created from container through azcopy with recursive flag to true and force flag set to false.
    # 5 deleted files should be downloaded. Number of failed transfer should be 15 and number of completed transfer should be 5
    result = util.Command("copy").add_arguments(destination).add_arguments(util.test_directory_path).add_flags(
        "log-level", "info"). \
        add_flags("recursive", "true").add_flags("force", "false").add_flags("output-json",
                                                                             "true").execute_azcopy_copy_command_get_output()
    result = util.parseAzcopyOutput(result)
    x = json.loads(result, object_hook=lambda d: namedtuple('X', d.keys())(*d.values()))
    if x.TransfersFailed is not 15 and x.TransfersCompleted is not 5:
        print(
            "test_force_flag_set_to_false_download failed with difference in the number of failed and successful transfers")
        return
    print("test_force_flag_set_to_false_download successfully passed")


# test_upload_block_blob_include_flag tests the include flag in the upload scenario
def test_upload_block_blob_include_flag():
    dir_name = "dir_include_flag_set_upload"
    # create 10 files inside the directory
    dir_n_files_path = util.create_test_n_files(1024, 10, dir_name)

    # create sub-directory inside the  dir_include_flag_set_upload
    sub_dir_name = os.path.join(dir_name, "sub_dir_include_flag_set_upload")
    # create 10 files inside the sub-dir
    sub_dir_n_file_path = util.create_test_n_files(1024, 10, sub_dir_name)

    # uploading the directory with 2 files in the include flag.
    result = util.Command("copy").add_arguments(dir_n_files_path).add_arguments(util.test_container_url). \
        add_flags("recursive", "true").add_flags("log-level", "info") \
        .add_flags("include", "test101024_2.txt;test101024_3.txt").add_flags("output-json",
                                                                             "true").execute_azcopy_copy_command_get_output()
    # parse the result to get the last job progress summary
    result = util.parseAzcopyOutput(result)
    # parse the Json Output
    x = json.loads(result, object_hook=lambda d: namedtuple('X', d.keys())(*d.values()))
    # Number of successful transfer should be 2 and there should be not a failed transfer
    if x.TransfersCompleted is not 2 and x.TransfersFailed is not 0:
        print(
            "test_upload_block_blob_include_flag failed with difference in the number of failed and successful transfers with 2 files in include flag")
        return

    # uploading the directory with sub-dir in the include flag.
    result = util.Command("copy").add_arguments(dir_n_files_path).add_arguments(util.test_container_url). \
        add_flags("recursive", "true").add_flags("log-level", "info") \
        .add_flags("include", "sub_dir_include_flag_set_upload").add_flags("output-json",
                                                                           "true").execute_azcopy_copy_command_get_output()
    # parse the result to get the last job progress summary
    result = util.parseAzcopyOutput(result)
    # parse the Json Output
    x = json.loads(result, object_hook=lambda d: namedtuple('X', d.keys())(*d.values()))
    # Number of successful transfer should be 10 and there should be not failed transfer
    if x.TransfersCompleted is not 10 and x.TransfersFailed is not 0:
        print(
            "test_upload_block_blob_include_flag failed with difference in the number of failed and successful transfers with sub-dir in include flag")
        return
    print("test_upload_block_blob_include_flag successfully passed")


# test_upload_block_blob_exclude_flag tests the exclude flag in the upload scenario
def test_upload_block_blob_exclude_flag():
    dir_name = "dir_exclude_flag_set_upload"
    # create 10 files inside the directory
    dir_n_files_path = util.create_test_n_files(1024, 10, dir_name)

    # create sub-directory inside the  dir_exclude_flag_set_upload
    sub_dir_name = os.path.join(dir_name, "sub_dir_exclude_flag_set_upload")
    # create 10 files inside the sub-dir
    sub_dir_n_file_path = util.create_test_n_files(1024, 10, sub_dir_name)

    # uploading the directory with 2 files in the exclude flag.
    result = util.Command("copy").add_arguments(dir_n_files_path).add_arguments(util.test_container_url). \
        add_flags("recursive", "true").add_flags("log-level", "info") \
        .add_flags("exclude", "test101024_2.txt;test101024_3.txt").add_flags("output-json",
                                                                             "true").execute_azcopy_copy_command_get_output()
    # parse the result to get the last job progress summary
    result = util.parseAzcopyOutput(result)
    # parse the Json Output
    x = json.loads(result, object_hook=lambda d: namedtuple('X', d.keys())(*d.values()))
    # Number of successful transfer should be 18 and there should be not failed transfer
    # Since total number of files inside dir_exclude_flag_set_upload is 20 and 2 files are set
    # to exclude, so total number of transfer should be 18
    if x.TransfersCompleted is not 18 and x.TransfersFailed is not 0:
        print(
            "test_upload_block_blob_exclude_flag failed with difference in the number of failed and successful transfers with two files in exclude flag")
        return

    # uploading the directory with sub-dir in the exclude flag.
    result = util.Command("copy").add_arguments(dir_n_files_path).add_arguments(util.test_container_url). \
        add_flags("recursive", "true").add_flags("log-level", "info") \
        .add_flags("exclude", "sub_dir_exclude_flag_set_upload").add_flags("output-json",
                                                                           "true").execute_azcopy_copy_command_get_output()
    # parse the result to get the last job progress summary
    result = util.parseAzcopyOutput(result)
    # parse the Json Output
    x = json.loads(result, object_hook=lambda d: namedtuple('X', d.keys())(*d.values()))
    # Number of successful transfer should be 10 and there should be not failed transfer
    # Since the total number of files in dir_exclude_flag_set_upload is 20 and sub_dir_exclude_flag_set_upload
    # sub-dir is set to exclude, total number of transfer will be 10
    if x.TransfersCompleted is not 10 and x.TransfersFailed is not 0:
        print(
            "test_upload_block_blob_exclude_flag failed with difference in the number of failed and successful transfers with sub-dir in exclude flag")
        return
    print("test_upload_block_blob_exclude_flag successfully passed")


def test_download_blob_include_flag():
    # create dir and 10 files of size 1024 inside it
    dir_name = "dir_include_flag_set_download"
    dir_n_files_path = util.create_test_n_files(1024, 10, dir_name)

    # create sub-dir inside dir dir_include_flag_set_download
    # create 10 files inside the sub-dir of size 1024
    sub_dir_name = os.path.join(dir_name, "sub_dir_include_flag_set_download")
    sub_dir_n_file_path = util.create_test_n_files(1024, 10, sub_dir_name)

    # uploading the directory with 20 files in it.
    result = util.Command("copy").add_arguments(dir_n_files_path).add_arguments(util.test_container_url). \
        add_flags("recursive", "true").add_flags("log-level", "info").execute_azcopy_copy_command()
    if not result:
        print("test_download_blob_include_flag failed while uploading ", 20,
              "files in dir_include_flag_set_download in the container")
        return
    # execute the validator and validating the uploaded directory.
    destination = util.get_resource_sas(dir_name)
    result = util.Command("testBlob").add_arguments(dir_n_files_path).add_arguments(destination). \
        add_flags("is-object-dir", "true").execute_azcopy_verify()
    if not result:
        print("test_download_blob_include_flag test case failed while validating the directory uploaded")

    # download from container with include flags
    destination_sas = util.get_resource_sas(dir_name)
    result = util.Command("copy").add_arguments(destination_sas).add_arguments(util.test_directory_path). \
        add_flags("recursive", "true").add_flags("log-level", "info").add_flags("output-json", "true"). \
        add_flags("include", "test101024_1.txt;test101024_2.txt;test101024_3.txt"). \
        execute_azcopy_copy_command_get_output()
    # parse the result to get the last job progress summary
    result = util.parseAzcopyOutput(result)
    # parse the Json Output
    x = json.loads(result, object_hook=lambda d: namedtuple('X', d.keys())(*d.values()))
    if x.TransfersCompleted is not 2 and x.TransfersFailed is not 0:
        print(
            "test_download_blob_include_flag failed with difference in the number of failed and successful transfers with 2 files in include flag")
        return

    # download from container with sub-dir in include flags
    destination_sas = util.get_resource_sas(dir_name)
    result = util.Command("copy").add_arguments(destination_sas).add_arguments(util.test_directory_path). \
        add_flags("recursive", "true").add_flags("log-level", "info").add_flags("output-json", "true"). \
        add_flags("include", "sub_dir_include_flag_set_download/"). \
        execute_azcopy_copy_command_get_output()
    # parse the result to get the last job progress summary
    result = util.parseAzcopyOutput(result)
    # parse the Json Output
    x = json.loads(result, object_hook=lambda d: namedtuple('X', d.keys())(*d.values()))
    if x.TransfersCompleted is not 10 and x.TransfersFailed is not 0:
        print(
            "test_download_blob_include_flag failed with difference in the number of failed and successful transfers with sub-dir in include flag")
        return
    print("test_download_blob_include_flag successfully passed")


def test_download_blob_exclude_flag():
    # create dir and 10 files of size 1024 inside it
    dir_name = "dir_exclude_flag_set_download"
    dir_n_files_path = util.create_test_n_files(1024, 10, dir_name)

    # create sub-dir inside dir dir_exclude_flag_set_download
    # create 10 files inside the sub-dir of size 1024
    sub_dir_name = os.path.join(dir_name, "sub_dir_exclude_flag_set_download")
    sub_dir_n_file_path = util.create_test_n_files(1024, 10, sub_dir_name)

    # uploading the directory with 20 files in it.
    result = util.Command("copy").add_arguments(dir_n_files_path).add_arguments(util.test_container_url). \
        add_flags("recursive", "true").add_flags("log-level", "info").execute_azcopy_copy_command()
    if not result:
        print("test_download_blob_exclude_flag failed while uploading ", 20,
              "files in dir_exclude_flag_set_download in the container")
        return
    # execute the validator and validating the uploaded directory.
    destination = util.get_resource_sas(dir_name)
    result = util.Command("testBlob").add_arguments(dir_n_files_path).add_arguments(destination). \
        add_flags("is-object-dir", "true").execute_azcopy_verify()
    if not result:
        print("test_download_blob_exclude_flag test case failed while validating the directory uploaded")

    # download from container with exclude flags
    destination_sas = util.get_resource_sas(dir_name)
    result = util.Command("copy").add_arguments(destination_sas).add_arguments(util.test_directory_path). \
        add_flags("recursive", "true").add_flags("log-level", "info").add_flags("output-json", "true"). \
        add_flags("exclude", "test101024_1.txt;test101024_2.txt;test101024_3.txt"). \
        execute_azcopy_copy_command_get_output()
    # parse the result to get the last job progress summary
    result = util.parseAzcopyOutput(result)
    # parse the Json Output
    x = json.loads(result, object_hook=lambda d: namedtuple('X', d.keys())(*d.values()))
    # Number of expected successful transfer should be 18 since two files in directory are set to exclude
    if x.TransfersCompleted is not 17 and x.TransfersFailed is not 0:
        print(
            "test_download_blob_include_flag failed with difference in the number of failed and successful transfers with 2 files in include flag")
        return

    # download from container with sub-dir in exclude flags
    destination_sas = util.get_resource_sas(dir_name)
    result = util.Command("copy").add_arguments(destination_sas).add_arguments(util.test_directory_path). \
        add_flags("recursive", "true").add_flags("log-level", "info").add_flags("output-json", "true"). \
        add_flags("exclude", "sub_dir_include_flag_set_download/"). \
        execute_azcopy_copy_command_get_output()
    # parse the result to get the last job progress summary
    result = util.parseAzcopyOutput(result)
    # parse the Json Output
    x = json.loads(result, object_hook=lambda d: namedtuple('X', d.keys())(*d.values()))
    # Number of Expected Transfer should be 10 since sub-dir is to exclude which has 10 files in it.
    if x.TransfersCompleted is not 10 and x.TransfersFailed is not 0:
        print(
            "test_download_blob_include_flag failed with difference in the number of failed and successful transfers with sub-dir in include flag")
        return
    print("test_download_blob_exclude_flag successfully passed")


def test_sync_local_to_blob_without_wildCards():
    # create 10 files inside the dir 'sync_local_blob'
    dir_name = "sync_local_blob"
    dir_n_files_path = util.create_test_n_files(1024, 10, dir_name)

    # create sub-dir inside dir sync_local_blob
    # create 10 files inside the sub-dir of size 1024
    sub_dir_name = os.path.join(dir_name, "sub_dir_sync_local_blob")
    sub_dir_n_file_path = util.create_test_n_files(1024, 10, sub_dir_name)

    # uploading the directory with 20 files in it.
    result = util.Command("copy").add_arguments(dir_n_files_path).add_arguments(util.test_container_url). \
        add_flags("recursive", "true").add_flags("log-level", "info").execute_azcopy_copy_command()
    if not result:
        print("test_sync_local_to_blob_without_wildCards failed while uploading ", 20,
              "files in sync_local_blob to the container")
        return
    # execute the validator and validating the uploaded directory.
    destination = util.get_resource_sas(dir_name)
    result = util.Command("testBlob").add_arguments(dir_n_files_path).add_arguments(destination). \
        add_flags("is-object-dir", "true").execute_azcopy_verify()
    if not result:
        print(
            "test_sync_local_to_blob_without_wildCards test case failed while validating the directory sync_upload_blob")
        return

    # download the destination to the source to match the last modified time
    result = util.Command("copy").add_arguments(destination).add_arguments(util.test_directory_path). \
        add_flags("log-level", "info").add_flags("recursive", "true").add_flags("output-json", "true"). \
        add_flags("preserve-last-modified-time", "true").execute_azcopy_copy_command_get_output()
    if not result:
        print("test_sync_local_to_blob_without_wildCards failed downloading the source ", destination,
              " to the destination ", dir_n_files_path)
        return

    # execute a sync command
    dir_sas = util.get_resource_sas(dir_name)
    result = util.Command("sync").add_arguments(dir_n_files_path).add_arguments(dir_sas). \
        add_flags("log-level", "info").add_flags("recursive", "true").execute_azcopy_copy_command()
    # since source and destination both are in sync, there should no sync and the azcopy should exit with error code
    if result:
        print("test_sync_local_to_blob_without_wildCards failed while performing a sync of ", dir_name, " and ",
              dir_sas)
        return
    try:
        shutil.rmtree(sub_dir_n_file_path)
    except:
        print("test_sync_local_to_blob_without_wildCards failed deleting the sub-directory ", sub_dir_name)
        return
    # deleted entire sub-dir inside the dir created above
    # sync between source and destination should delete the sub-dir on container
    # number of successful transfer should be equal to 10
    result = util.Command("sync").add_arguments(dir_n_files_path).add_arguments(dir_sas). \
        add_flags("log-level", "info").add_flags("recursive", "true").add_flags("output-json",
                                                                              "true").execute_azcopy_copy_command_get_output()
    # parse the result to get the last job progress summary
    result = util.parseAzcopyOutput(result)
    # parse the Json Output
    x = json.loads(result, object_hook=lambda d: namedtuple('X', d.keys())(*d.values()))
    # Number of Expected Transfer should be 10 since sub-dir is to exclude which has 10 files in it.
    if x.TransfersCompleted is not 10 and x.TransfersFailed is not 0:
        print(
            "test_sync_local_to_blob_without_wildCards failed with difference in the number of failed and successful transfers")
        return

    # delete 5 files inside the directory
    for r in range(5, 9):
        filename = "test101024_" + str(r) + ".txt"
        filepath = os.path.join(dir_n_files_path, filename)
        try:
            os.remove(filepath)
        except:
            print("test_sync_local_to_blob_without_wildCards failed removing the file ", filepath)
            return

    # sync between source and destination should delete the deleted files on container
    # number of successful transfer should be equal to 5
    result = util.Command("sync").add_arguments(dir_n_files_path).add_arguments(dir_sas). \
        add_flags("log-level", "info").add_flags("recursive", "true").add_flags("output-json",
                                                                              "true").execute_azcopy_copy_command_get_output()
    # parse the result to get the last job progress summary
    result = util.parseAzcopyOutput(result)
    # parse the Json Output
    x = json.loads(result, object_hook=lambda d: namedtuple('X', d.keys())(*d.values()))
    # Number of Expected Transfer should be 10 since 10 files were deleted
    if x.TransfersCompleted is not 10 and x.TransfersFailed is not 0:
        print(
            "test_sync_local_to_blob_without_wildCards failed with difference in the number of failed and successful transfers")
        return

    # change the modified time of file
    # perform the sync
    # expected number of transfer is 1
    filepath = os.path.join(dir_n_files_path, "test101024_0.txt")
    st = os.stat(filepath)
    atime = st[ST_ATIME]  # access time
    mtime = st[ST_MTIME]  # modification time
    new_mtime = mtime + (4 * 3600)  # new modification time
    os.utime(filepath, (atime, new_mtime))
    # sync source to destination
    result = util.Command("sync").add_arguments(dir_n_files_path).add_arguments(dir_sas). \
        add_flags("log-level", "info").add_flags("recursive", "true").add_flags("output-json",
                                                                              "true").execute_azcopy_copy_command_get_output()
    # parse the result to get the last job progress summary
    result = util.parseAzcopyOutput(result)
    # parse the Json Output
    x = json.loads(result, object_hook=lambda d: namedtuple('X', d.keys())(*d.values()))
    # Number of Expected Transfer should be 10 since 10 files were deleted
    if x.TransfersCompleted is not 1 and x.TransfersFailed is not 0:
        print(
            "test_sync_local_to_blob_without_wildCards failed with difference in the number of failed and successful transfers")
        return
    print("test_sync_local_to_blob_without_wildCards successfully passed")


def test_sync_local_to_blob_with_wildCards():
    # create 10 files inside the dir 'sync_local_blob'
    dir_name = "sync_local_blob_wc"
    dir_n_files_path = util.create_test_n_files(1024, 10, dir_name)

    # create sub-dir inside dir sync_local_blob_wc
    # create 10 files inside the sub-dir of size 1024
    sub_dir_1 = os.path.join(dir_name, "sub_dir_1")
    sub_dir1_n_file_path = util.create_test_n_files(1024, 10, sub_dir_1)

    # create sub-dir inside dir sync_local_blob_wc
    sub_dir_2 = os.path.join(dir_name, "sub_dir_2")
    sub_dir2_n_file_path = util.create_test_n_files(1024, 10, sub_dir_2)

    # uploading the directory with 30 files in it.
    result = util.Command("copy").add_arguments(dir_n_files_path).add_arguments(util.test_container_url). \
        add_flags("recursive", "true").add_flags("log-level", "info").execute_azcopy_copy_command()
    if not result:
        print("test_sync_local_to_blob_with_wildCards failed while uploading ", 30,
              "files in sync_local_blob_wc to the container")
        return

    # execute the validator and validating the uploaded directory.
    destination = util.get_resource_sas(dir_name)
    result = util.Command("testBlob").add_arguments(dir_n_files_path).add_arguments(destination). \
        add_flags("is-object-dir", "true").execute_azcopy_verify()
    if not result:
        print(
            "test_sync_local_to_blob_with_wildCards test case failed while validating the directory sync_upload_blob_wc")
        return

    # download the destination to the source to match the last modified time
    result = util.Command("copy").add_arguments(destination).add_arguments(util.test_directory_path). \
        add_flags("log-level", "info").add_flags("recursive", "true").add_flags("output-json", "true"). \
        add_flags("preserve-last-modified-time", "true").execute_azcopy_copy_command_get_output()
    if not result:
        print("test_sync_local_to_blob_with_wildCards failed downloading the source ", destination,
              " to the destination ", dir_n_files_path)
        return

    # add wildcard at the end of dirpath
    dir_n_files_path_wcard = os.path.join(dir_n_files_path, "*")
    # execute a sync command
    dir_sas = util.get_resource_sas(dir_name)
    result = util.Command("sync").add_arguments(dir_n_files_path_wcard).add_arguments(dir_sas). \
        add_flags("log-level", "info").add_flags("recursive", "true").execute_azcopy_copy_command()
    # since source and destination both are in sync, there should no sync and the azcopy should exit with error code
    if result:
        print("test_sync_local_to_blob_with_wildCards failed while performing a sync of ", dir_n_files_path_wcard,
              " and ", dir_sas)
        return

    # sync all the files the ends with .txt extension inside all sub-dirs inside inside
    # sd_dir_n_files_path_wcard is in format dir/*/*.txt
    sd_dir_n_files_path_wcard = os.path.join(dir_n_files_path_wcard, "*.txt")
    result = util.Command("sync").add_arguments(sd_dir_n_files_path_wcard).add_arguments(dir_sas). \
        add_flags("log-level", "info").add_flags("recursive", "true").execute_azcopy_copy_command()
    # since source and destination both are in sync, there should no sync and the azcopy should exit with error code
    if result:
        print("test_sync_local_to_blob_with_wildCards failed while performing a sync of ", sd_dir_n_files_path_wcard,
              " and ", dir_sas)
        return

    # remove 5 files inside both the sub-directories
    for r in range(5, 9):
        filename = "test101024_" + str(r) + ".txt"
        filepath = os.path.join(sub_dir1_n_file_path, filename)
        try:
            os.remove(filepath)
        except:
            print("test_sync_local_to_blob_with_wildCards failed removing the file ", filepath)
            return
        filepath = os.path.join(sub_dir2_n_file_path, filename)
        try:
            os.remove(filepath)
        except:
            print("test_sync_local_to_blob_with_wildCards failed removing the file ", filepath)
            return
    # sync all the files the ends with .txt extension inside all sub-dirs inside inside
    # since 5 files inside each sub-dir are deleted, sync will have total 10 transfer
    # 10 files will deleted from container
    sd_dir_n_files_path_wcard = os.path.join(dir_n_files_path_wcard, "*.txt")
    result = util.Command("sync").add_arguments(sd_dir_n_files_path_wcard).add_arguments(dir_sas). \
        add_flags("log-level", "info").add_flags("recursive", "true").add_flags("output-json",
                                                                              "true").execute_azcopy_copy_command_get_output()
    # parse the result to get the last job progress summary
    result = util.parseAzcopyOutput(result)
    # parse the Json Output
    x = json.loads(result, object_hook=lambda d: namedtuple('X', d.keys())(*d.values()))
    # Number of Expected Transfer should be 10 since 10 files were deleted
    if x.TransfersCompleted is not 10 and x.TransfersFailed is not 0:
        print(
            "test_sync_local_to_blob_with_wildCards failed with difference in the number of failed and successful transfers")
        return
    print("test_sync_local_to_blob_with_wildCards successfully passed")
