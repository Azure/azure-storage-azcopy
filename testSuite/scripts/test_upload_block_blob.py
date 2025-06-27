import json
import os
import unittest
import shutil
import time
from collections import namedtuple
from stat import *
import utility as util

class Block_Upload_User_Scenarios(unittest.TestCase):
    # test_63mb_blob_upload verifies the azcopy upload of 63mb blob upload.
    def test_63mb_blob_upload(self):
        # creating file of 63mb size.
        filename = "test63Mb_blob.txt"
        file_path = util.create_test_file(filename, 8 * 1024 * 1024)

        # execute azcopy copy upload.
        dest = util.get_resource_sas(filename)
        result = util.Command("copy").add_arguments(file_path).add_arguments(dest) \
            .add_flags("log-level", "info").add_flags("block-size-mb", "100").add_flags("recursive", "true"). \
            execute_azcopy_copy_command()
        self.assertTrue(result)

        # Verifying the uploaded blob
        # calling the testBlob validator to verify whether blob has been successfully uploaded or not
        result = util.Command("testBlob").add_arguments(file_path).add_arguments(dest).execute_azcopy_verify()
        self.assertTrue(result)

    # test_metaData_content_encoding_content_type verifies the meta data, content type,
    # content encoding of 2kb upload to container through azcopy.
    def test_blob_metadata_content_encoding_content_type(self):
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
        self.assertTrue(result)

        # execute azcopy validate order.
        # adding the source in validator as first argument.
        # adding the destination in validator as second argument.
        result = util.Command("testBlob").add_arguments(file_path).add_arguments(destination_sas).add_flags("metadata",
                                                                                                            "author=prjain;viewport=width;description=test file"). \
            add_flags("content-type", "testctype").add_flags("content-encoding", "testenc").add_flags("no-guess-mime-type",
                                                                                                      "true").execute_azcopy_verify()
        self.assertTrue(result)


    # test_1G_blob_upload verifies the azcopy upload of 1Gb blob upload in blocks of 100 Mb
    def util_test_1GB_blob_upload(self, use_oauth_session=False):
        # create 1Gb file
        filename = "test_1G_blob.txt"
        file_path = util.create_test_file(filename, 1 * 1024 * 1024 * 1024)

        # execute azcopy upload.
        if not use_oauth_session:
            dest = util.get_resource_sas(filename)
            dest_validate = dest
        else:
            dest = util.get_resource_from_oauth_container(filename)
            dest_validate = util.get_resource_from_oauth_container_validate(filename)

        result = util.Command("copy").add_arguments(file_path).add_arguments(dest).add_flags("log-level", "info"). \
            add_flags("block-size-mb", "100").add_flags("recursive", "true").execute_azcopy_copy_command()
        self.assertTrue(result)

        # Verifying the uploaded blob.
        # adding local file path as first argument.
        # adding file sas as local argument.
        # calling the testBlob validator to verify whether blob has been successfully uploaded or not.
        result = util.Command("testBlob").add_arguments(file_path).add_arguments(dest_validate).execute_azcopy_verify()
        self.assertTrue(result)

    # test_1GB_blob_upload_with_sas verifies the azcopy upload of 1Gb blob upload in blocks of 100 Mb with SAS
    def test_1GB_blob_upload_with_sas(self):
        self.util_test_1GB_blob_upload()

    # test_1GB_blob_upload_with_oauth verifies the azcopy upload of 1Gb blob upload in blocks of 100 Mb with OAuth
    @unittest.skip("single file authentication for oauth covered by small file")
    def test_1GB_blob_upload_with_oauth(self):
        self.util_test_1GB_blob_upload(True)

    # test_block_size verifies azcopy upload of blob in blocks of given block-size
    # performs the upload, verify the blob and number of blocks.
    def test_block_size(self):

        block_size_mb = 4
        # create file of size 63 Mb
        filename = "test63Mb_blob.txt"
        file_path = util.create_test_file(filename, 63 * 1024 * 1024)

        # execute azcopy upload of 63 Mb file.
        destination_sas = util.get_resource_sas(filename)
        result = util.Command("copy").add_arguments(file_path).add_arguments(destination_sas).add_flags("log-level", "info"). \
            add_flags("block-size-mb", str(block_size_mb)).add_flags("recursive", "true").execute_azcopy_copy_command()
        self.assertTrue(result)

        # Verifying the uploaded blob
        # calling the testBlob validator to verify whether blob has been successfully uploaded or not
        block_size_bytes = block_size_mb * 1024 * 1024
        if (63 * 1024 * 1024) % block_size_bytes == 0:
            number_of_blocks = int(63 * 1024 * 1024 / block_size_bytes)
        else:
            number_of_blocks = int(63 * 1024 * 1024 / block_size_bytes) + 1
        result = util.Command("testBlob").add_arguments(file_path).add_arguments(destination_sas).add_flags(
            "verify-block-size", "true").add_flags("number-blocks-or-pages", str(number_of_blocks)).execute_azcopy_verify()
        self.assertTrue(result)


    # test_guess_mime_type verifies the mime type detection by azcopy while uploading the blob
    def test_guess_mime_type(self):
        # create a test html file
        filename = "test_guessmimetype.html"
        file_path = util.create_test_html_file(filename)

        # execute azcopy upload of html file.
        destination_sas = util.get_resource_sas(filename)
        result = util.Command("copy").add_arguments(file_path).add_arguments(destination_sas).add_flags("log-level", "info"). \
            add_flags("recursive", "true").execute_azcopy_copy_command()
        self.assertTrue(result)

        # execute the validator to verify the content-type.
        result = util.Command("testBlob").add_arguments(file_path).add_arguments(destination_sas).add_flags("log-level",
                                                                                                            "info"). \
            add_flags("recursive", "true")
        self.assertTrue(result)


    def test_set_block_blob_tier(self):
        # create a file file_hot_block_blob_tier
        filename = "test_hot_block_blob_tier.txt"
        file_path = util.create_test_file(filename, 10 * 1024)

        # uploading the file file_hot_block_blob_tier using azcopy and setting the block-blob-tier to Hot
        destination_sas = util.get_resource_sas(filename)
        result = util.Command("copy").add_arguments(file_path).add_arguments(destination_sas). \
            add_flags("log-level", "info").add_flags("block-blob-tier", "Hot").execute_azcopy_copy_command()
        self.assertTrue(result)
        # execute azcopy validate order.
        # added the expected blob-tier "Hot"
        result = util.Command("testBlob").add_arguments(file_path).add_arguments(destination_sas).add_flags("blob-tier",
                                                                                                            "Hot").execute_azcopy_verify()
        self.assertTrue(result)

        # create file to upload with block blob tier set to "Cool".
        filename = "test_cool_block_blob_tier.txt"
        file_path = util.create_test_file(filename, 10 * 1024)

        # uploading the file file_cool_block_blob_tier using azcopy and setting the block-blob-tier to Cool.
        destination_sas = util.get_resource_sas(filename)
        result = util.Command("copy").add_arguments(file_path).add_arguments(destination_sas). \
            add_flags("log-level", "info").add_flags("block-blob-tier", "Cool").execute_azcopy_copy_command()
        self.assertTrue(result)

        # execute azcopy validate order.
        # added the expected blob-tier "Cool"
        result = util.Command("testBlob").add_arguments(file_path).add_arguments(destination_sas).add_flags("blob-tier",                                                                                                "Cool").execute_azcopy_verify()
        self.assertTrue(result)

        # create file to upload with block blob tier set to "Archive".
        filename = "test_archive_block_blob_tier.txt"
        file_path = util.create_test_file(filename, 10 * 1024)

        # uploading the file file_archive_block_blob_tier using azcopy and setting the block-blob-tier to Archive.
        destination_sas = util.get_resource_sas(filename)
        result = util.Command("copy").add_arguments(file_path).add_arguments(destination_sas). \
            add_flags("log-level", "info").add_flags("block-blob-tier", "archive").execute_azcopy_copy_command()
        self.assertTrue(result)

        # execute azcopy validate order.
        # added the expected blob-tier "Archive"
        result = util.Command("testBlob").add_arguments(file_path).add_arguments(destination_sas).add_flags("blob-tier",
                                                                                                            "Archive").execute_azcopy_verify()
        self.assertTrue(result)

    def test_force_flag_set_to_false_upload(self):
        # creating directory with 20 files in it.
        dir_name = "dir_force_flag_set_upload"
        dir_n_files_path = util.create_test_n_files(1024, 20, dir_name)
        # uploading the directory with 20 files in it.
        result = util.Command("copy").add_arguments(dir_n_files_path).add_arguments(util.test_container_url). \
            add_flags("recursive", "true").add_flags("log-level", "info").execute_azcopy_copy_command()
        self.assertTrue(result)

        # execute the validator and validating the uploaded directory.
        destination = util.get_resource_sas(dir_name)
        result = util.Command("testBlob").add_arguments(dir_n_files_path).add_arguments(destination). \
            add_flags("is-object-dir", "true").execute_azcopy_verify()
        self.assertTrue(result)

        # uploading the directory again with force flag set to false.
        result = util.Command("copy").add_arguments(dir_n_files_path).add_arguments(util.test_container_url). \
            add_flags("recursive", "true").add_flags("overwrite", "false").add_flags("log-level", "info"). \
            add_flags("output-type", "json").execute_azcopy_copy_command_get_output()
        self.assertNotEquals(result, None)

        # parsing the json and comparing the number of failed and successful transfers.
        result = util.parseAzcopyOutput(result)
        try:
            x = json.loads(result, object_hook=lambda d: namedtuple('X', d.keys())(*d.values()))
        except:
            self.fail('error parsing the output in Json Format')
        self.assertEqual(x.TransfersSkipped, "20")
        self.assertEqual(x.TransfersCompleted, "0")

        # uploading a sub-directory inside the above dir with 20 files inside the sub-directory.
        # total number of file inside the dir is 40
        sub_dir_name = os.path.join(dir_name + "/sub_dir_force_flag_set_upload")
        sub_dir_n_files_path = util.create_test_n_files(1024, 20, sub_dir_name)

        result = util.Command("copy").add_arguments(dir_n_files_path).add_arguments(util.test_container_url). \
            add_flags("recursive", "true").add_flags("log-level", "info").execute_azcopy_copy_command()
        self.assertTrue(result)

        # execute the validator and verifying the uploaded sub directory.
        sub_directory_resource_sas = util.get_resource_sas(sub_dir_name)

        result = util.Command("testBlob").add_arguments(sub_dir_n_files_path).add_arguments(sub_directory_resource_sas). \
            add_flags("is-object-dir", "true").execute_azcopy_verify()
        self.assertTrue(result)

        # removing the sub directory.
        result = util.Command("rm").add_arguments(sub_directory_resource_sas). \
            add_flags("log-level", "info").add_flags("recursive", "true").execute_azcopy_copy_command()
        self.assertTrue(result)

        # uploading the directory again with force flag set to false.
        result = util.Command("copy").add_arguments(dir_n_files_path).add_arguments(util.test_container_url). \
            add_flags("recursive", "true").add_flags("overwrite", "false").add_flags("log-level", "info"). \
            add_flags("output-type", "json").execute_azcopy_copy_command_get_output()
        self.assertNotEquals(result, None)

        # parsing the json and comparing the number of failed and successful transfers.
        # Number of failed transfers should be 20 and number of successful transfer should be 20.
        result = util.parseAzcopyOutput(result)
        try:
            x = json.loads(result, object_hook=lambda d: namedtuple('X', d.keys())(*d.values()))
        except:
            self.fail('error parsing the output in json format')
        self.assertEqual(x.TransfersCompleted, "20")
        self.assertEqual(x.TransfersSkipped, "20")


    def test_force_flag_set_to_false_download(self):
        # creating directory with 20 files in it.
        dir_name = "dir_force_flag_set_download"
        dir_n_files_path = util.create_test_n_files(1024, 20, dir_name)
        # uploading the directory with 20 files in it.
        result = util.Command("copy").add_arguments(dir_n_files_path).add_arguments(util.test_container_url). \
            add_flags("recursive", "true").add_flags("log-level", "info").execute_azcopy_copy_command()
        self.assertTrue(result)

        # execute the validator and validating the uploaded directory.
        destination = util.get_resource_sas(dir_name)
        result = util.Command("testBlob").add_arguments(dir_n_files_path).add_arguments(destination). \
            add_flags("is-object-dir", "true").execute_azcopy_verify()
        self.assertTrue(result)

        # removing the directory dir_force_flag_set_download
        try:
            shutil.rmtree(dir_n_files_path)
        except:
            self.fail('error removing the directory ' + dir_n_files_path)

        # downloading the directory created from container through azcopy with recursive flag to true.
        result = util.Command("copy").add_arguments(destination).add_arguments(util.test_directory_path).add_flags(
            "log-level", "info").add_flags("recursive", "true").execute_azcopy_copy_command()
        self.assertTrue(result)

        # verify downloaded blob.
        result = util.Command("testBlob").add_arguments(dir_n_files_path).add_arguments(destination).add_flags(
            "is-object-dir", "true").execute_azcopy_verify()
        self.assertTrue(result)

        # downloading the directory created from container through azcopy with recursive flag to true and force flag set to false.
        result = util.Command("copy").add_arguments(destination).add_arguments(util.test_directory_path).add_flags(
            "log-level", "info"). \
            add_flags("recursive", "true").add_flags("overwrite", "false").add_flags("output-type",
                                                                                 "json").execute_azcopy_copy_command_get_output()
        result = util.parseAzcopyOutput(result)
        try:
            x = json.loads(result, object_hook=lambda d: namedtuple('X', d.keys())(*d.values()))
        except:
            self.fail('error parsing the output in Json Format')
        # Since all files exists locally and overwrite flag is set to false, all 20 transfers will be skipped
        self.assertEqual(x.TransfersSkipped, "20")
        self.assertEqual(x.TransfersCompleted, "0")

        # removing 5 files with suffix from 10 to 14
        for index in range(10, 15):
            file_path_remove = dir_n_files_path + os.sep + "test201024" + "_" + str(index) + ".txt"
            try:
                os.remove(file_path_remove)
            except:
                self.fail('error deleting the file ' + file_path_remove)

        # downloading the directory created from container through azcopy with recursive flag to true and force flag set to false.
        # 5 deleted files should be downloaded. Number of failed transfer should be 15 and number of completed transfer should be 5
        result = util.Command("copy").add_arguments(destination).add_arguments(util.test_directory_path).add_flags(
            "log-level", "info"). \
            add_flags("recursive", "true").add_flags("overwrite", "false").add_flags("output-type",
                                                                                 "json").execute_azcopy_copy_command_get_output()
        result = util.parseAzcopyOutput(result)
        try:
            x = json.loads(result, object_hook=lambda d: namedtuple('X', d.keys())(*d.values()))
        except:
            self.fail('error parsing the output in Json Format')
        self.assertEqual(x.TransfersSkipped, "15")
        self.assertEqual(x.TransfersCompleted, "5")

    def test_overwrite_flag_set_to_if_source_new_upload(self):
        # creating directory with 20 files in it.
        dir_name = "dir_overwrite_flag_set_upload"
        dir_n_files_path = util.create_test_n_files(1024, 20, dir_name)

        # uploading the directory with 20 files in it. Wait a bit so that the lmt of the source is in the past
        time.sleep(2)
        result = util.Command("copy").add_arguments(dir_n_files_path).add_arguments(util.test_container_url). \
            add_flags("recursive", "true").add_flags("log-level", "info").execute_azcopy_copy_command()
        self.assertTrue(result)

        # uploading the directory again with force flag set to ifSourceNewer.
        result = util.Command("copy").add_arguments(dir_n_files_path).add_arguments(util.test_container_url). \
            add_flags("recursive", "true").add_flags("overwrite", "ifSourceNewer").add_flags("log-level", "info"). \
            add_flags("output-type", "json").execute_azcopy_copy_command_get_output()
        self.assertNotEquals(result, None)

        # parsing the json and comparing the number of failed and successful transfers.
        result = util.parseAzcopyOutput(result)
        try:
            x = json.loads(result, object_hook=lambda d: namedtuple('X', d.keys())(*d.values()))
        except:
            self.fail('error parsing the output in Json Format')
        self.assertEqual(x.TransfersSkipped, "20")
        self.assertEqual(x.TransfersCompleted, "0")

        time.sleep(10)
        # refresh the lmts of the source files so that they appear newer
        for filename in os.listdir(dir_n_files_path):
            # update the lmts of the files to the latest
            os.utime(os.path.join(dir_n_files_path, filename), None)

        # uploading the directory again with force flag set to ifSourceNewer.
        result = util.Command("copy").add_arguments(dir_n_files_path).add_arguments(util.test_container_url). \
            add_flags("recursive", "true").add_flags("overwrite", "ifSourceNewer").add_flags("log-level", "info"). \
            add_flags("output-type", "json").execute_azcopy_copy_command_get_output()
        self.assertNotEquals(result, None)

        # parsing the json and comparing the number of failed and successful transfers.
        result = util.parseAzcopyOutput(result)
        try:
            x = json.loads(result, object_hook=lambda d: namedtuple('X', d.keys())(*d.values()))
        except:
            self.fail('error parsing the output in Json Format')
        self.assertEqual(x.TransfersSkipped, "0")
        self.assertEqual(x.TransfersCompleted, "20")

    def test_overwrite_flag_set_to_if_source_new_download(self):
        # creating directory with 20 files in it.
        dir_name = "dir_overwrite_flag_set_download_setup"
        dir_n_files_path = util.create_test_n_files(1024, 20, dir_name)
        # uploading the directory with 20 files in it.
        result = util.Command("copy").add_arguments(dir_n_files_path).add_arguments(util.test_container_url). \
            add_flags("recursive", "true").add_flags("log-level", "info").execute_azcopy_copy_command()
        self.assertTrue(result)

        # case 1: destination is empty
        # download the directory with force flag set to ifSourceNewer.
        # target an empty folder, so the download should succeed normally
        # sleep a bit so that the lmts of the source blobs are in the past
        time.sleep(10)
        source = util.get_resource_sas(dir_name)
        destination = os.path.join(util.test_directory_path, "dir_overwrite_flag_set_download")
        os.mkdir(destination)
        result = util.Command("copy").add_arguments(source).add_arguments(destination). \
            add_flags("recursive", "true").add_flags("overwrite", "ifSourceNewer").add_flags("log-level", "info"). \
            add_flags("output-type", "json").execute_azcopy_copy_command_get_output()
        self.assertNotEquals(result, None)

        # parsing the json and comparing the number of failed and successful transfers.
        result = util.parseAzcopyOutput(result)
        try:
            x = json.loads(result, object_hook=lambda d: namedtuple('X', d.keys())(*d.values()))
        except:
            self.fail('error parsing the output in Json Format')
        self.assertEqual(x.TransfersSkipped, "0")
        self.assertEqual(x.TransfersCompleted, "20")

        # case 2: local files are newer
        # download the directory again with force flag set to ifSourceNewer.
        # this time, since the local files are newer, no download should occur
        result = util.Command("copy").add_arguments(source).add_arguments(destination). \
            add_flags("recursive", "true").add_flags("overwrite", "ifSourceNewer").add_flags("log-level", "info"). \
            add_flags("output-type", "json").execute_azcopy_copy_command_get_output()
        self.assertNotEquals(result, None)

        # parsing the json and comparing the number of failed and successful transfers.
        result = util.parseAzcopyOutput(result)
        try:
            x = json.loads(result, object_hook=lambda d: namedtuple('X', d.keys())(*d.values()))
        except:
            self.fail('error parsing the output in Json Format')
        self.assertEqual(x.TransfersSkipped, "20")
        self.assertEqual(x.TransfersCompleted, "0")

        # re-uploading the directory with 20 files in it, to refresh the lmts of the source
        time.sleep(2)
        result = util.Command("copy").add_arguments(dir_n_files_path).add_arguments(util.test_container_url). \
            add_flags("recursive", "true").add_flags("log-level", "info").execute_azcopy_copy_command()
        self.assertTrue(result)

        # case 3: source blobs are newer now, so download should proceed
        result = util.Command("copy").add_arguments(source).add_arguments(destination). \
            add_flags("recursive", "true").add_flags("overwrite", "ifSourceNewer").add_flags("log-level", "info"). \
            add_flags("output-type", "json").execute_azcopy_copy_command_get_output()
        self.assertNotEquals(result, None)

        # parsing the json and comparing the number of failed and successful transfers.
        result = util.parseAzcopyOutput(result)
        try:
            x = json.loads(result, object_hook=lambda d: namedtuple('X', d.keys())(*d.values()))
        except:
            self.fail('error parsing the output in Json Format')
        self.assertEqual(x.TransfersSkipped, "0")
        self.assertEqual(x.TransfersCompleted, "20")

    # test_upload_block_blob_include_flag tests the include flag in the upload scenario
    def test_upload_block_blob_include_flag(self):
        dir_name = "dir_include_flag_set_upload"
        # create 10 files inside the directory
        dir_n_files_path = util.create_test_n_files(1024, 10, dir_name)

        # create sub-directory inside the  dir_include_flag_set_upload
        sub_dir_name = os.path.join(dir_name, "sub_dir_include_flag_set_upload")
        # create 10 files inside the sub-dir
        sub_dir_n_file_path = util.create_test_n_files(1024, 10, sub_dir_name)

        # uploading the directory with 2 file names (4 files) in the include flag.
        result = util.Command("copy").add_arguments(dir_n_files_path).add_arguments(util.test_container_url). \
            add_flags("recursive", "true").add_flags("log-level", "info") \
            .add_flags("include-pattern", "test101024_2.txt;test101024_3.txt").add_flags("output-type",
                                                                                 "json").execute_azcopy_copy_command_get_output()
        # parse the result to get the last job progress summary
        result = util.parseAzcopyOutput(result)
        # parse the Json Output
        try:
            x = json.loads(result, object_hook=lambda d: namedtuple('X', d.keys())(*d.values()))
        except:
            self.fail('error parsing output in Json format')
        # Number of successful transfer should be 4 and there should be not a failed transfer
        self.assertEqual(x.TransfersCompleted, "4")
        self.assertEqual(x.TransfersFailed, "0")

        # uploading the directory with sub-dir in the include flag.
        result = util.Command("copy").add_arguments(dir_n_files_path).add_arguments(util.test_container_url). \
            add_flags("recursive", "true").add_flags("log-level", "info") \
            .add_flags("include-path", "sub_dir_include_flag_set_upload/").add_flags("output-type",
                                                                               "json").execute_azcopy_copy_command_get_output()
        # parse the result to get the last job progress summary
        result = util.parseAzcopyOutput(result)
        try:
            # parse the Json Output
            x = json.loads(result, object_hook=lambda d: namedtuple('X', d.keys())(*d.values()))
        except:
            self.fail('error parsing the output in Json Format')
        # Number of successful transfer should be 10 and there should be not failed transfer
        self.assertEqual(x.TransfersCompleted, "10")
        self.assertEqual(x.TransfersFailed, "0")

    # test_upload_block_blob_exclude_flag tests the exclude flag in the upload scenario
    def test_upload_block_blob_exclude_flag(self):
        dir_name = "dir_exclude_flag_set_upload"
        # create 10 files inside the directory
        dir_n_files_path = util.create_test_n_files(1024, 10, dir_name)

        # create sub-directory inside the  dir_exclude_flag_set_upload
        sub_dir_name = os.path.join(dir_name, "sub_dir_exclude_flag_set_upload")
        # create 10 files inside the sub-dir
        sub_dir_n_file_path = util.create_test_n_files(1024, 10, sub_dir_name)

        # uploading the directory with 2 file names (4 total) in the exclude flag.
        result = util.Command("copy").add_arguments(dir_n_files_path).add_arguments(util.test_container_url). \
            add_flags("recursive", "true").add_flags("log-level", "info") \
            .add_flags("exclude-pattern", "test101024_2.txt;test101024_3.txt").add_flags("output-type",
                                                                                 "json").execute_azcopy_copy_command_get_output()
        # parse the result to get the last job progress summary
        result = util.parseAzcopyOutput(result)
        try:
            # parse the Json Output
            x = json.loads(result, object_hook=lambda d: namedtuple('X', d.keys())(*d.values()))
        except:
            self.fail('error parsing the output in Json Format')
        # Number of successful transfer should be 16 and there should be not failed transfer
        # Since total number of files inside dir_exclude_flag_set_upload is 20 and 4 files are set
        # to exclude, so total number of transfer should be 16
        self.assertEqual(x.TransfersCompleted, "16")
        self.assertEqual(x.TransfersFailed, "0")

        # uploading the directory with sub-dir in the exclude flag.
        result = util.Command("copy").add_arguments(dir_n_files_path).add_arguments(util.test_container_url). \
            add_flags("recursive", "true").add_flags("log-level", "info") \
            .add_flags("exclude-path", "sub_dir_exclude_flag_set_upload/").add_flags("output-type",
                                                                               "json").execute_azcopy_copy_command_get_output()
        # parse the result to get the last job progress summary
        result = util.parseAzcopyOutput(result)
        try:
            # parse the Json Output
            x = json.loads(result, object_hook=lambda d: namedtuple('X', d.keys())(*d.values()))
        except:
            self.fail('error parsing the output in Json Format')

        # Number of successful transfer should be 10 and there should be not failed transfer
        # Since the total number of files in dir_exclude_flag_set_upload is 20 and sub_dir_exclude_flag_set_upload
        # sub-dir is set to exclude, total number of transfer will be 10
        self.assertEqual(x.TransfersCompleted, "10")
        self.assertEqual(x.TransfersFailed, "0")

    def test_download_blob_include_flag(self):
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
        self.assertTrue(result)

        # execute the validator and validating the uploaded directory.
        destination = util.get_resource_sas(dir_name)
        result = util.Command("testBlob").add_arguments(dir_n_files_path).add_arguments(destination). \
            add_flags("is-object-dir", "true").execute_azcopy_verify()
        self.assertTrue(result)

        # download from container with include flags
        destination_sas = util.get_resource_sas(dir_name)
        result = util.Command("copy").add_arguments(destination_sas).add_arguments(util.test_directory_path). \
            add_flags("recursive", "true").add_flags("log-level", "info").add_flags("output-type", "json"). \
            add_flags("include-pattern", "test101024_1.txt;test101024_2.txt;test101024_3.txt"). \
            execute_azcopy_copy_command_get_output()
        # parse the result to get the last job progress summary
        result = util.parseAzcopyOutput(result)
        try:
            # parse the Json Output
            x = json.loads(result, object_hook=lambda d: namedtuple('X', d.keys())(*d.values()))
        except:
            self.fail('error parsing the output in Json Format')
        self.assertEqual(x.TransfersCompleted, "6")
        self.assertEqual(x.TransfersFailed, "0")

        # download from container with sub-dir in include flags
        # TODO: Make this use include-path in the DL refactor
        destination_sas = util.get_resource_sas(dir_name)
        result = util.Command("copy").add_arguments(destination_sas).add_arguments(util.test_directory_path). \
            add_flags("recursive", "true").add_flags("log-level", "info").add_flags("output-type", "json"). \
            add_flags("include-path", "sub_dir_include_flag_set_download/"). \
            execute_azcopy_copy_command_get_output()
        # parse the result to get the last job progress summary
        result = util.parseAzcopyOutput(result)
        try:
            # parse the Json Output
            x = json.loads(result, object_hook=lambda d: namedtuple('X', d.keys())(*d.values()))
        except:
            self.fail('error parsing the output in Json Format')
        self.assertEqual(x.TransfersCompleted, "10")
        self.assertEqual(x.TransfersFailed, "0")

    def test_download_blob_exclude_flag(self):
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
        self.assertTrue(result)

        # execute the validator and validating the uploaded directory.
        destination = util.get_resource_sas(dir_name)
        result = util.Command("testBlob").add_arguments(dir_n_files_path).add_arguments(destination). \
            add_flags("is-object-dir", "true").execute_azcopy_verify()
        self.assertTrue(result)

        # download from container with exclude flags
        destination_sas = util.get_resource_sas(dir_name)
        result = util.Command("copy").add_arguments(destination_sas).add_arguments(util.test_directory_path). \
            add_flags("recursive", "true").add_flags("log-level", "info").add_flags("output-type", "json"). \
            add_flags("exclude-pattern", "test101024_1.txt;test101024_2.txt;test101024_3.txt"). \
            execute_azcopy_copy_command_get_output()
        # parse the result to get the last job progress summary
        result = util.parseAzcopyOutput(result)
        try:
            # parse the Json Output
            x = json.loads(result, object_hook=lambda d: namedtuple('X', d.keys())(*d.values()))
        except:
            self.fail('error parsing the output in JSON Format')
        # Number of expected successful transfer should be 18 since two files in directory are set to exclude
        self.assertEqual(x.TransfersCompleted, "14")
        self.assertEqual(x.TransfersFailed, "0")

        # download from container with sub-dir in exclude flags
        destination_sas = util.get_resource_sas(dir_name)
        result = util.Command("copy").add_arguments(destination_sas).add_arguments(util.test_directory_path). \
            add_flags("recursive", "true").add_flags("log-level", "info").add_flags("output-type", "json"). \
            add_flags("exclude-path", "sub_dir_exclude_flag_set_download/"). \
            execute_azcopy_copy_command_get_output()

        # parse the result to get the last job progress summary
        result = util.parseAzcopyOutput(result)
        try:
            # parse the Json Output
            x = json.loads(result, object_hook=lambda d: namedtuple('X', d.keys())(*d.values()))
        except:
            self.fail('error parsing the output in Json Format')

        # Number of Expected Transfer should be 10 since sub-dir is to exclude which has 10 files in it.
        self.assertEqual(x.TransfersCompleted, "10")
        self.assertEqual(x.TransfersFailed, "0")

    def test_0KB_blob_upload(self):
        # Creating a single File Of size 0 KB
        filename = "test0KB.txt"
        file_path = util.create_test_file(filename, 0)

        # executing the azcopy command to upload the 0KB file.
        src = file_path
        dest = util.get_resource_sas(filename)
        result = util.Command("copy").add_arguments(src).add_arguments(dest). \
            add_flags("log-level", "info").add_flags("recursive", "true").execute_azcopy_copy_command()
        self.assertTrue(result)

        # Verifying the uploaded blob.
        # the resource local path should be the first argument for the azcopy validator.
        # the resource sas should be the second argument for azcopy validator.
        result = util.Command("testBlob").add_arguments(file_path).add_arguments(dest).execute_azcopy_verify()
        self.assertTrue(result)

    def test_upload_hidden_file(self):
        # Create directory for storing the hidden files
        dir_name = "dir_hidden_files"
        dir_path = os.path.join(util.test_directory_path, dir_name)
        try:
            shutil.rmtree(dir_path)
        except:
            print("")
        finally:
            os.mkdir(dir_path)
        for i in range(0, 10):
            file_name = "hidden_" + str(i) + ".txt"
            util.create_hidden_file(dir_path, file_name, "hidden file text")

        result = util.Command("copy").add_arguments(dir_path).add_arguments(util.test_container_url). \
            add_flags("log-level", "Info").add_flags("recursive", "true").add_flags("output-type", "json").execute_azcopy_copy_command_get_output()

        result = util.parseAzcopyOutput(result)
        try:
            # parse the Json Output
            x = json.loads(result, object_hook=lambda d: namedtuple('X', d.keys())(*d.values()))
        except:
            self.fail('error parsing the output in Json Format')
        self.assertEqual(x.TransfersCompleted, "10")
        self.assertEqual(x.TransfersFailed, "0")

    def test_upload_download_file_non_ascii_characters(self):
        file_name = u"Espa\u00F1a"
        #file_name = "abc.txt"
        file_path = util.create_file_in_path(util.test_directory_path, file_name, "non ascii characters")
        # Upload the file
        result = util.Command("copy").add_arguments(file_path).add_arguments(util.test_container_url).\
                add_flags("log-level", "Info").add_flags("output-type", "json").execute_azcopy_copy_command_get_output()
        result = util.parseAzcopyOutput(result)
        try:
            # parse the Json Output
            x = json.loads(result, object_hook=lambda d: namedtuple('X', d.keys())(*d.values()))
        except:
            self.fail('error parsing the output in Json Format')

        self.assertEqual(x.TransfersCompleted, "1")
        self.assertEqual(x.TransfersFailed, "0")

        #download the file
        dir_path = os.path.join(util.test_directory_path, "non-ascii-dir")
        try:
            shutil.rmtree(dir_path)
        except:
            print("")
        finally:
            os.mkdir(dir_path)
        destination_url = util.get_resource_sas(file_name)
        result = util.Command("copy").add_arguments(destination_url).add_arguments(dir_path).\
                add_flags("log-level", "Info").add_flags("output-type", "json").execute_azcopy_copy_command_get_output()
        result = util.parseAzcopyOutput(result)
        try:
            # parse the Json Output
            x = json.loads(result, object_hook=lambda d: namedtuple('X', d.keys())(*d.values()))
        except:
            self.fail('error parsing the output in Json Format')
        self.assertEqual(x.TransfersCompleted, "1")
        self.assertEqual(x.TransfersFailed, "0")

    def test_long_file_path_upload_with_nested_directories(self):
        dir_name = "dir_lfpupwnds"
        dir_path = util.create_test_n_files(1024, 10, dir_name)
        parent_dir = dir_name
        for i in range(0, 30):
            sub_dir_name = "s_" + str(i)
            parent_dir = os.path.join(parent_dir, sub_dir_name)
            util.create_test_n_files(1024, 10, parent_dir)

        # Upload the file
        result = util.Command("copy").add_arguments(dir_path).add_arguments(util.test_container_url). \
            add_flags("log-level", "Info").add_flags("output-type", "json").add_flags("recursive", "true").execute_azcopy_copy_command_get_output()
        result = util.parseAzcopyOutput(result)
        try:
            # parse the Json Output
            x = json.loads(result, object_hook=lambda d: namedtuple('X', d.keys())(*d.values()))
        except:
            self.fail('error parsing the output in Json Format')

        self.assertEqual(x.TransfersCompleted, "310")
        self.assertEqual(x.TransfersFailed, "0")

    def test_follow_symlinks_upload(self):
        link_name = "dir_link"
        outside_dir = "dir_outside_linkto"
        home_dir = "dir_home_follow_symlink_upload"

        # produce all necessary paths
        outside_path = util.create_test_n_files(1024, 10, outside_dir)
        home_path = util.create_test_dir(home_dir)
        link_path = os.path.join(home_path, link_name)

        # Create the symlink
        os.symlink(outside_path, link_path, target_is_directory=True)

        # Upload home path
        result = util.Command("copy").add_arguments(home_path).add_arguments(util.test_container_url). \
            add_flags("log-level", "Info").add_flags("recursive", "true").add_flags("output-type", "json"). \
            add_flags("follow-symlinks", "true").execute_azcopy_copy_command_get_output()
        result = util.parseAzcopyOutput(result)

        try:
            # parse the JSON output
            x = json.loads(result, object_hook=lambda d: namedtuple('X', d.keys())(*d.values()))
        except:
            self.fail('error parsing the output in JSON format')

        self.assertEqual(x.TransfersCompleted, "10")
        self.assertEqual(x.TransfersFailed, "0")