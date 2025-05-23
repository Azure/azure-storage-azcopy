import json
import os
import shutil
import time
import urllib
import urllib.parse as urlparse
from collections import namedtuple
import utility as util
import unittest

class Blob_Download_User_Scenario(unittest.TestCase):
    # test_download_1kb_blob_to_null verifies that a 1kb blob can be downloaded to null and the md5 can be checked successfully
    def test_download_1kb_blob_to_null(self):
        # create file of size 1kb
        filename = "test_1kb_blob_upload_download_null.txt"
        file_path = util.create_test_file(filename, 1024)

        # upload 1kb using azcopy
        src = file_path
        dst = util.test_container_url
        result = util.Command("copy").add_arguments(src).add_arguments(dst). \
            add_flags("log-level", "info").execute_azcopy_copy_command()
        self.assertTrue(result)

        # verify the uploaded blob
        resource_url = util.get_resource_sas(filename)
        result = util.Command("testBlob").add_arguments(file_path).add_arguments(resource_url).execute_azcopy_verify()
        self.assertTrue(result)

        # downloading the uploaded blob to devnull
        # note we have no tests to verify the success of check-md5. TODO: remove this when fault induction is introduced
        src = util.get_resource_sas(filename)
        dst = os.devnull
        result = util.Command("copy").add_arguments(src).add_arguments(dst).add_flags("log-level", "info")

    def test_download_1kb_blob_to_root(self):
        # create file of size 1kb
        filename = "test_1kb_blob_upload_download_null.txt"
        file_path = util.create_test_file(filename, 1024)

        # upload 1kb using azcopy
        src = file_path
        dst = util.test_container_url
        result = util.Command("copy").add_arguments(src).add_arguments(dst). \
            add_flags("log-level", "info").execute_azcopy_copy_command()
        self.assertTrue(result)

        # verify the uploaded blob
        resource_url = util.get_resource_sas(filename)
        result = util.Command("testBlob").add_arguments(file_path).add_arguments(resource_url).execute_azcopy_verify()
        self.assertTrue(result)

        # downloading the uploaded blob to devnull
        # note we have no tests to verify the success of check-md5. TODO: remove this when fault induction is introduced
        src = util.get_resource_sas(filename)
        dst = "/"
        result = util.Command("copy").add_arguments(src).add_arguments(dst).add_flags("log-level", "info")

    # test_download_preserve_last_modified_time verifies the azcopy downloaded file
    # and its modified time preserved locally on disk
    def test_blob_download_preserve_last_modified_time(self):
        # create a file of 2KB
        filename = "test_upload_preserve_last_mtime.txt"
        file_path = util.create_test_file(filename, 2048)

        # upload file through azcopy.
        destination_sas = util.get_resource_sas(filename)
        result = util.Command("copy").add_arguments(file_path).add_arguments(destination_sas). \
            add_flags("log-level", "info").add_flags("recursive", "true").execute_azcopy_copy_command()
        self.assertTrue(result)

        # Verifying the uploaded blob
        result = util.Command("testBlob").add_arguments(file_path).add_arguments(destination_sas).execute_azcopy_verify()
        self.assertTrue(result)

        time.sleep(5)

        # download file through azcopy with flag preserve-last-modified-time set to true
        download_file_name = util.test_directory_path + "/test_download_preserve_last_mtime.txt"
        result = util.Command("copy").add_arguments(destination_sas).add_arguments(download_file_name)\
                    .add_flags("log-level","info").add_flags("preserve-last-modified-time", "true").\
                    execute_azcopy_copy_command()
        self.assertTrue(result)

        # Verifying the downloaded blob and its modified with the modified time of blob.
        result = util.Command("testBlob").add_arguments(download_file_name).add_arguments(destination_sas).add_flags(
            "preserve-last-modified-time", "true").execute_azcopy_verify()
        self.assertTrue(result)

    # test_blob_download_63mb_in_4mb downloads 63mb file in block of 4mb through azcopy
    def test_blob_download_63mb_in_4mb(self):
        # create file of 63mb
        file_name = "test_63mb_in4mb_upload.txt"
        file_path = util.create_test_file(file_name, 63 * 1024 * 1024)

        # uploading file through azcopy with flag block-size set to 4mb
        destination_sas = util.get_resource_sas(file_name)
        result = util.Command("copy").add_arguments(file_path).add_arguments(destination_sas).\
                    add_flags("log-level","info").add_flags("block-size-mb", "4").execute_azcopy_copy_command()
        self.assertTrue(result)

        # verify the uploaded file.
        result = util.Command("testBlob").add_arguments(file_path).add_arguments(destination_sas).execute_azcopy_verify()
        self.assertTrue(result)

        # downloading the created parallelly in blocks of 4mb file through azcopy.
        download_file = util.test_directory_path + "/test_63mb_in4mb_download.txt"
        result = util.Command("copy").add_arguments(destination_sas).add_arguments(download_file)\
                    .add_flags("log-level","info").add_flags("block-size-mb", "4").execute_azcopy_copy_command()
        self.assertTrue(result)

        # verify the downloaded file
        result = util.Command("testBlob").add_arguments(download_file).add_arguments(
            destination_sas).execute_azcopy_verify()
        self.assertTrue(result)

    def test_blob_download_with_special_characters(self):
        filename_special_characters = "abc|>rd*"
        # encode filename beforehand to avoid erroring out
        resource_url = util.get_resource_sas(filename_special_characters.replace("*", "%2A"))
        # creating the file with random characters and with file name having special characters.
        result = util.Command("create").add_arguments(resource_url).add_flags("serviceType", "Blob").add_flags(
            "resourceType", "SingleFile").add_flags("blob-size", "1024").execute_azcopy_verify()
        self.assertTrue(result)

        # downloading the blob created above.
        result = util.Command("copy").add_arguments(resource_url).add_arguments(util.test_directory_path).add_flags(
            "log-level", "info").execute_azcopy_copy_command()
        self.assertTrue(result)

        expected_filename = filename_special_characters
        if os.name == "nt":
            # Windows will encode special characters.
            expected_filename = urllib.parse.quote_plus(filename_special_characters)
        # verify if the downloaded file exists or not.
        filepath = util.test_directory_path + "/" + expected_filename
        self.assertTrue(os.path.isfile(filepath))

        # verify the downloaded blob.
        result = util.Command("testBlob").add_arguments(filepath).add_arguments(resource_url).execute_azcopy_verify()
        self.assertTrue(result)

    def test_blob_download_wildcard_recursive_false_1(self):
        #This test verifies the azcopy behavior when wildcards are
        # provided in the source and recursive flag is set to false
        # example src = https://<container>/<vd-1>/*?<sig> recursive = false
        dir_name = "dir_download_wildcard_recursive_false_1"
        dir_path = util.create_test_n_files(1024, 10, dir_name)

        #create sub-directory inside directory
        sub_dir_name = os.path.join(dir_name, "sub_dir_download_wildcard_recursive_false_1")
        sub_dir_path = util.create_test_n_files(1024, 10, sub_dir_name)

        #upload the directory with 20 files
        # upload the directory
        # execute azcopy command
        result = util.Command("copy").add_arguments(dir_path).add_arguments(util.test_container_url). \
            add_flags("recursive", "true").add_flags("log-level", "info").execute_azcopy_copy_command()
        self.assertTrue(result)

        # execute the validator.
        dir_sas = util.get_resource_sas(dir_name)
        result = util.Command("testBlob").add_arguments(dir_path).add_arguments(dir_sas). \
            add_flags("is-object-dir", "true").execute_azcopy_verify()
        self.assertTrue(result)

        # Dir dir_download_wildcard_recursive_false_1 inside the container is attempted to download
        # but recursive flag is set to false, so no files will be downloaded
        dir_sas = util.get_resource_sas(dir_name)
        result = util.Command("copy").add_arguments(dir_sas).add_arguments(dir_path). \
            add_flags("log-level", "Info").add_flags("output-type","json").execute_azcopy_copy_command()
        self.assertEqual(result, False)

        # create the resource sas
        dir_sas = util.get_resource_sas(dir_name + "/*")
        #download the directory
        result = util.Command("copy").add_arguments(dir_sas).add_arguments(dir_path). \
            add_flags("log-level", "Info").add_flags("output-type","json").\
            execute_azcopy_copy_command_get_output()
        # parse the result to get the last job progress summary
        result = util.parseAzcopyOutput(result)
        try:
            # parse the Json Output
            x = json.loads(result, object_hook=lambda d: namedtuple('X', d.keys())(*d.values()))
        except:
            self.fail('error parsing the output in Json Format')
        # since the wildcards '*' exists at the end of dir_name in the sas
        # and recursive is set to false, files inside dir will be download
        # and not files inside the sub-dir
        # Number of Expected Transfer should be 10
        self.assertEqual(x.TransfersCompleted, "10")
        self.assertEqual(x.TransfersFailed, "0")

        # create the resource sas
        dir_sas = util.get_resource_sas(dir_name + "/sub_dir_download_wildcard_recursive_false_1/*")
        result = util.Command("copy").add_arguments(dir_sas).add_arguments(dir_path).\
            add_flags("log-level", "Info").add_flags("output-type","json").add_flags("include-pattern", "*.txt").\
            execute_azcopy_copy_command_get_output()
        #download the directory
        # parse the result to get the last job progress summary
        result = util.parseAzcopyOutput(result)
        try:
            # parse the Json Output
            x = json.loads(result, object_hook=lambda d: namedtuple('X', d.keys())(*d.values()))
        except:
            self.fail('error parsing the output in Json Format')
        # since the wildcards '*/*.txt' exists at the end of dir_name in the sas
        # and recursive is set to false, .txt files inside sub-dir inside the dir
        # will be downloaded
        # Number of Expected Transfer should be 10
        self.assertEqual(x.TransfersCompleted, "10")
        self.assertEqual(x.TransfersFailed, "0")

    def test_blob_download_wildcard_recursive_true_1(self):
        #This test verifies the azcopy behavior when wildcards are
        # provided in the source and recursive flag is set to false
        # example src = https://<container>/<vd-1>/*?<sig> recursive = false
        dir_name = "dir_download_wildcard_recursive=true"
        dir_path = util.create_test_n_files(1024, 10, dir_name)

        #create sub-directory inside directory
        sub_dir_name_1 = os.path.join(dir_name, "logs")
        sub_dir_path_1 = util.create_test_n_files(1024, 10, sub_dir_name_1)

        #create sub-directory inside sub-directory
        sub_dir_name_2 = os.path.join(sub_dir_name_1, "abc")
        sub_dir_path_2 = util.create_test_n_files(1024, 10, sub_dir_name_2)

        #upload the directory with 30 files
        # upload the directory
        # execute azcopy command
        result = util.Command("copy").add_arguments(dir_path).add_arguments(util.test_container_url). \
            add_flags("recursive", "true").add_flags("log-level", "info").execute_azcopy_copy_command()
        self.assertTrue(result)

        # execute the validator.
        dir_sas = util.get_resource_sas(dir_name)
        result = util.Command("testBlob").add_arguments(dir_path).add_arguments(dir_sas). \
            add_flags("is-object-dir", "true").execute_azcopy_verify()
        self.assertTrue(result)

        # create the resource sas
        dir_sas_with_wildcard = util.get_resource_sas(dir_name + "/*")
        #download the directory
        result = util.Command("copy").add_arguments(dir_sas_with_wildcard).add_arguments(dir_path). \
            add_flags("log-level", "Info").add_flags("output-type", "json").add_flags("recursive", "true").\
            execute_azcopy_copy_command_get_output()
        # parse the result to get the last job progress summary
        result = util.parseAzcopyOutput(result)
        try:
            # parse the Json Output
            x = json.loads(result, object_hook=lambda d: namedtuple('X', d.keys())(*d.values()))
        except:
            self.fail('error parsing the output in Json Format')
        # since the wildcards '*' exists at the end of dir_name in the sas
        # and recursive is set to true, all files inside dir and
        # inside sub-dirs will be download
        # Number of Expected Transfer should be 30
        self.assertEqual(x.TransfersCompleted, "30")
        self.assertEqual(x.TransfersFailed, "0")

        # create the resource sas
        dir_sas_with_wildcard = util.get_resource_sas(dir_name + "/*")
        # download the directory
        result = util.Command("copy").add_arguments(dir_sas_with_wildcard).add_arguments(dir_path). \
            add_flags("log-level", "Info").add_flags("output-type", "json").add_flags("recursive", "true").\
            add_flags("include-path", "logs/;abc/").execute_azcopy_copy_command_get_output()
        # parse the result to get the last job progress summary
        result = util.parseAzcopyOutput(result)
        try:
            # parse the Json Output
            x = json.loads(result, object_hook=lambda d: namedtuple('X', d.keys())(*d.values()))
        except:
            self.fail('error parsing the output in Json Format')
        # since the wildcards '*' exists at the end of dir_name in the sas
        # include-path is set to logs/;abc/
        # and recursive is set to true, files immediately inside will not be downloaded
        # but files inside sub-dir logs and sub-dir inside logs i.e abc inside dir will be downloaded
        # Number of Expected Transfer should be 20
        self.assertEqual(x.TransfersCompleted, "20")
        self.assertEqual(x.TransfersFailed, "0")

        # prepare testing paths
        log_path = os.path.join(dir_path, "logs/")
        abc_path = os.path.join(dir_path, "abc/")
        log_url = util.get_resource_sas(dir_name + "/logs")
        abc_url = util.get_resource_sas(dir_name + "/abc")

        # Test log path
        result = util.Command("testBlob").add_arguments(log_path).add_arguments(log_url).\
            add_flags("is-object-dir", "true").execute_azcopy_verify()
        self.assertTrue(result)

        # test abc path
        result = util.Command("testBlob").add_arguments(abc_path).add_arguments(abc_url)\
            .add_flags("is-object-dir", "true").execute_azcopy_verify()
        self.assertTrue(result)

        # This test validates the functionality if list-of-files flag.
    def test_blob_download_list_of_files_flag(self):
        #This test verifies the azcopy behavior blobs are downloaded using
        # list-of-files flag
        dir_name = "dir_download_list_of_files_flag"
        dir_path = util.create_test_n_files(1024, 10, dir_name)

        #create sub-directory inside directory
        sub_dir_name_1 = os.path.join(dir_name, "logs")
        sub_dir_path_1 = util.create_test_n_files(1024, 10, sub_dir_name_1)

        #create sub-directory inside sub-directory
        sub_dir_name_2 = os.path.join(sub_dir_name_1, "abc")
        sub_dir_path_2 = util.create_test_n_files(1024, 10, sub_dir_name_2)

        #upload the directory with 30 files
        # upload the directory
        # execute azcopy command
        result = util.Command("copy").add_arguments(dir_path).add_arguments(util.test_container_url). \
            add_flags("recursive", "true").add_flags("log-level", "info").execute_azcopy_copy_command()
        self.assertTrue(result)

        # execute the validator.
        dir_sas = util.get_resource_sas(dir_name)
        result = util.Command("testBlob").add_arguments(dir_path).add_arguments(dir_sas). \
            add_flags("is-object-dir", "true").execute_azcopy_verify()
        self.assertTrue(result)

        #download the entire directory with list-of-files-flag
        list_of_files = [dir_name]
        filePath = util.create_new_list_of_files("testfile", list_of_files)
        result = util.Command("copy").add_arguments(util.test_container_url).add_arguments(dir_path). \
            add_flags("log-level", "Info").add_flags("output-type","json").add_flags("recursive","true") \
            .add_flags("list-of-files", filePath).execute_azcopy_copy_command_get_output()
        # parse the result to get the last job progress summary
        result = util.parseAzcopyOutput(result)
        try:
            # parse the Json Output
            x = json.loads(result, object_hook=lambda d: namedtuple('X', d.keys())(*d.values()))
        except:
            self.fail('error parsing the output in Json Format')
        # since entire directory is downloaded
        self.assertEqual(x.TransfersCompleted, "30")
        self.assertEqual(x.TransfersFailed, "0")

        # create the resource sas
        dir_sas = util.get_resource_sas(dir_name)
        # download the logs directory inside the dir
        list_of_files = ["logs"]
        filePath = util.create_new_list_of_files("testfile", list_of_files)
        result = util.Command("copy").add_arguments(dir_sas).add_arguments(dir_path). \
            add_flags("log-level", "Info").add_flags("output-type","json").add_flags("recursive","true"). \
            add_flags("list-of-files", filePath).execute_azcopy_copy_command_get_output()
        # parse the result to get the last job progress summary
        result = util.parseAzcopyOutput(result)
        try:
            # parse the Json Output
            x = json.loads(result, object_hook=lambda d: namedtuple('X', d.keys())(*d.values()))
        except:
            self.fail('error parsing the output in Json Format')
        #since only logs sub-directory is downloaded, transfers will be 20
        self.assertEqual(x.TransfersCompleted, "20")
        self.assertEqual(x.TransfersFailed, "0")
