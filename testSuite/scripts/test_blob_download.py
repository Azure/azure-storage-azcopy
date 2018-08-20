import json
import os
import shutil
import time
import urllib
from collections import namedtuple
import utility as util
import unittest

class Blob_Download_User_Scenario(unittest.TestCase):

    # test_download_1kb_blob verifies the download of 1Kb blob using azcopy.
    def test_download_1kb_blob(self):
        # create file of size 1KB.
        filename = "test_1kb_blob_upload.txt"
        file_path = util.create_test_file(filename, 1024)

        # Upload 1KB file using azcopy.
        src = file_path
        dest = util.test_container_url
        result = util.Command("copy").add_arguments(src).add_arguments(dest). \
            add_flags("log-level", "info").add_flags("recursive", "true").execute_azcopy_copy_command()
        self.assertTrue(result)

        # Verifying the uploaded blob.
        # the resource local path should be the first argument for the azcopy validator.
        # the resource sas should be the second argument for azcopy validator.
        resource_url = util.get_resource_sas(filename)
        result = util.Command("testBlob").add_arguments(file_path).add_arguments(resource_url).execute_azcopy_verify()
        self.assertTrue(result)

        time.sleep(5)

        # downloading the uploaded file
        src = util.get_resource_sas(filename)
        dest = util.test_directory_path + "/test_1kb_blob_download.txt"
        result = util.Command("copy").add_arguments(src).add_arguments(dest).add_flags("log-level",
                                                                                       "info").execute_azcopy_copy_command()
        self.assertTrue(result)

        # Verifying the downloaded blob
        result = util.Command("testBlob").add_arguments(dest).add_arguments(src).execute_azcopy_verify()
        self.assertTrue(result)

    # test_download_perserve_last_modified_time verifies the azcopy downloaded file
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

        # uploading file through azcopy with flag block-size set to 4194304 i.e 4mb
        destination_sas = util.get_resource_sas(file_name)
        result = util.Command("copy").add_arguments(file_path).add_arguments(destination_sas).\
                    add_flags("log-level","info").add_flags("block-size", "4194304").execute_azcopy_copy_command()
        self.assertTrue(result)

        # verify the uploaded file.
        result = util.Command("testBlob").add_arguments(file_path).add_arguments(destination_sas).execute_azcopy_verify()
        self.assertTrue(result)

        # downloading the created parallely in blocks of 4mb file through azcopy.
        download_file = util.test_directory_path + "/test_63mb_in4mb_download.txt"
        result = util.Command("copy").add_arguments(destination_sas).add_arguments(download_file)\
                    .add_flags("log-level","info").add_flags("block-size", "4194304").execute_azcopy_copy_command()
        self.assertTrue(result)

        # verify the downloaded file
        result = util.Command("testBlob").add_arguments(download_file).add_arguments(
            destination_sas).execute_azcopy_verify()
        self.assertTrue(result)

    # test_recursive_download_blob downloads a directory recursively from container through azcopy
    def recursive_download_blob(self):
        # create directory and 5 files of 1KB inside that directory.
        dir_name = "dir_" + str(10) + "_files"
        dir1_path = util.create_test_n_files(1024, 5, dir_name)

        # upload the directory to container through azcopy with recursive set to true.
        result = util.Command("copy").add_arguments(dir1_path).add_arguments(util.test_container_url).\
                        add_flags("log-level","info").add_flags("recursive", "true").execute_azcopy_copy_command()
        self.assertTrue(result)

        # verify the uploaded file.
        destination_sas = util.get_resource_sas(dir_name)
        result = util.Command("testBlob").add_arguments(dir1_path).add_arguments(destination_sas).\
            add_flags("is-object-dir","true").execute_azcopy_verify()
        self.assertTrue(result)
        try:
            shutil.rmtree(dir1_path)
        except OSError as e:
            self.fail('error removing the file ' + dir1_path)

        # downloading the directory created from container through azcopy with recursive flag to true.
        result = util.Command("copy").add_arguments(destination_sas).add_arguments(util.test_directory_path).add_flags(
            "log-level", "info").add_flags("recursive", "true").execute_azcopy_copy_command()
        self.assertTrue(result)

        # verify downloaded blob.
        result = util.Command("testBlob").add_arguments(dir1_path).add_arguments(destination_sas).\
            add_flags("is-object-dir","true").execute_azcopy_verify()
        self.assertTrue(result)


    def test_blob_download_with_special_characters(self):
        filename_special_characters = "abc|>rd*"
        resource_url = util.get_resource_sas(filename_special_characters)
        # creating the file with random characters and with file name having special characters.
        result = util.Command("create").add_arguments(resource_url).add_flags("serviceType", "Blob").add_flags(
            "resourceType", "SingleFile").add_flags("blob-size", "1024").execute_azcopy_verify()
        self.assertTrue(result)

        # downloading the blob created above.
        result = util.Command("copy").add_arguments(resource_url).add_arguments(util.test_directory_path).add_flags(
            "log-level", "info").execute_azcopy_copy_command()
        self.assertTrue(result)

        expected_filename = urllib.parse.quote_plus(filename_special_characters)
        # verify if the downloaded file exists or not.
        filepath = util.test_directory_path + "/" + expected_filename
        self.assertTrue(os.path.isfile(filepath))

        # verify the downloaded blob.
        result = util.Command("testBlob").add_arguments(filepath).add_arguments(resource_url).execute_azcopy_verify()
        self.assertTrue(result)

    def test_sync_blob_download_without_wildcards(self):
        # created a directory and created 10 files inside the directory
        dir_name = "sync_download_without_wildcards"
        dir_n_files_path = util.create_test_n_files(1024, 10, dir_name)

        # upload the directory
        # execute azcopy command
        result = util.Command("copy").add_arguments(dir_n_files_path).add_arguments(util.test_container_url). \
            add_flags("recursive", "true").add_flags("log-level", "info").execute_azcopy_copy_command()
        self.assertTrue(result)

        # execute the validator.
        dir_sas = util.get_resource_sas(dir_name)
        result = util.Command("testBlob").add_arguments(dir_n_files_path).add_arguments(dir_sas). \
            add_flags("is-object-dir", "true").execute_azcopy_verify()
        self.assertTrue(result)

        # download the destination to the source to match the last modified time
        result = util.Command("copy").add_arguments(dir_sas).add_arguments(util.test_directory_path). \
            add_flags("log-level", "info").add_flags("recursive", "true").add_flags("output", "json"). \
            add_flags("preserve-last-modified-time", "true").execute_azcopy_copy_command_get_output()
        self.assertNotEquals(result, None)

        # execute the validator and verify the downloaded dir
        result = util.Command("testBlob").add_arguments(dir_n_files_path).add_arguments(dir_sas). \
            add_flags("is-object-dir", "true").execute_azcopy_verify()
        self.assertTrue(result)

        # sync the source and destination
        result = util.Command("sync").add_arguments(dir_sas).add_arguments(dir_n_files_path). \
            add_flags("log-level", "info").add_flags("recursive", "true").execute_azcopy_copy_command()
        self.assertFalse(result)


    def test_sync_blob_download_with_wildcards(self):
        # created a directory and created 10 files inside the directory
        dir_name = "sync_download_with_wildcards"
        dir_n_files_path = util.create_test_n_files(1024, 10, dir_name)

        # upload the directory
        # execute azcopy command
        result = util.Command("copy").add_arguments(dir_n_files_path).add_arguments(util.test_container_url). \
            add_flags("recursive", "true").add_flags("log-level", "info").execute_azcopy_copy_command()
        self.assertTrue(result)

        # execute the validator.
        dir_sas = util.get_resource_sas(dir_name)
        result = util.Command("testBlob").add_arguments(dir_n_files_path).add_arguments(dir_sas). \
            add_flags("is-object-dir", "true").execute_azcopy_verify()
        self.assertTrue(result)

        # download the destination to the source to match the last modified time
        result = util.Command("copy").add_arguments(dir_sas).add_arguments(util.test_directory_path). \
            add_flags("log-level", "info").add_flags("recursive", "true").add_flags("output", "json"). \
            add_flags("preserve-last-modified-time", "true").execute_azcopy_copy_command_get_output()
        self.assertNotEquals(result, None)

        # execute the validator and verify the downloaded dir
        result = util.Command("testBlob").add_arguments(dir_n_files_path).add_arguments(dir_sas). \
            add_flags("is-object-dir", "true").execute_azcopy_verify()
        self.assertTrue(result)

        # add "*" at the end of dir sas
        # since both the source and destination are in sync, it will fail
        dir_sas = util.append_text_path_resource_sas(dir_sas, "*")
        # sync the source and destination
        result = util.Command("sync").add_arguments(dir_sas).add_arguments(dir_n_files_path). \
            add_flags("log-level", "info").add_flags("recursive", "true").execute_azcopy_copy_command()
        self.assertFalse(result)

        subdir1 = os.path.join(dir_name, "subdir1")
        subdir1_file_path = util.create_test_n_files(1024, 10, subdir1)

        subdir2 = os.path.join(dir_name, "subdir2")
        subdir2_file_path = util.create_test_n_files(1024, 10, subdir2)

        # upload the directory
        # execute azcopy command
        result = util.Command("copy").add_arguments(dir_n_files_path).add_arguments(util.test_container_url). \
            add_flags("recursive", "true").add_flags("log-level", "info").execute_azcopy_copy_command()
        self.assertTrue(result)

        # execute the validator.
        dir_sas = util.get_resource_sas(dir_name)
        result = util.Command("testBlob").add_arguments(dir_n_files_path).add_arguments(dir_sas). \
            add_flags("is-object-dir", "true").execute_azcopy_verify()
        self.assertTrue(result)

        # Download the directory to match the blob modified time
        result = util.Command("copy").add_arguments(dir_sas).add_arguments(util.test_directory_path). \
            add_flags("log-level", "Info").add_flags("recursive", "true").execute_azcopy_copy_command()
        self.assertTrue(result)

        # sync the source and destination
        # add extra wildcards
        # since source and destination both are in sync, it will fail
        dir_sas = util.append_text_path_resource_sas(dir_sas, "*/*.txt")
        result = util.Command("sync").add_arguments(dir_sas).add_arguments(dir_n_files_path). \
            add_flags("log-level", "info").add_flags("recursive", "true").execute_azcopy_copy_command()
        self.assertFalse(result)

        # delete 5 files inside each sub-directories locally
        for r in range(5, 10):
            filename = "test101024_" + str(r) + ".txt"
            filepath = os.path.join(subdir1_file_path, filename)
            try:
                os.remove(filepath)
            except:
                self.fail('error deleting the file ' + filepath)
            filepath = os.path.join(subdir2_file_path, filename)
            try:
                os.remove(filepath)
            except:
                self.fail('error deleting the file ' + filepath)
        # 10 files have been deleted inside the sub-dir
        # sync remote to local
        # 10 files will be downloaded
        result = util.Command("sync").add_arguments(dir_sas).add_arguments(dir_n_files_path). \
            add_flags("log-level", "info").add_flags("recursive", "true").add_flags("output","json").\
            execute_azcopy_copy_command_get_output()
        # parse the result to get the last job progress summary
        result = util.parseAzcopyOutput(result)
        try:
            # parse the Json Output
            x = json.loads(result, object_hook=lambda d: namedtuple('X', d.keys())(*d.values()))
        except:
            self.fail('error parsing the output in Json Format')
        # Number of Expected Transfer should be 10 since 10 files were deleted
        self.assertEquals(x.TransfersCompleted, 10)
        self.assertEquals(x.TransfersFailed, 0)

    # test_download_1kb_blob verifies the download of 1Kb blob using azcopy.
    def test_download_1kb_blob_with_oauth(self):
        self.util_test_download_1kb_blob_with_oauth()

    def util_test_download_1kb_blob_with_oauth(
        self,
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
        self.assertTrue(result)

        # Verifying the uploaded blob.
        # the resource local path should be the first argument for the azcopy validator.
        # the resource sas should be the second argument for azcopy validator.
        dest_validate = util.get_resource_from_oauth_container_validate(filename)
        result = util.Command("testBlob").add_arguments(file_path).add_arguments(dest_validate).execute_azcopy_verify()
        self.assertTrue(result)

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
        self.assertTrue(result)

        # Verifying the downloaded blob
        result = util.Command("testBlob").add_arguments(dest).add_arguments(src_validate).execute_azcopy_verify()
        self.assertTrue(result)

    # test_recursive_download_blob downloads a directory recursively from container through azcopy
    def test_recursive_download_blob_with_oauth(self):
        self.util_test_recursive_download_blob_with_oauth()

    def util_test_recursive_download_blob_with_oauth(
        self,
        forceOAuthLogin=False,
        tenantID="",
        aadEndpoint=""):
        # create directory and 5 files of 1KB inside that directory.
        dir_name = "util_test_recursive_download_blob_with_oauth_dir_" + str(10) + "_files"
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
        self.assertTrue(result)

        # verify the uploaded file.
        dest_validate = util.get_resource_from_oauth_container_validate(dir_name)
        result = util.Command("testBlob").add_arguments(dir1_path).add_arguments(dest_validate).add_flags("is-object-dir",
                                                                                                "true").execute_azcopy_verify()
        self.assertTrue(result)

        try:
            shutil.rmtree(dir1_path)
        except OSError as e:
            self.fail("error removing the upload files. " + e)

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
        self.assertTrue(result)

        # verify downloaded blob.
        result = util.Command("testBlob").add_arguments(dir1_path).add_arguments(dest_validate).add_flags("is-object-dir",
                                                                                                            "true").execute_azcopy_verify()
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
            add_flags("log-level", "Info").add_flags("output","json").execute_azcopy_copy_command()
        self.assertEquals(result, False)

        # create the resource sas
        dir_sas_with_wildcard = util.get_resource_sas(dir_name + "/*")
        #download the directory
        result = util.Command("copy").add_arguments(dir_sas_with_wildcard).add_arguments(dir_path).\
                add_flags("log-level", "Info").add_flags("output","json").execute_azcopy_copy_command_get_output()
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
        self.assertEquals(x.TransfersCompleted, 10)
        self.assertEquals(x.TransfersFailed, 0)

        # create the resource sas
        dir_sas_with_wildcard = util.get_resource_sas(dir_name + "/*/*.txt")
        #download the directory
        result = util.Command("copy").add_arguments(dir_sas_with_wildcard).add_arguments(dir_path). \
            add_flags("log-level", "Info").add_flags("output","json").execute_azcopy_copy_command_get_output()
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
        self.assertEquals(x.TransfersCompleted, 10)
        self.assertEquals(x.TransfersFailed, 0)

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
            add_flags("log-level", "Info").add_flags("output","json").add_flags("recursive","true").\
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
        self.assertEquals(x.TransfersCompleted, 30)
        self.assertEquals(x.TransfersFailed, 0)

        # create the resource sas
        dir_sas_with_wildcard = util.get_resource_sas(dir_name + "/*/*")
        #download the directory
        result = util.Command("copy").add_arguments(dir_sas_with_wildcard).add_arguments(dir_path). \
            add_flags("log-level", "Info").add_flags("output","json") \
            .add_flags("recursive", "true").execute_azcopy_copy_command_get_output()
        # parse the result to get the last job progress summary
        result = util.parseAzcopyOutput(result)
        try:
            # parse the Json Output
            x = json.loads(result, object_hook=lambda d: namedtuple('X', d.keys())(*d.values()))
        except:
            self.fail('error parsing the output in Json Format')
        # since the wildcards '*/*' exists at the end of dir_name in the sas
        # and recursive is set to true, files immediately inside will not be downloaded
        # but files inside sub-dir logs and sub-dir inside logs i.e abc inside dir will be downloaded
        # Number of Expected Transfer should be 20
        self.assertEquals(x.TransfersCompleted, 20)
        self.assertEquals(x.TransfersFailed, 0)
