import os
import shutil
import time
import unittest
import utility as util

class FileShare_Download_User_Scenario(unittest.TestCase):

        # Verifying the downloaded file
        result = util.Command("testFile").add_arguments(dest).add_arguments(src).execute_azcopy_verify()
        self.assertTrue(result)

    # test_upload_download_1kb_file_wildcard_all_files verifies the upload/download of 1Kb file with wildcard using azcopy.
    def test_upload_download_1kb_file_wildcard_all_files(self):
        # create file of size 1KB.
        filename = "test_upload_download_1kb_file_wildcard_all_files.txt"
        file_path = util.create_test_file(filename, 1024)

        wildcard_path = file_path.replace(filename, "*")

        # Upload 1KB file using azcopy.
        result = util.Command("copy").add_arguments(wildcard_path).add_arguments(util.test_share_url). \
            add_flags("log-level", "info").execute_azcopy_copy_command()
        self.assertTrue(result)

        # Verifying the uploaded file.
        # the resource local path should be the first argument for the azcopy validator.
        # the resource sas should be the second argument for azcopy validator.
        resource_url = util.get_resource_sas_from_share(filename)
        result = util.Command("testFile").add_arguments(file_path).add_arguments(resource_url).execute_azcopy_verify()
        self.assertTrue(result)

        # downloading the uploaded file
        src = util.get_resource_sas_from_share(filename)
        src_wildcard = util.get_resource_sas_from_share("*")
        dest = util.test_directory_path + "/test_upload_download_1kb_file_wildcard_all_files_dir"
        try:
            if os.path.exists(dest) and os.path.isdir(dest):
                shutil.rmtree(dest)
        except:
            self.fail('error removing directory ' + dest)
        finally:
            os.makedirs(dest)

        result = util.Command("copy").add_arguments(src_wildcard).add_arguments(dest). \
            add_flags("log-level", "info").add_flags("include-pattern", filename.replace("wildcard", "*")). \
            execute_azcopy_copy_command()
        self.assertTrue(result)

        # Verifying the downloaded file
        result = util.Command("testFile").add_arguments(os.path.join(dest, filename)).add_arguments(
            src).execute_azcopy_verify()
        self.assertTrue(result)

    # test_upload_download_1kb_file_fullname verifies the upload/download of 1Kb file with wildcard using azcopy.
    def test_upload_download_1kb_file_wildcard_several_files(self):
        # create file of size 1KB.
        filename = "test_upload_download_1kb_file_wildcard_several_files.txt"
        prefix = "test_upload_download_1kb_file_wildcard_several*"
        file_path = util.create_test_file(filename, 1024)

        wildcardSrc = file_path.replace(filename, prefix)
        # Upload 1KB file using azcopy.
        result = util.Command("copy").add_arguments(wildcardSrc).add_arguments(util.test_share_url). \
            add_flags("log-level", "info").execute_azcopy_copy_command()
        self.assertTrue(result)

        # Verifying the uploaded file.
        # the resource local path should be the first argument for the azcopy validator.
        # the resource sas should be the second argument for azcopy validator.
        resource_url = util.get_resource_sas_from_share(filename)
        result = util.Command("testFile").add_arguments(file_path).add_arguments(resource_url).execute_azcopy_verify()
        self.assertTrue(result)

        # downloading the uploaded file
        src = util.get_resource_sas_from_share(filename)
        wildcardSrc = util.get_resource_sas_from_share(prefix)
        dest = util.test_directory_path + "/test_upload_download_1kb_file_wildcard_several_files"
        try:
            if os.path.exists(dest) and os.path.isdir(dest):
                shutil.rmtree(dest)
        except:
            self.fail('error removing directory ' + dest)
        finally:
            os.makedirs(dest)

        result = util.Command("copy").add_arguments(src).add_arguments(dest).add_flags("include-pattern", prefix). \
            add_flags("log-level", "info").execute_azcopy_copy_command()
        self.assertTrue(result)

        # Verifying the downloaded file
        result = util.Command("testFile").add_arguments(os.path.join(dest, filename)).add_arguments(
            src).execute_azcopy_verify()
        self.assertTrue(result)
