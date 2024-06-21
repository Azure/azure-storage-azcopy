import os
import shutil
import time
import utility as util
import unittest

class FileShare_Upload_User_Scenario(unittest.TestCase):

    def test_file_upload_1mb_wildcard(self):
        # create the test file.
        file_name = "test_file_upload_1mb_wildcard"
        file_path = util.create_test_file(file_name, 1024 * 1024)

        # execute azcopy upload.
        destination = util.get_resource_sas_from_share(file_name)
        wildcard_path = file_path.replace(file_name, "test_file_upload_1mb_wildcard*")
        result = util.Command("copy").add_arguments(wildcard_path).add_arguments(util.test_share_url).add_flags("log-level", "info"). \
            add_flags("block-size-mb", "4").execute_azcopy_copy_command()
        self.assertTrue(result)

        # execute validator.
        result = util.Command("testFile").add_arguments(file_path).add_arguments(destination).execute_azcopy_verify()
        self.assertTrue(result)