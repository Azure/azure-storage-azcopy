import json
import os
import shutil
import time
import urllib
from collections import namedtuple
import utility as util
import unittest


# Temporary tests (mostly copy-pasted from blob tests) to guarantee simple sync scenarios still work
# TODO Replace with better tests in the future
class Blob_Sync_User_Scenario(unittest.TestCase):

    def test_sync_single_blob(self):
        # create file of size 1KB.
        filename = "test_1kb_blob_sync.txt"
        file_path = util.create_test_file(filename, 1024)
        blob_path = util.get_resource_sas(filename)

        # Upload 1KB file using azcopy.
        src = file_path
        dest = blob_path
        result = util.Command("cp").add_arguments(src).add_arguments(dest). \
            add_flags("log-level", "info").execute_azcopy_copy_command()
        self.assertTrue(result)

        # Verifying the uploaded blob.
        # the resource local path should be the first argument for the azcopy validator.
        # the resource sas should be the second argument for azcopy validator.
        resource_url = util.get_resource_sas(filename)
        result = util.Command("testBlob").add_arguments(file_path).add_arguments(resource_url).execute_azcopy_verify()
        self.assertTrue(result)

        # Sync 1KB file to local using azcopy.
        src = blob_path
        dest = file_path
        result = util.Command("sync").add_arguments(src).add_arguments(dest). \
            add_flags("log-level", "info").execute_azcopy_copy_command()
        self.assertTrue(result)

        # Sync 1KB file to blob using azcopy.
        # reset local file lmt first
        util.create_test_file(filename, 1024)
        src = file_path
        dest = blob_path
        result = util.Command("sync").add_arguments(src).add_arguments(dest). \
            add_flags("log-level", "info").execute_azcopy_copy_command()
        self.assertTrue(result)

    def test_sync_entire_directory(self):
        dir_name = "dir_sync_test"
        dir_path = util.create_test_n_files(1024, 10, dir_name)

        # create sub-directory inside directory
        sub_dir_name = os.path.join(dir_name, "sub_dir_sync_test")
        util.create_test_n_files(1024, 10, sub_dir_name)

        # upload the directory with 20 files
        # upload the directory
        # execute azcopy command
        result = util.Command("copy").add_arguments(dir_path).add_arguments(util.test_container_url). \
            add_flags("recursive", "true").add_flags("log-level", "info").execute_azcopy_copy_command()
        self.assertTrue(result)

        # execute the validator.
        vdir_sas = util.get_resource_sas(dir_name)
        result = util.Command("testBlob").add_arguments(dir_path).add_arguments(vdir_sas). \
            add_flags("is-object-dir", "true").execute_azcopy_verify()
        self.assertTrue(result)

        # sync to local
        src = vdir_sas
        dst = dir_path
        result = util.Command("sync").add_arguments(src).add_arguments(dst).add_flags("log-level", "info")\
            .execute_azcopy_copy_command()
        self.assertTrue(result)

        # sync back to blob after recreating the files
        util.create_test_n_files(1024, 10, sub_dir_name)
        src = dir_path
        dst = vdir_sas
        result = util.Command("sync").add_arguments(src).add_arguments(dst).add_flags("log-level", "info") \
            .execute_azcopy_copy_command()
        self.assertTrue(result)
