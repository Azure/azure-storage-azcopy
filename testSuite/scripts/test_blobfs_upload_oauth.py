import json
import os
import shutil
from collections import namedtuple
from stat import *
import sys
import utility as util
import unittest

class BlobFs_Upload_OAuth_User_Scenarios(unittest.TestCase):

    def setUp(self):
        # ensure account key is not being used
        self.cachedAzCopyAccountKey = os.environ['ACCOUNT_KEY']
        os.environ['ACCOUNT_KEY'] = ''
        os.environ['AZCOPY_AUTO_LOGIN_TYPE'] = 'SPN'
        os.environ['AZCOPY_SPA_APPLICATION_ID'] = os.environ['ACTIVE_DIRECTORY_APPLICATION_ID']
        os.environ['AZCOPY_TENANT_ID'] = os.environ['OAUTH_TENANT_ID']

    def tearDown(self):
        os.environ['ACCOUNT_KEY'] = self.cachedAzCopyAccountKey
        os.environ['AZCOPY_AUTO_LOGIN_TYPE'] = ''
        os.environ['AZCOPY_SPA_APPLICATION_ID'] = ''
        os.environ['AZCOPY_TENANT_ID'] = ''

    def util_test_blobfs_upload_1Kb_file(
        self,
        explictFromTo=False):
        # create file of size 1KB
        filename = "test_blob_1kb_file.txt"
        file_path = util.create_test_file(filename, 1024)
        # upload the file using Azcopy
        cmd = util.Command("copy").add_arguments(file_path).add_arguments(util.test_bfs_account_url). \
            add_flags("log-level", "DEBUG")
        util.process_oauth_command(
            cmd,
            "LocalBlobFS" if explictFromTo else "")
        result = cmd.execute_azcopy_copy_command()
        self.assertTrue(result)

        # Validate the file uploaded file
        os.environ['ACCOUNT_KEY'] = self.cachedAzCopyAccountKey
        fileUrl = util.test_bfs_account_url + filename
        result = util.Command("testBlobFS").add_arguments(file_path).add_arguments(fileUrl).execute_azcopy_verify()
        self.assertTrue(result)
        self.cachedAzCopyAccountKey = os.environ['ACCOUNT_KEY']
        os.environ['ACCOUNT_KEY'] = ''

    def util_test_blobfs_upload_64MB_file(
        self,
        explictFromTo=False):
        # create test file of size 64MB
        filename = "test_blob_64MB_file.txt"
        file_path = util.create_test_file(filename, 64*1024*1024)
        # Upload the file using Azcopy
        cmd = util.Command("copy").add_arguments(file_path).add_arguments(util.test_bfs_account_url). \
            add_flags("log-level", "DEBUG")
        util.process_oauth_command(
            cmd,
            "LocalBlobFS" if explictFromTo else "")
        result = cmd.execute_azcopy_copy_command()
        self.assertTrue(result)

        # Validate the file uploaded
        os.environ['ACCOUNT_KEY'] = self.cachedAzCopyAccountKey
        fileUrl = util.test_bfs_account_url + filename
        result = util.Command("testBlobFS").add_arguments(file_path).add_arguments(fileUrl).execute_azcopy_verify()
        self.assertTrue(result)
        self.cachedAzCopyAccountKey = os.environ['ACCOUNT_KEY']
        os.environ['ACCOUNT_KEY'] = ''

    def util_test_blobfs_upload_uneven_multiflush_file(
        self,
        explicitFromTo=False):
        # create test file of size 64MB
        filename = "test_uneven_multiflush_64MB_file.txt"
        file_path = util.create_test_file(filename, 64*1024*1024)
        # Upload the file using AzCopy @ 1MB blocks, 15 block flushes (5 flushes, 4 15 blocks, 1 4 blocks)
        cmd = util.Command("copy").add_arguments(file_path).add_arguments(util.test_bfs_account_url). \
            add_flags("block-size-mb", "1").add_flags("flush-threshold", "15").add_flags("log-level", "DEBUG")
        util.process_oauth_command(
            cmd,
            "LocalBlobFS" if explicitFromTo else "")
        result = cmd.execute_azcopy_copy_command()
        self.assertTrue(result)

        # Validate the file uploaded
        os.environ['ACCOUNT_KEY'] = self.cachedAzCopyAccountKey
        fileUrl = util.test_bfs_account_url + filename
        result = util.Command("testBlobFS").add_arguments(file_path).add_arguments(fileUrl).execute_azcopy_verify()
        self.assertTrue(result)
        self.cachedAzCopyAccountKey = os.environ['ACCOUNT_KEY']
        os.environ['ACCOUNT_KEY'] = ''

    def util_test_blobfs_upload_even_multiflush_file(
            self,
            explicitFromTo=False):
        # create test file of size 64MB
        filename = "test_even_multiflush_64MB_file.txt"
        file_path = util.create_test_file(filename, 64 * 1024 * 1024)
        # Upload the file using AzCopy @ 1MB blocks, 16 block flushes (4 16 block flushes)
        cmd = util.Command("copy").add_arguments(file_path).add_arguments(util.test_bfs_account_url). \
            add_flags("block-size-mb", "1").add_flags("flush-threshold", "16").add_flags("log-level", "DEBUG")
        util.process_oauth_command(
            cmd,
            "LocalBlobFS" if explicitFromTo else "")
        result = cmd.execute_azcopy_copy_command()
        self.assertTrue(result)

        # Validate the file uploaded
        os.environ['ACCOUNT_KEY'] = self.cachedAzCopyAccountKey
        fileUrl = util.test_bfs_account_url + filename
        result = util.Command("testBlobFS").add_arguments(file_path).add_arguments(fileUrl).execute_azcopy_verify()
        self.assertTrue(result)
        self.cachedAzCopyAccountKey = os.environ['ACCOUNT_KEY']
        os.environ['ACCOUNT_KEY'] = ''

    def util_test_blobfs_upload_100_1Kb_file(
        self,
        explictFromTo=False):
        # create dir with 100 1KB files inside it
        dir_name = "dir_blobfs_100_1K"
        dir_n_file_path = util.create_test_n_files(1024, 100, dir_name)

        # Upload the directory with 100 files inside it
        cmd = util.Command("copy").add_arguments(dir_n_file_path).add_arguments(util.test_bfs_account_url). \
            add_flags("log-level", "DEBUG").add_flags("recursive","true")
        util.process_oauth_command(
            cmd,
            "LocalBlobFS" if explictFromTo else "")
        result = cmd.execute_azcopy_copy_command()
        self.assertTrue(result)

        # Validate the uploaded directory
        os.environ['ACCOUNT_KEY'] = self.cachedAzCopyAccountKey
        dirUrl = util.test_bfs_account_url + dir_name
        result = util.Command("testBlobFS").add_arguments(dir_n_file_path).add_arguments(dirUrl). \
            add_flags("is-object-dir", "true").execute_azcopy_verify()
        self.assertTrue(result)
        self.cachedAzCopyAccountKey = os.environ['ACCOUNT_KEY']
        os.environ['ACCOUNT_KEY'] = ''

    def test_blobfs_upload_1Kb_file_with_oauth(self):
        self.util_test_blobfs_upload_1Kb_file()

    @unittest.skip("single file authentication for oauth covered by 1kb case")
    def test_blobfs_upload_64MB_file_with_oauth(self):
        self.util_test_blobfs_upload_64MB_file()

    def test_blobfs_upload_100_1Kb_file_with_oauth(self):
        self.util_test_blobfs_upload_100_1Kb_file()

    def test_blobfs_upload_uneven_multiflush_with_oauth(self):
        self.util_test_blobfs_upload_uneven_multiflush_file()

    def test_blobfs_upload_even_multiflush_with_oauth(self):
        self.util_test_blobfs_upload_even_multiflush_file()
