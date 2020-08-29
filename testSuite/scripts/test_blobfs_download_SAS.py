import unittest
import os
import utility as util
import shutil

class BlobFs_Download_SAS_User_Scenarios(unittest.TestCase):
    def setUp(self):
        cmd = util.Command("login").add_arguments("--service-principal").add_flags("application-id", os.environ['ACTIVE_DIRECTORY_APPLICATION_ID'])
        cmd.execute_azcopy_copy_command()
        self.cachedAzCopyAccountKey = os.environ['ACCOUNT_KEY']
        os.environ['ACCOUNT_KEY'] = ''

    def tearDown(self):
        cmd = util.Command("logout")
        cmd.execute_azcopy_copy_command()
        os.environ['ACCOUNT_KEY'] = self.cachedAzCopyAccountKey

    def test_blobfs_sas_download_1Kb_file(self):
        # Create file of size 1KB
        filename = "test_blobfs_sas_d_1kb_file.txt"
        file_path = util.create_test_file(filename, 1024)
        # Upload the file using azCopy
        result = util.Command("copy").add_arguments(file_path).add_arguments(util.test_bfs_sas_account_url). \
            add_flags("log-level", "Info").execute_azcopy_copy_command()
        self.assertTrue(result)

        # Validate the uploaded file
        file_url = util.get_resource_sas_from_bfs(filename)
        result = util.Command("testBlobFS").add_arguments(file_path).add_arguments(file_url).execute_azcopy_verify()
        self.assertTrue(result)

        # Delete the local file
        try:
            os.remove(file_path)
        except:
            self.fail('error deleting the file ' + file_path)

        # Download the file using AzCopy
        result = util.Command("copy").add_arguments(file_url).add_arguments(util.test_directory_path). \
            add_flags("log-level", "Info").execute_azcopy_copy_command()
        self.assertTrue(result)

        # Validate the downloaded file
        result = util.Command("testBlobFS").add_arguments(file_path).add_arguments(file_url).execute_azcopy_verify()
        self.assertTrue(result)

    def test_blobfs_sas_download_64MB_file(self):
        # Create file of size 1KB
        filename = "test_blobfs_sas_d_64MB_file.txt"
        file_path = util.create_test_file(filename, 64*1024*1024)
        # Upload the file using azCopy
        result = util.Command("copy").add_arguments(file_path).add_arguments(util.test_bfs_sas_account_url). \
            add_flags("log-level", "Info").execute_azcopy_copy_command()
        self.assertTrue(result)

        # Validate the uploaded file
        file_url = util.get_resource_sas_from_bfs(filename)
        result = util.Command("testBlobFS").add_arguments(file_path).add_arguments(file_url).execute_azcopy_verify()
        self.assertTrue(result)

        # Delete the local file
        try:
            os.remove(file_path)
        except:
            self.fail('error deleting the file ' + file_path)

        # Download the file using AzCopy
        result = util.Command("copy").add_arguments(file_url).add_arguments(util.test_directory_path). \
            add_flags("log-level", "Info").execute_azcopy_copy_command()
        self.assertTrue(result)

        # Validate the downloaded file
        result = util.Command("testBlobFS").add_arguments(file_path).add_arguments(file_url).execute_azcopy_verify()
        self.assertTrue(result)

    def test_blobfs_sas_download_100_1Kb_file(self):
        # Create dir with 100 1kb files inside it
        dir_name = "dir_blobfs_sas_d_100_1K"
        dir_n_file_path = util.create_test_n_files(1024, 100, dir_name)

        # Upload the directory with 100 files inside it
        result = util.Command("copy").add_arguments(dir_n_file_path).add_arguments(util.test_bfs_sas_account_url). \
            add_flags("log-level", "Info").add_flags("recursive", "true").execute_azcopy_copy_command()
        self.assertTrue(result)

        # Validate the uploaded directory
        dirUrl = util.get_resource_sas_from_bfs(dir_name)
        dirUrl_nosas = util.test_bfs_account_url + dir_name
        result = util.Command("testBlobFS").add_arguments(dir_n_file_path).add_arguments(dirUrl). \
            add_flags("is-object-dir", "true").execute_azcopy_verify()
        self.assertTrue(result)

        # Delete the local files
        try:
            shutil.rmtree(dir_n_file_path)
        except:
            self.fail('error deleting the directory ' + dir_n_file_path)

        # Download the directory
        result = util.Command("copy").add_arguments(dirUrl).add_arguments(util.test_directory_path). \
            add_flags("log-level", "Info").add_flags("recursive", "true").execute_azcopy_copy_command()
        self.assertTrue(result)

        # Validate the downloaded directory
        result = util.Command("testBlobFS").add_arguments(dir_n_file_path).add_arguments(dirUrl). \
            add_flags("is-object-dir", "true").execute_azcopy_verify()
        self.assertTrue(result)
