import os
import shutil
import time
import unittest
import utility as util

class FileShare_Download_User_Scenario(unittest.TestCase):

    # test_upload_download_1kb_file_fullname verifies the upload/download of 1Kb file with fullname using azcopy.
    def test_upload_download_1kb_file_fullname(self):
        # create file of size 1KB.
        filename = "test_upload_download_1kb_file_fullname.txt"
        file_path = util.create_test_file(filename, 1024)

        # Upload 1KB file using azcopy.
        src = file_path
        dest = util.test_share_url
        result = util.Command("copy").add_arguments(src).add_arguments(dest). \
            add_flags("log-level", "info").execute_azcopy_copy_command()
        self.assertTrue(result)

        # Verifying the uploaded file.
        # the resource local path should be the first argument for the azcopy validator.
        # the resource sas should be the second argument for azcopy validator.
        resource_url = util.get_resource_sas_from_share(filename)
        result = util.Command("testFile").add_arguments(file_path).add_arguments(resource_url).execute_azcopy_verify()
        self.assertTrue(result)

        time.sleep(5)

        # downloading the uploaded file
        src = util.get_resource_sas_from_share(filename)
        dest = util.test_directory_path + "/test_1kb_file_download.txt"
        result = util.Command("copy").add_arguments(src).add_arguments(dest).add_flags("log-level",
                                                                                       "info").execute_azcopy_copy_command()
        self.assertTrue(result)

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

        time.sleep(5)

        # downloading the uploaded file
        src = util.get_resource_sas_from_share(filename)
        wildcardSrc = util.get_resource_sas_from_share("*")
        dest = util.test_directory_path + "/test_upload_download_1kb_file_wildcard_all_files_dir"
        try:
            if os.path.exists(dest) and os.path.isdir(dest):
                shutil.rmtree(dest)
        except:
            self.fail('error removing directory ' + dest)
        finally:
            os.makedirs(dest)

        result = util.Command("copy").add_arguments(wildcardSrc).add_arguments(dest). \
            add_flags("log-level", "info").add_flags("recursive", "true").execute_azcopy_copy_command()
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

        wildcard_path = file_path.replace(filename, prefix)

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

        time.sleep(5)

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

        result = util.Command("copy").add_arguments(wildcardSrc).add_arguments(dest).add_flags("log-level",
                                                                                               "info").execute_azcopy_copy_command()
        self.assertTrue(result)

        # Verifying the downloaded file
        result = util.Command("testFile").add_arguments(os.path.join(dest, filename)).add_arguments(
            src).execute_azcopy_verify()
        self.assertTrue(result)

    # util_test_n_1kb_file_in_dir_upload_download_share verifies the upload of n 1kb file to the share.
    def util_test_n_1kb_file_in_dir_upload_download_share(self, number_of_files):
        # create dir dir_n_files and 1 kb files inside the dir.
        dir_name = "dir_test_n_1kb_file_in_dir_upload_download_share_" + str(number_of_files) + "_files"
        sub_dir_name = "dir subdir_" + str(number_of_files) + "_files"

        # create n test files in dir
        src_dir = util.create_test_n_files(1024, number_of_files, dir_name)

        # create n test files in subdir, subdir is contained in dir
        util.create_test_n_files(1024, number_of_files, os.path.join(dir_name, sub_dir_name))

        # execute azcopy command
        dest_share = util.test_share_url
        result = util.Command("copy").add_arguments(src_dir).add_arguments(dest_share). \
            add_flags("recursive", "true").add_flags("log-level", "info").execute_azcopy_copy_command()
        self.assertTrue(result)

        # execute the validator.
        dest_azure_dir = util.get_resource_sas_from_share(dir_name)
        result = util.Command("testFile").add_arguments(src_dir).add_arguments(dest_azure_dir). \
            add_flags("is-object-dir", "true").execute_azcopy_verify()
        self.assertTrue(result)

        download_azure_src_dir = dest_azure_dir
        download_local_dest_dir = src_dir + "_download"

        try:
            if os.path.exists(download_local_dest_dir) and os.path.isdir(download_local_dest_dir):
                shutil.rmtree(download_local_dest_dir)
        except:
            self.fail("error removing " + download_local_dest_dir)
        finally:
            os.makedirs(download_local_dest_dir)

        # downloading the directory created from azure file share through azcopy with recursive flag to true.
        result = util.Command("copy").add_arguments(download_azure_src_dir).add_arguments(
            download_local_dest_dir).add_flags("log-level", "info").add_flags("recursive", "true").execute_azcopy_copy_command()
        self.assertTrue(result)

        # verify downloaded file.
        result = util.Command("testFile").add_arguments(os.path.join(download_local_dest_dir, dir_name)).add_arguments(
            download_azure_src_dir).add_flags("is-object-dir", "true").execute_azcopy_verify()
        self.assertTrue(result)

    def test_6_1kb_file_in_dir_upload_download_share(self):
        self.util_test_n_1kb_file_in_dir_upload_download_share(6)

    # util_test_n_1kb_file_in_dir_upload_download_azure_directory verifies the upload of n 1kb file to the share.
    def util_test_n_1kb_file_in_dir_upload_download_azure_directory(self, number_of_files, recursive):
        # create dir dir_n_files and 1 kb files inside the dir.
        dir_name = "util_test_n_1kb_file_in_dir_upload_download_azure_directory_" + recursive + "_" + str(
            number_of_files) + "_files"
        sub_dir_name = "dir_subdir_" + str(number_of_files) + "_files"

        # create n test files in dir
        src_dir = util.create_test_n_files(1024, number_of_files, dir_name)

        # create n test files in subdir, subdir is contained in dir
        util.create_test_n_files(1024, number_of_files, os.path.join(dir_name, sub_dir_name))

        # prepare destination directory.
        # TODO: note azcopy v2 currently only support existing directory and share.
        dest_azure_dir_name = "dest azure_dir_name"
        dest_azure_dir = util.get_resource_sas_from_share(dest_azure_dir_name)

        result = util.Command("create").add_arguments(dest_azure_dir).add_flags("serviceType", "File"). \
            add_flags("resourceType", "Bucket").execute_azcopy_create()
        self.assertTrue(result)

        # execute azcopy command
        result = util.Command("copy").add_arguments(src_dir).add_arguments(dest_azure_dir). \
            add_flags("recursive", recursive).add_flags("log-level", "info").execute_azcopy_copy_command()
        self.assertTrue(result)
        
        # execute the validator.
        dest_azure_dir_to_compare = util.get_resource_sas_from_share(dest_azure_dir_name + "/" + dir_name)
        result = util.Command("testFile").add_arguments(src_dir).add_arguments(dest_azure_dir_to_compare). \
            add_flags("is-object-dir", "true").add_flags("is-recursive", recursive).execute_azcopy_verify()
        self.assertTrue(result)

        download_azure_src_dir = dest_azure_dir_to_compare
        download_local_dest_dir = src_dir + "_download"

        try:
            if os.path.exists(download_local_dest_dir) and os.path.isdir(download_local_dest_dir):
                shutil.rmtree(download_local_dest_dir)
        except:
            print("catch error for removing " + download_local_dest_dir)
        finally:
            os.makedirs(download_local_dest_dir)

        # downloading the directory created from azure file directory through azcopy with recursive flag to true.
        result = util.Command("copy").add_arguments(download_azure_src_dir).add_arguments(
            download_local_dest_dir).add_flags("log-level", "info"). \
            add_flags("recursive", recursive).execute_azcopy_copy_command()
        self.assertTrue(result)

        # verify downloaded file.
        # todo: ensure the comparing here
        result = util.Command("testFile").add_arguments(os.path.join(download_local_dest_dir, dir_name)).add_arguments(
            download_azure_src_dir). \
            add_flags("is-object-dir", "true").add_flags("is-recursive", recursive).execute_azcopy_verify()
        self.assertTrue(result)

    def test_3_1kb_file_in_dir_upload_download_azure_directory_recursive(self):
        self.util_test_n_1kb_file_in_dir_upload_download_azure_directory(3, "true")

    @unittest.skip("upload directory without --recursive specified is not supported currently.")
    def test_8_1kb_file_in_dir_upload_download_azure_directory_non_recursive(self):
        self.util_test_n_1kb_file_in_dir_upload_download_azure_directory(8, "false")

    # test_download_perserve_last_modified_time verifies the azcopy downloaded file
    # and its modified time preserved locally on disk
    def test_download_perserve_last_modified_time(self):
        # create a file of 2KB
        filename = "test_upload_preserve_last_mtime.txt"
        file_path = util.create_test_file(filename, 2048)

        # upload file through azcopy.
        destination_sas = util.get_resource_sas_from_share(filename)
        result = util.Command("copy").add_arguments(file_path).add_arguments(destination_sas). \
            add_flags("log-level", "info").add_flags("recursive", "true").execute_azcopy_copy_command()
        self.assertTrue(result)

        # Verifying the uploaded file
        result = util.Command("testFile").add_arguments(file_path).add_arguments(destination_sas).execute_azcopy_verify()
        self.assertTrue(result)

        time.sleep(5)

        # download file through azcopy with flag preserve-last-modified-time set to true
        download_file_name = util.test_directory_path + "/test_download_preserve_last_mtime.txt"
        result = util.Command("copy").add_arguments(destination_sas).add_arguments(download_file_name).add_flags("log-level",
                                                                                                                 "info").add_flags(
            "preserve-last-modified-time", "true").execute_azcopy_copy_command()
        self.assertTrue(result)

        # Verifying the downloaded file and its modified with the modified time of file.
        result = util.Command("testFile").add_arguments(download_file_name).add_arguments(destination_sas).add_flags(
            "preserve-last-modified-time", "true").execute_azcopy_verify()
        self.assertTrue(result)

    # test_file_download_63mb_in_4mb downloads 63mb file in block of 4mb through azcopy
    def test_file_download_63mb_in_4mb(self):
        # create file of 63mb
        file_name = "test_63mb_in4mb_upload.txt"
        file_path = util.create_test_file(file_name, 63 * 1024 * 1024)

        # uploading file through azcopy with flag block-size set to 4194304 i.e 4mb
        destination_sas = util.get_resource_sas_from_share(file_name)
        result = util.Command("copy").add_arguments(file_path).add_arguments(destination_sas).add_flags("log-level",
                                                                                                        "info").add_flags(
            "block-size", "4194304").execute_azcopy_copy_command()
        self.assertTrue(result)

        # verify the uploaded file.
        result = util.Command("testFile").add_arguments(file_path).add_arguments(destination_sas).execute_azcopy_verify()
        self.assertTrue(result)

        # downloading the created parallely in blocks of 4mb file through azcopy.
        download_file = util.test_directory_path + "/test_63mb_in4mb_download.txt"
        result = util.Command("copy").add_arguments(destination_sas).add_arguments(download_file).add_flags("log-level",
                                                                                                            "info").add_flags(
            "block-size", "4194304").execute_azcopy_copy_command()
        self.assertTrue(result)

        # verify the downloaded file
        result = util.Command("testFile").add_arguments(download_file).add_arguments(
            destination_sas).execute_azcopy_verify()
        self.assertTrue(result)

    # test_recursive_download_file downloads a directory recursively from share through azcopy
    def test_recursive_download_file(self):
        # create directory and 5 files of 1KB inside that directory.
        dir_name = "dir_" + str(10) + "_files"
        dir1_path = util.create_test_n_files(1024, 5, dir_name)

        # upload the directory to share through azcopy with recursive set to true.
        result = util.Command("copy").add_arguments(dir1_path).add_arguments(util.test_share_url).add_flags("log-level",
                                                                                                            "info").add_flags(
            "recursive", "true").execute_azcopy_copy_command()
        self.assertTrue(result)

        # verify the uploaded file.
        destination_sas = util.get_resource_sas_from_share(dir_name)
        result = util.Command("testFile").add_arguments(dir1_path).add_arguments(destination_sas).add_flags("is-object-dir",
                                                                                                            "true").execute_azcopy_verify()
        self.assertTrue(result)

        try:
            shutil.rmtree(dir1_path)
        except OSError as e:
            self.fail("error removing the uploaded files. " +  str(e))

        # downloading the directory created from share through azcopy with recursive flag to true.
        result = util.Command("copy").add_arguments(destination_sas).add_arguments(util.test_directory_path).add_flags(
            "log-level", "info").add_flags("recursive", "true").execute_azcopy_copy_command()
        self.assertTrue(result)

        # verify downloaded file.
        result = util.Command("testFile").add_arguments(dir1_path).add_arguments(destination_sas).add_flags("is-object-dir",
                                                                                                            "true").execute_azcopy_verify()
        self.assertTrue(result)
