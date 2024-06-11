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

    # util_test_n_1kb_file_in_dir_upload_to_share verifies the upload of n 1kb file to the share.
    def util_test_n_1kb_file_in_dir_upload_to_share(self, number_of_files):
        # create dir dir_n_files and 1 kb files inside the dir.
        dir_name = "dir_" + str(number_of_files) + "_files"
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

    @unittest.skip("already covered during downloading.")
    def test_6_1kb_file_in_dir_upload_to_share(self):
        self.util_test_n_1kb_file_in_dir_upload_to_share(6)

    # util_test_n_1kb_file_in_dir_upload_to_azure_directory verifies the upload of n 1kb file to the share.
    def util_test_n_1kb_file_in_dir_upload_to_azure_directory(self, number_of_files, recursive):
        # create dir dir_n_files and 1 kb files inside the dir.
        dir_name = "dir_" + str(number_of_files) + "_files"
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

    @unittest.skip("already covered during downloading.")
    def test_3_1kb_file_in_dir_upload_to_azure_directory_recursive(self):
        self.util_test_n_1kb_file_in_dir_upload_to_azure_directory(3, "true")

    @unittest.skip("upload directory without --recursive specified is not supported currently.")
    def test_8_1kb_file_in_dir_upload_to_azure_directory_non_recursive(self):
        self.util_test_n_1kb_file_in_dir_upload_to_azure_directory(8, "false")

    # test_1G_file_upload verifies the azcopy upload of 1Gb file upload in blocks of 100 Mb
    @unittest.skip("covered by stress")
    def test_1GB_file_upload(self):
        # create 1Gb file
        filename = "test_1G_file.txt"
        file_path = util.create_test_file(filename, 1 * 1024 * 1024 * 1024)

        # execute azcopy upload.
        destination_sas = util.get_resource_sas_from_share(filename)
        result = util.Command("copy").add_arguments(file_path).add_arguments(destination_sas).add_flags("log-level", "info"). \
            add_flags("block-size-mb", "100").add_flags("recursive", "true").execute_azcopy_copy_command()
        self.assertTrue(result)

        # Verifying the uploaded file.
        # adding local file path as first argument.
        # adding file sas as local argument.
        # calling the testFile validator to verify whether file has been successfully uploaded or not.
        result = util.Command("testFile").add_arguments(file_path).add_arguments(destination_sas).execute_azcopy_verify()
        self.assertTrue(result)
