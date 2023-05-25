import utility as util
import json
import unittest
from collections import namedtuple
import sys
import os

class Azcopy_Operation_User_Scenario(unittest.TestCase):
    def setUp(self):
        cmd = util.Command("login").add_arguments("--service-principal").add_flags("application-id", os.environ['ACTIVE_DIRECTORY_APPLICATION_ID'])
        cmd.execute_azcopy_copy_command()

    def tearDown(self):
        cmd = util.Command("logout")
        cmd.execute_azcopy_copy_command()


    # test_remove_virtual_directory  creates a virtual directory, removes the virtual directory created
    # and then verifies the contents of virtual directory.
    def test_remove_virtual_directory(self):
        # create dir dir_10_files and 1 kb files inside the dir.
        dir_name = "dir_" + str(10) + "_files_rm"
        dir_n_files_path = util.create_test_n_files(1024, 10, dir_name)

        # execute azcopy command
        result = util.Command("copy").add_arguments(dir_n_files_path).add_arguments(util.test_container_url). \
            add_flags("recursive", "true").add_flags("log-level", "info").execute_azcopy_copy_command()
        self.assertTrue(result)

        destination = util.get_resource_sas(dir_name)
        result = util.Command("rm").add_arguments(destination).add_flags("recursive", "true").execute_azcopy_copy_command()
        self.assertTrue(result)

        result = util.Command("list").add_arguments(destination).add_flags("resource-num", "0").execute_azcopy_verify()
        self.assertTrue(result)

    def test_remove_virtual_directory_oauth(self):
        # create dir dir_10_files and 1 kb files inside the dir.
        dir_name = "dir_" + str(10) + "_files_rm_oauth"
        dir_n_files_path = util.create_test_n_files(1024, 10, dir_name)

        # execute azcopy command
        result = util.Command("copy").add_arguments(dir_n_files_path).add_arguments(util.test_oauth_container_url). \
            add_flags("recursive", "true").add_flags("log-level", "info").execute_azcopy_copy_command()
        self.assertTrue(result)

        destination = util.get_object_without_sas(util.test_oauth_container_url, dir_name)
        result = util.Command("rm").add_arguments(destination).add_flags("recursive", "true").execute_azcopy_copy_command()
        self.assertTrue(result)

        destination_with_sas = util.get_object_sas(util.test_oauth_container_validate_sas_url, dir_name)
        result = util.Command("list").add_arguments(destination_with_sas).add_flags("resource-num", "0").execute_azcopy_verify()
        self.assertTrue(result)
