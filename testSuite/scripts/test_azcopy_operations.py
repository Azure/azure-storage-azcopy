import utility as util
import json
import unittest
from collections import namedtuple
import sys

class Azcopy_Operation_User_Scenario(unittest.TestCase):

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


    def test_remove_files_with_Wildcard(self):
        # create dir dir_remove_files_with_wildcard
        # create 40 files inside the dir
        dir_name = "dir_remove_files_with_wildcard"
        dir_n_files_path = util.create_test_n_files(1024, 40, dir_name)

        # Upload the directory by azcopy
        result = util.Command("copy").add_arguments(dir_n_files_path).add_arguments(util.test_container_url). \
            add_flags("log-level", "Info").add_flags("recursive", "true").execute_azcopy_copy_command()
        self.assertTrue(result)

        # destination is the remote URl of the uploaded dir
        destination = util.get_resource_sas(dir_name)
        # Verify the Uploaded directory
        # execute the validator.
        result = util.Command("testBlob").add_arguments(dir_n_files_path).add_arguments(destination). \
            add_flags("is-object-dir", "true").execute_azcopy_verify()
        self.assertTrue(result)

        # removes the files that ends with 4.txt
        destination_sas_with_wildcard = util.append_text_path_resource_sas(destination, "*4.txt")
        result = util.Command("rm").add_arguments(destination_sas_with_wildcard).add_flags("log-level", "Info"). \
            add_flags("recursive", "true").add_flags("output-json", "true").execute_azcopy_operation_get_output()
        # Get the latest Job Summary
        result = util.parseAzcopyOutput(result)
        try:
            # Parse the Json Output
            x = json.loads(result, object_hook=lambda d: namedtuple('X', d.keys())(*d.values()))
        except:
            self.fail('error parsing the output in Json Format')
        self.assertEquals(x.TransfersFailed, 0)
        self.assertEquals(x.TransfersCompleted, 4)

        # removes the files that starts with test
        destination_sas_with_wildcard = util.append_text_path_resource_sas(destination, "test*")
        result = util.Command("rm").add_arguments(destination_sas_with_wildcard).add_flags("log-level", "Info"). \
            add_flags("recursive", "true").add_flags("output-json", "true").execute_azcopy_operation_get_output()
        # Get the latest Job Summary
        result = util.parseAzcopyOutput(result)
        try:
            # Parse the Json Output
            x = json.loads(result, object_hook=lambda d: namedtuple('X', d.keys())(*d.values()))
        except:
            self.fail('error parsing the output in Json Format')

        # Expected number of successful transfer will be 36 since 4 files have already been deleted
        self.assertEquals(x.TransfersCompleted, 36)
        self.assertEquals(x.TransfersFailed, 0)

        # Create directory dir_remove_all_files_with_wildcard
        dir_name = "dir_remove_all_files_with_wildcard"
        dir_n_files_path = util.create_test_n_files(1024, 40, dir_name)

        # Upload the directory using Azcopy
        result = util.Command("copy").add_arguments(dir_n_files_path).add_arguments(util.test_container_url). \
            add_flags("log-level", "Info").add_flags("recursive", "true").execute_azcopy_copy_command()
        self.assertTrue(result)

        # destination is the remote URl of the uploaded dir
        destination = util.get_resource_sas(dir_name)
        # Validate the Uploaded directory
        # execute the validator.
        result = util.Command("testBlob").add_arguments(dir_n_files_path).add_arguments(destination). \
            add_flags("is-object-dir", "true").execute_azcopy_verify()
        self.assertTrue(result)

        # add * at the end of destination sas
        # destination_sas_with_wildcard = https://<container-name>/<dir-name>/*?<sig>
        destination_sas_with_wildcard = util.append_text_path_resource_sas(destination, "*")
        result = util.Command("rm").add_arguments(destination_sas_with_wildcard).add_flags("log-level", "Info"). \
            add_flags("recursive", "true").add_flags("output-json", "true").execute_azcopy_operation_get_output()
        # Get the latest Job Summary
        result = util.parseAzcopyOutput(result)
        try:
            # Parse the Json Output
            x = json.loads(result, object_hook=lambda d: namedtuple('X', d.keys())(*d.values()))
        except:
            self.fail('error parsing the output in Json Format')
        # Expected number of successful transfer will be 40 since all files will be deleted
        self.assertEquals(x.TransfersFailed, 0)
        self.assertEquals(x.TransfersCompleted, 40)

        # removing multiple directories with use of WildCards
        for i in range(1, 4):
            dir_name = "rdir" + str(i)
            dir_n_files_path = util.create_test_n_files(1024, 40, dir_name)
            # Upload the directory
            result = util.Command("copy").add_arguments(dir_n_files_path).add_arguments(util.test_container_url). \
                add_flags("log-level", "Info").add_flags("recursive", "true").execute_azcopy_copy_command()
            self.assertTrue(result)

            # execute the validator
            destination = util.get_resource_sas(dir_name)
            result = util.Command("testBlob").add_arguments(dir_n_files_path).add_arguments(destination). \
                add_flags("is-object-dir", "true").execute_azcopy_verify()
            self.assertTrue(result)

        destination_sas_with_wildcard = util.append_text_path_resource_sas(util.test_container_url, "rdir*")
        result = util.Command("rm").add_arguments(destination_sas_with_wildcard).add_flags("log-level", "Info"). \
            add_flags("output-json", "true").add_flags("recursive", "true").execute_azcopy_operation_get_output()

        # Get the latest Job Summary
        result = util.parseAzcopyOutput(result)
        try:
            # Parse the Json Output
            x = json.loads(result, object_hook=lambda d: namedtuple('X', d.keys())(*d.values()))
        except:
            self.fail('error parsing the Output in Json Format')
        # Expected number of successful transfer will be 40 since all files will be deleted
        self.assertEquals(x.TransfersFailed, 0)
        self.assertEquals(x.TransfersCompleted, 120)
