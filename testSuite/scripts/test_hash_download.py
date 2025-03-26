from collections import namedtuple
import utility as util
import unittest
import json
import time
import os

TIMESTAMP = int(time.time())

class Hash_Download_User_Scenarios(unittest.TestCase):
    # test_single_file_download verifies that a single file is downloaded successfully with the tamper-proof flag set.
    def test_single_file_download(self):

        # creating directory with 1 file in it.
        num_files = 1
        dir_name = f"download_dir_hash_single_file_{TIMESTAMP}"
        dir_n_files_path = util.create_test_n_files(1024, num_files, dir_name)
        tamper_proof = util.test_tamper_proof_endpoint

        # upload using azcopy
        result = util.Command("copy").add_arguments(dir_n_files_path).add_arguments(util.test_container_url) \
            .add_flags("log-level", "info").add_flags("recursive", "true").add_flags("put-md5", "true"). \
            add_flags("tamper-proof", tamper_proof).execute_azcopy_copy_command()
        self.assertTrue(result)

        # verify the uploaded blob
        result = util.Command("testBlob").add_arguments(dir_n_files_path).add_arguments(util.test_container_url) \
            .add_flags("is-object-dir", "true").execute_azcopy_verify()
        self.assertTrue(result)

        # downloading the uploaded blob to devnull
        src = util.get_resource_sas(dir_name)
        dst = os.devnull
        result = util.Command("copy").add_arguments(src).add_arguments(dst).add_flags("log-level", "info") \
            .add_flags("recursive", "true").add_flags("output-type","json") \
                .add_flags("check-md5", "FailIfDifferentOrMissing").add_flags("tamper-proof", tamper_proof) \
                    .execute_azcopy_copy_command_get_output()
        result = util.parseAzcopyOutput(result)
        try:
            # parse the Json Output
            x = json.loads(result, object_hook=lambda d: namedtuple('X', d.keys())(*d.values()))
        except:
            self.fail('error parsing the output in Json Format')
        # check results
        self.assertEquals(x.TransfersCompleted, str(num_files))
        self.assertEquals(x.TransfersFailed, "0")

    # test_multiple_file_download verifies that multiple files are downloaded successfully with the tamper-proof flag set.
    def test_multiple_file_download(self):

        # creating directory with 3 file in it.
        num_files = 3
        dir_name = f"download_dir_hash_multiple_file_{TIMESTAMP}"
        dir_n_files_path = util.create_test_n_files(1024, num_files, dir_name)
        tamper_proof = util.test_tamper_proof_endpoint

        # upload using azcopy
        result = util.Command("copy").add_arguments(dir_n_files_path).add_arguments(util.test_container_url) \
            .add_flags("log-level", "info").add_flags("recursive", "true").add_flags("put-md5", "true"). \
            add_flags("tamper-proof", tamper_proof).execute_azcopy_copy_command()
        self.assertTrue(result)

        # verify the uploaded blob
        result = util.Command("testBlob").add_arguments(dir_n_files_path).add_arguments(util.test_container_url).add_flags("is-object-dir", "true").execute_azcopy_verify()
        self.assertTrue(result)

        # downloading the uploaded blob to devnull
        src = util.get_resource_sas(dir_name)
        dst = os.devnull
        result = util.Command("copy").add_arguments(src).add_arguments(dst).add_flags("log-level", "info").add_flags("recursive", "true").add_flags("output-type","json").add_flags("check-md5", "FailIfDifferentOrMissing").add_flags("tamper-proof", tamper_proof).execute_azcopy_copy_command_get_output()
        result = util.parseAzcopyOutput(result)
        try:
            # parse the Json Output
            x = json.loads(result, object_hook=lambda d: namedtuple('X', d.keys())(*d.values()))
        except:
            self.fail('error parsing the output in Json Format')
        # check results
        self.assertEquals(x.TransfersCompleted, str(num_files))
        self.assertEquals(x.TransfersFailed, "0")

    def test_download_hash_no_match(self):

        dir_name = f"download_dir_hash_single_no_match_file_{TIMESTAMP}"
        util.create_test_dir(dir_name)
        tamper_proof = util.test_tamper_proof_endpoint


        # Creating a directory with 1 file in it, uploading it with azcopy and then modifying it.
        for i in range(1,2):
            dir_n_files_path = util.create_test_file(f"{dir_name}/test.txt", 1024*i)
            result = util.Command("copy").add_arguments(f"{util.test_directory_path}/{dir_name}").add_arguments(util.test_container_url) \
                .add_flags("log-level", "info").add_flags("recursive", "true").add_flags("put-md5", "true"). \
                add_flags("tamper-proof", tamper_proof).execute_azcopy_copy_command()
            self.assertTrue(result)

            # verify the uploaded blob
            result = util.Command("testBlob").add_arguments(f"{util.test_directory_path}/{dir_name}").add_arguments(util.test_container_url).add_flags("is-object-dir", "true").execute_azcopy_verify()
            self.assertTrue(result)

        # downloading the uploaded blob to devnull
        src = util.get_resource_sas(dir_name)
        dst = os.devnull
        result = util.Command("copy").add_arguments(src).add_arguments(dst).add_flags("log-level", "info").add_flags("recursive", "true").add_flags("output-type","json").add_flags("check-md5", "FailIfDifferentOrMissing").add_flags("tamper-proof", tamper_proof).execute_azcopy_copy_command_get_output()
        result = util.parseAzcopyOutput(result)
        try:
            # parse the Json Output
            x = json.loads(result, object_hook=lambda d: namedtuple('X', d.keys())(*d.values()))
        except:
            self.fail('error parsing the output in Json Format')
        # expected to fail since file was modified
        self.assertEquals(x.TransfersCompleted, "0")
        self.assertEquals(x.TransfersFailed, "1")
        