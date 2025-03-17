import utility as util
import unittest
import time


TIMESTAMP = int(time.time())

class Hash_Upload_User_Scenarios(unittest.TestCase):
    
    def test_single_file_upload(self):
        # creating directory with 1 file in it.
        dir_name = f"upload_dir_hash_single_file_{TIMESTAMP}"
        dir_n_files_path = util.create_test_n_files(1024, 1, dir_name)
        tamper_proof = util.test_tamper_proof_endpoint

        # execute azcopy copy upload.
        result = util.Command("copy").add_arguments(dir_n_files_path).add_arguments(util.test_container_url) \
            .add_flags("log-level", "info").add_flags("recursive", "true").add_flags("put-md5", "true"). \
            add_flags("tamper-proof", tamper_proof).execute_azcopy_copy_command()
        self.assertTrue(result)

        # Verifying the uploaded blob
        # calling the testBlob validator to verify whether blob has been successfully uploaded or not
        result = util.Command("testBlob").add_arguments(dir_n_files_path).add_arguments(util.test_container_url).add_flags("is-object-dir", "true").execute_azcopy_verify()
        self.assertTrue(result)

    def test_multiple_file_upload(self):
        # creating directory with multiple files in it.
        dir_name = f"upload_dir_hash_multiple_file_{TIMESTAMP}"
        dir_n_files_path = util.create_test_n_files(1024, 3, dir_name)
        tamper_proof = util.test_tamper_proof_endpoint

        # execute azcopy copy upload.
        result = util.Command("copy").add_arguments(dir_n_files_path).add_arguments(util.test_container_url) \
            .add_flags("log-level", "info").add_flags("recursive", "true").add_flags("put-md5", "true"). \
            add_flags("tamper-proof", tamper_proof).execute_azcopy_copy_command()
        self.assertTrue(result)

        # Verifying the uploaded blob
        # calling the testBlob validator to verify whether blob has been successfully uploaded or not
        result = util.Command("testBlob").add_arguments(dir_n_files_path).add_arguments(util.test_container_url).add_flags("is-object-dir", "true").execute_azcopy_verify()
        self.assertTrue(result)
