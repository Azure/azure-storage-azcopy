import os
import unittest

import utility as util


class File_Service_2_Service_Copy_User_Scenario(unittest.TestCase):

    def test_copy_single_file_to_file(self):
        content_file_name_src = "test_1kb_file_copy_src.txt"
        content_file_path_src = util.create_test_file(content_file_name_src, 1024)
        content_file_name_dst = "test_1kb_file_copy_dst.txt"

        # create source file of size 1KB.
        remote_src_file_path = util.get_resource_sas_from_share(content_file_name_src)
        remote_dst_file_path = util.get_resource_sas_from_share(content_file_name_dst)
        result = util.Command("cp").add_arguments(content_file_path_src).add_arguments(remote_src_file_path). \
            add_flags("log-level", "info").execute_azcopy_copy_command()
        self.assertTrue(result)

        # perform the single file copy using azcopy.
        result = util.Command("copy").add_arguments(remote_src_file_path).add_arguments(remote_dst_file_path). \
            add_flags("log-level", "info").execute_azcopy_copy_command()
        self.assertTrue(result)

        # verifying the copy worked, both remote source and destination should be identical to the local source
        result = util.Command("testFile").add_arguments(content_file_path_src).add_arguments(
            remote_src_file_path).execute_azcopy_verify()
        self.assertTrue(result)
        result = util.Command("testFile").add_arguments(content_file_path_src).add_arguments(
            remote_dst_file_path).execute_azcopy_verify()
        self.assertTrue(result)

    def test_copy_entire_dir_to_dir(self):
        content_dir_name_src = "dir_file_copy_test_src"
        content_dir_path_src = util.create_test_n_files(1024, 10, content_dir_name_src)
        content_dir_name_dst = "dir_file_copy_test_dst"

        # create sub-directory inside directory
        sub_dir_name = os.path.join(content_dir_name_src, "sub_dir_copy_test")
        util.create_test_n_files(1024, 10, sub_dir_name)

        # upload to the source
        result = util.Command("copy").add_arguments(content_dir_path_src).add_arguments(util.test_share_url). \
            add_flags("recursive", "true").add_flags("log-level", "info").execute_azcopy_copy_command()
        self.assertTrue(result)

        # copy to destination
        remote_src_dir_path = util.get_resource_sas_from_share(content_dir_name_src)
        remote_dst_dir_path = util.get_resource_sas_from_share(content_dir_name_dst)
        result = util.Command("copy").add_arguments(util.get_resource_sas_from_share(content_dir_name_src+"/*"))\
            .add_arguments(remote_dst_dir_path).add_flags("log-level", "info").add_flags("recursive", "true")\
            .execute_azcopy_copy_command()
        self.assertTrue(result)

        # execute the validator to make sure the copy worked, both remote src and dst should match local src
        result = util.Command("testFile").add_arguments(content_dir_path_src).add_arguments(remote_src_dir_path). \
            add_flags("is-object-dir", "true").execute_azcopy_verify()
        self.assertTrue(result)
        result = util.Command("testFile").add_arguments(content_dir_path_src).add_arguments(remote_dst_dir_path). \
            add_flags("is-object-dir", "true").execute_azcopy_verify()
        self.assertTrue(result)
