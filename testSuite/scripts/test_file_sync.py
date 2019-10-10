import os
import unittest

import utility as util


class File_Sync_User_Scenario(unittest.TestCase):

    def test_sync_single_file_to_file(self):
        content_file_name_src = "test_1kb_file_sync_src.txt"
        content_file_path_src = util.create_test_file(content_file_name_src, 1024)
        content_file_name_dst = "test_1kb_file_sync_dst.txt"
        content_file_path_dst = util.create_test_file(content_file_name_src, 1024)

        # create source and destination files of size 1KB.
        # make sure to create the destination first so that it has an older lmt
        remote_src_file_path = util.get_resource_sas_from_share(content_file_name_src)
        remote_dst_file_path = util.get_resource_sas_from_share(content_file_name_dst)
        result = util.Command("cp").add_arguments(content_file_path_dst).add_arguments(remote_dst_file_path). \
            add_flags("log-level", "info").execute_azcopy_copy_command()
        self.assertTrue(result)
        result = util.Command("cp").add_arguments(content_file_path_src).add_arguments(remote_src_file_path). \
            add_flags("log-level", "info").execute_azcopy_copy_command()
        self.assertTrue(result)

        # perform the single file sync using azcopy.
        result = util.Command("sync").add_arguments(remote_src_file_path).add_arguments(remote_dst_file_path). \
            add_flags("log-level", "info").execute_azcopy_copy_command()
        self.assertTrue(result)

        # verifying the sync worked, both remote source and destination should be identical to the local source
        result = util.Command("testFile").add_arguments(content_file_path_src).add_arguments(
            remote_src_file_path).execute_azcopy_verify()
        self.assertTrue(result)
        result = util.Command("testFile").add_arguments(content_file_path_src).add_arguments(
            remote_dst_file_path).execute_azcopy_verify()
        self.assertTrue(result)

    def test_sync_entire_dir_to_dir(self):
        content_dir_name_src = "dir_file_sync_test_src"
        content_dir_path_src = util.create_test_n_files(1024, 10, content_dir_name_src)
        content_dir_name_dst = "dir_file_sync_test_dst"
        content_dir_path_dst = util.create_test_n_files(1024, 10, content_dir_name_dst)

        # create sub-directory inside directory
        sub_dir_name = os.path.join(content_dir_name_src, "sub_dir_sync_test")
        util.create_test_n_files(1024, 10, sub_dir_name)

        # upload to both the destination and the source with different random data
        result = util.Command("copy").add_arguments(content_dir_path_dst).add_arguments(util.test_share_url). \
            add_flags("recursive", "true").add_flags("log-level", "info").execute_azcopy_copy_command()
        self.assertTrue(result)
        result = util.Command("copy").add_arguments(content_dir_path_src).add_arguments(util.test_share_url). \
            add_flags("recursive", "true").add_flags("log-level", "info").execute_azcopy_copy_command()
        self.assertTrue(result)

        # sync to destination
        remote_src_dir_path = util.get_resource_sas_from_share(content_dir_name_src)
        remote_dst_dir_path = util.get_resource_sas_from_share(content_dir_name_dst)
        result = util.Command("sync").add_arguments(remote_src_dir_path).add_arguments(remote_dst_dir_path)\
            .add_flags("log-level", "info").execute_azcopy_copy_command()
        self.assertTrue(result)

        # execute the validator to make sure the sync worked, both remote src and dst should match local src
        result = util.Command("testFile").add_arguments(content_dir_path_src).add_arguments(remote_src_dir_path). \
            add_flags("is-object-dir", "true").execute_azcopy_verify()
        self.assertTrue(result)
        result = util.Command("testFile").add_arguments(content_dir_path_src).add_arguments(remote_dst_dir_path). \
            add_flags("is-object-dir", "true").execute_azcopy_verify()
        self.assertTrue(result)
