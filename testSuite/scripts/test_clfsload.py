import os
import unittest

import utility as util


# Temporary tests to guarantee simple load scenario works
class LoadUserScenario(unittest.TestCase):

    def test_load_entire_directory(self):
        dir_name = "dir_load_test"
        dir_path = util.create_test_n_files(1024, 10, dir_name)

        # create sub-directory inside directory
        sub_dir_name = os.path.join(dir_name, "sub_dir_load_test")
        util.create_test_n_files(1024, 10, sub_dir_name)

        # clean out the container
        # execute azcopy command
        # ignore the error since the container might be already empty
        util.Command("rm").add_arguments(util.test_container_url). \
            add_flags("recursive", "true").execute_azcopy_copy_command()

        # invoke the load command
        state_path = os.path.join(util.test_directory_path, "clfsload-state")
        result = util.Command("load clfs").add_arguments(dir_path).add_arguments(util.test_container_url). \
            add_flags("max-errors", "8").add_flags("state-path", state_path).add_flags("preserve-hardlinks", "true") \
            .add_flags("compression-type", "LZ4").execute_azcopy_copy_command()
        self.assertTrue(result)
