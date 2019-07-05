import filecmp
import os
import unittest
import utility as util

class Autodetect_Blob_Type_Scenario(unittest.TestCase):
    # Currently, the only auto-detected blob type is page blob.
    # Copy a VHD without specifying page blob and see what it does.
    def test_auto_detect_blob_type_vhd(self):
        file_name = "myVHD.vhd"
        file_path = util.create_test_file(file_name, 4 * 1024 * 1024)

        # copy VHD file without specifying page blob. Page blob is inferred for VHD, VHDX, and VMDK
        destination_sas = util.get_resource_sas(file_name)
        result = util.Command("copy").add_arguments(file_path).add_arguments(destination_sas).add_flags("log-level",
                                                                                                        "info"). \
            add_flags("block-size-mb", 4).execute_azcopy_copy_command()
        self.assertTrue(result)

        # execute validator. Validator will ensure it's a page blob as validator now checks blob type before testing.
        result = util.Command("testBlob").add_arguments(file_path).add_arguments(destination_sas). \
            add_flags("blob-type", "PageBlob").execute_azcopy_verify()
        self.assertTrue(result)

    def test_copy_infer_blob_type_from_block_to_page_blob(self):
        file_name = "testS2SVHD.vhd"
        src_container_url = util.get_object_sas(util.test_s2s_src_blob_account_url, file_name)
        dst_container_url = util.get_object_sas(util.test_s2s_dst_blob_account_url, file_name)

        file_path = util.create_test_file(file_name, 4 * 1024 * 1024)

        result = util.Command("copy").add_arguments(file_path).add_arguments(src_container_url). \
            add_flags("blob-type", "BlockBlob").add_flags("log-level", "info").execute_azcopy_copy_command()
        self.assertTrue(result)

        result = util.Command("copy").add_arguments(src_container_url).add_arguments(dst_container_url). \
            add_flags("log-level", "info").execute_azcopy_copy_command()  # Blob type in this case will be inferred.
        self.assertTrue(result)

        # Verify blob type (should be page blob)
        result = util.Command("testBlob").add_arguments(file_path).add_arguments(dst_container_url). \
            add_flags("blob-type", "PageBlob").execute_azcopy_verify()
        self.assertTrue(result)