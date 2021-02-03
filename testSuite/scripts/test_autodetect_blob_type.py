import filecmp
import os
import unittest
import utility as util

class Autodetect_Blob_Type_Scenario(unittest.TestCase):
    def setUp(self):
        cmd = util.Command("login").add_arguments("--service-principal").add_flags("application-id", os.environ['ACTIVE_DIRECTORY_APPLICATION_ID'])
        cmd.execute_azcopy_copy_command()

    def tearDown(self):
        cmd = util.Command("logout")
        cmd.execute_azcopy_copy_command()

    # Currently, the only auto-detected blob type is page blob.
    # Copy a VHD without specifying page blob and see what it does.
    def test_auto_infer_blob_type_vhd(self):
        # Upload to blob storage, detecting as a page blob.
        file_name = "myVHD.vHd"  # awkward capitalization to see if azcopy catches it
        file_path = util.create_test_file(file_name, 4 * 1024 * 1024)

        # copy VHD file without specifying page blob. Page blob is inferred for VHD, VHDX, and VMDK
        destination_sas = util.get_object_sas(util.test_container_url, file_name)
        result = util.Command("copy").add_arguments(file_path).add_arguments(destination_sas).add_flags("log-level",
                                                                                                        "info"). \
            add_flags("block-size-mb", 4).execute_azcopy_copy_command()
        self.assertTrue(result)

        # execute validator. Validator will ensure it's a page blob as validator now checks blob type before testing.
        result = util.Command("testBlob").add_arguments(file_path).add_arguments(destination_sas). \
            add_flags("blob-type", "PageBlob").execute_azcopy_verify()
        self.assertTrue(result)

    def test_copy_infer_blob_type_from_files_to_page_blob(self):
        # Upload to Azure Files and then transfer to Blob to detect as page blob.
        file_name = "testS2SVHD.vhd"
        containerName = util.get_resource_name("s2sbtautodetect")

        # These run on seperate accounts in CI, so even without "dst", it's OK.
        # Needed this to run on a single account, though.
        dstbase = util.get_object_sas(util.test_s2s_dst_blob_account_url, containerName + "dst")
        srcbase = util.get_object_sas(util.test_s2s_src_file_account_url, containerName)

        result = util.Command("create").add_arguments(srcbase).add_flags("serviceType", "File"). \
            add_flags("resourceType", "Bucket").execute_azcopy_create()
        self.assertTrue(result)

        result = util.Command("create").add_arguments(dstbase).add_flags("serviceType", "Blob"). \
            add_flags("resourceType", "Bucket").execute_azcopy_create()
        self.assertTrue(result)

        src_container_url = util.get_object_sas(srcbase, file_name)
        dst_container_url = util.get_object_sas(dstbase, file_name)

        file_path = util.create_test_file(file_name, 4 * 1024 * 1024)

        result = util.Command("copy").add_arguments(file_path).add_arguments(src_container_url). \
            add_flags("log-level", "info").execute_azcopy_copy_command()
        self.assertTrue(result)

        result = util.Command("copy").add_arguments(src_container_url).add_arguments(dst_container_url). \
            add_flags("log-level", "info").execute_azcopy_copy_command()  # Blob type in this case will be inferred.
        self.assertTrue(result)

        # Verify blob type (should be page blob)
        result = util.Command("testBlob").add_arguments(file_path).add_arguments(dst_container_url). \
            add_flags("blob-type", "PageBlob").execute_azcopy_verify()
        self.assertTrue(result)

    def test_copy_detect_blob_type_from_blob_to_blob(self):
        # Upload to Azure Blob Storage as Block Blob and detect as Block Blob.
        # Why is this necessary, you ask?
        # Because we detect the source blob type.
        # This means that in all scenarios EXCEPT Blob -> Blob, .vhd corresponds to page blob.
        # However, in Blob -> Blob, we preserve the blob type instead of detecting it.
        # As a result, this behavior should be tested.
        file_name = "testS2SVHD.vhd"
        containerName = util.get_resource_name("s2sbtautodetect")

        # These run on seperate accounts in CI, so even without "dst", it's OK.
        # Needed this to run on a single account, though.
        dstbase = util.get_object_sas(util.test_s2s_dst_blob_account_url, containerName + "dst")
        srcbase = util.get_object_sas(util.test_s2s_src_blob_account_url, containerName)

        result = util.Command("create").add_arguments(srcbase).add_flags("serviceType", "Blob"). \
            add_flags("resourceType", "Bucket").execute_azcopy_create()
        self.assertTrue(result)

        result = util.Command("create").add_arguments(dstbase).add_flags("serviceType", "Blob"). \
            add_flags("resourceType", "Bucket").execute_azcopy_create()
        self.assertTrue(result)

        src_container_url = util.get_object_sas(srcbase, file_name)
        dst_container_url = util.get_object_sas(dstbase, file_name)

        file_path = util.create_test_file(file_name, 4 * 1024 * 1024)

        # Explicitly define BlockBlob as this is local -> blob
        result = util.Command("copy").add_arguments(file_path).add_arguments(src_container_url). \
            add_flags("blob-type", "BlockBlob").add_flags("log-level", "info").execute_azcopy_copy_command()
        self.assertTrue(result)

        result = util.Command("copy").add_arguments(src_container_url).add_arguments(dst_container_url). \
            add_flags("log-level", "info").execute_azcopy_copy_command()  # Blob type will be detected as block.
        self.assertTrue(result)

        # Verify blob type (should be block blob)
        result = util.Command("testBlob").add_arguments(file_path).add_arguments(dst_container_url). \
            add_flags("blob-type", "BlockBlob").execute_azcopy_verify()
        self.assertTrue(result)
