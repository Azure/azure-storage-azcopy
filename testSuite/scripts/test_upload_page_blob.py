import utility as util
import unittest

class PageBlob_Upload_User_Scenarios(unittest.TestCase):
    # util_test_page_blob_upload_1mb verifies the azcopy upload of 1mb file
    # as a page blob.
    def util_test_page_blob_upload_1mb(self, use_oauth=False):
        # create the test gile.
        file_name = "test_page_blob_1mb.vhd"
        file_path = util.create_test_file(file_name, 1024 * 1024)

        # execute azcopy upload.
        if not use_oauth:
            dest = util.get_resource_sas(file_name)
            dest_validate = dest
        else:
            dest = util.get_resource_from_oauth_container(file_name)
            dest_validate = util.get_resource_from_oauth_container_validate(file_name)

        result = util.Command("copy").add_arguments(file_path).add_arguments(dest).add_flags("log-level", "info"). \
            add_flags("block-size-mb", "4").add_flags("blob-type", "PageBlob").execute_azcopy_copy_command()
        self.assertTrue(result)

        # execute validator.
        result = util.Command("testBlob").add_arguments(file_path).add_arguments(dest_validate).\
                add_flags("blob-type", "PageBlob").execute_azcopy_verify()
        self.assertTrue(result)

    # test_page_blob_upload_1mb_with_sas verifies the azcopy upload of 1mb file
    # as a page blob with sas.
    def test_page_blob_upload_1mb_with_sas(self):
        self.util_test_page_blob_upload_1mb(False)

    # test_page_blob_upload_1mb_with_oauth verifies the azcopy upload of 1mb file
    # as a page blob with oauth.
    def test_page_blob_upload_1mb_with_oauth(self):
        self.util_test_page_blob_upload_1mb(True)

    # test_page_range_for_complete_sparse_file verifies the number of Page ranges for
    # complete empty file i.e each character is Null character.
    def test_page_range_for_complete_sparse_file(self):
        # step 1: uploading a sparse file should be optimized
        # create test file.
        file_name = "sparse_file.vhd"
        file_path = util.create_complete_sparse_file(file_name, 4 * 1024 * 1024)

        # execute azcopy page blob upload.
        upload_destination_sas = util.get_resource_sas(file_name)
        result = util.Command("copy").add_arguments(file_path).add_arguments(upload_destination_sas).add_flags(
            "log-level", "info"). \
            add_flags("block-size-mb", "1").add_flags("blob-type", "PageBlob").execute_azcopy_copy_command()
        self.assertTrue(result)

        # execute validator.
        # no of page ranges should be 0 for the empty sparse file.
        result = util.Command("testBlob").add_arguments(file_path).add_arguments(upload_destination_sas). \
            add_flags("blob-type", "PageBlob").add_flags("verify-block-size", "true"). \
            add_flags("number-blocks-or-pages", "0").execute_azcopy_verify()
        self.assertTrue(result)

        # step 2: copy the blob to a second blob should also be optimized
        copy_destination_sas = util.get_resource_sas('sparse_file2.vhd')

        # execute copy
        result = util.Command("copy").add_arguments(upload_destination_sas).add_arguments(copy_destination_sas) \
            .add_flags("log-level", "info").add_flags("block-size-mb", "1").execute_azcopy_copy_command()
        self.assertTrue(result)

        # execute validator.
        # no of page ranges should be 0 for the empty sparse file.
        result = util.Command("testBlob").add_arguments(file_path).add_arguments(copy_destination_sas). \
            add_flags("blob-type", "PageBlob").add_flags("verify-block-size", "true"). \
            add_flags("number-blocks-or-pages", "0").execute_azcopy_verify()
        self.assertTrue(result)

    # test_page_blob_upload_partial_sparse_file verifies the number of page ranges
    # for PageBlob upload by azcopy.
    def test_page_blob_upload_partial_sparse_file(self):
        # step 1: uploading a sparse file should be optimized
        # create test file.
        file_name = "test_partial_sparse_file.vhd"
        file_path = util.create_partial_sparse_file(file_name, 16 * 1024 * 1024)

        # execute azcopy pageblob upload.
        upload_destination_sas = util.get_resource_sas(file_name)
        result = util.Command("copy").add_arguments(file_path).add_arguments(upload_destination_sas).add_flags(
            "log-level", "info"). \
            add_flags("block-size-mb", "4").add_flags("blob-type", "PageBlob").execute_azcopy_copy_command()
        self.assertTrue(result)

        # number of page range for partial sparse created above will be (size/2)
        number_of_page_ranges = int((16 * 1024 * 1024 / (4 * 1024 * 1024)) / 2)
        # execute validator to verify the number of page range for uploaded blob.
        result = util.Command("testBlob").add_arguments(file_path).add_arguments(upload_destination_sas). \
            add_flags("blob-type", "PageBlob").add_flags("verify-block-size", "true"). \
            add_flags("number-blocks-or-pages", str(number_of_page_ranges)).execute_azcopy_verify()
        self.assertTrue(result)

        # step 2: copy the blob to a second blob should also be optimized
        copy_destination_sas = util.get_resource_sas('sparse_file2.vhd')

        # execute copy
        result = util.Command("copy").add_arguments(upload_destination_sas).add_arguments(copy_destination_sas) \
            .add_flags("log-level", "info").add_flags("block-size-mb", "4").execute_azcopy_copy_command()
        self.assertTrue(result)

        # execute validator to verify the number of page range for uploaded blob.
        result = util.Command("testBlob").add_arguments(file_path).add_arguments(copy_destination_sas). \
            add_flags("blob-type", "PageBlob").add_flags("verify-block-size", "true"). \
            add_flags("number-blocks-or-pages", str(number_of_page_ranges)).execute_azcopy_verify()
        self.assertTrue(result)

    def test_set_page_blob_tier(self):
        # test for P10 Page Blob Access Tier
        filename = "test_page_P10_blob_tier.vhd"
        file_path = util.create_test_file(filename, 100 * 1024)

        destination_sas = util.get_resource_sas_from_premium_container_sas(filename)
        result = util.Command("copy").add_arguments(file_path).add_arguments(destination_sas). \
            add_flags("log-level", "info").add_flags("blob-type","PageBlob").add_flags("page-blob-tier", "P10").execute_azcopy_copy_command()
        self.assertTrue(result)

        # execute azcopy validate order.
        # added the expected blob-tier "P10"
        result = util.Command("testBlob").add_arguments(file_path).add_arguments(destination_sas).\
                    add_flags("blob-type","PageBlob"). add_flags("blob-tier", "P10").execute_azcopy_verify()
        self.assertTrue(result)

        # test for P20 Page Blob Access Tier
        filename = "test_page_P20_blob_tier.vhd"
        file_path = util.create_test_file(filename, 100 * 1024)
        destination_sas = util.get_resource_sas_from_premium_container_sas(filename)
        result = util.Command("copy").add_arguments(file_path).add_arguments(destination_sas). \
            add_flags("log-level", "info").add_flags("blob-type","PageBlob").add_flags("page-blob-tier", "P20").execute_azcopy_copy_command()
        self.assertTrue(result)

        # execute azcopy validate order.
        # added the expected blob-tier "P20"
        result = util.Command("testBlob").add_arguments(file_path).add_arguments(destination_sas).\
                    add_flags("blob-type","PageBlob") .add_flags("blob-tier", "P20").execute_azcopy_verify()
        self.assertTrue(result)

        # test for P30 Page Blob Access Tier
        filename = "test_page_P30_blob_tier.vhd"
        file_path = util.create_test_file(filename, 100 * 1024)
        destination_sas = util.get_resource_sas_from_premium_container_sas(filename)
        result = util.Command("copy").add_arguments(file_path).add_arguments(destination_sas). \
            add_flags("log-level", "info").add_flags("blob-type","PageBlob").add_flags("page-blob-tier", "P30").execute_azcopy_copy_command()
        self.assertTrue(result)

        # execute azcopy validate order.
        # added the expected blob-tier "P30"
        result = util.Command("testBlob").add_arguments(file_path).add_arguments(destination_sas). \
            add_flags("blob-type", "PageBlob").add_flags("blob-tier", "P30").execute_azcopy_verify()
        self.assertTrue(result)

        # test for P4 Page Blob Access Tier
        filename = "test_page_P4_blob_tier.vhd"
        file_path = util.create_test_file(filename, 100 * 1024)
        destination_sas = util.get_resource_sas_from_premium_container_sas(filename)
        result = util.Command("copy").add_arguments(file_path).add_arguments(destination_sas). \
            add_flags("log-level", "info").add_flags("blob-type","PageBlob").add_flags("page-blob-tier", "P4").execute_azcopy_copy_command()
        self.assertTrue(result)

        # execute azcopy validate order.
        # added the expected blob-tier "P4"
        result = util.Command("testBlob").add_arguments(file_path).add_arguments(destination_sas). \
            add_flags("blob-type", "PageBlob").add_flags("blob-tier", "P4").execute_azcopy_verify()
        self.assertTrue(result)

        # test for P40 Page Blob Access Tier
        filename = "test_page_P40_blob_tier.vhd"
        file_path = util.create_test_file(filename, 100 * 1024)
        destination_sas = util.get_resource_sas_from_premium_container_sas(filename)
        result = util.Command("copy").add_arguments(file_path).add_arguments(destination_sas). \
            add_flags("log-level", "info").add_flags("blob-type","PageBlob").add_flags("page-blob-tier", "P40").execute_azcopy_copy_command()
        self.assertTrue(result)

        # execute azcopy validate order.
        # added the expected blob-tier "P40"
        result = util.Command("testBlob").add_arguments(file_path).add_arguments(destination_sas). \
            add_flags("blob-type", "PageBlob").add_flags("blob-tier", "P40").execute_azcopy_verify()
        self.assertTrue(result)

        # test for P50 Page Blob Access Tier
        filename = "test_page_P50_blob_tier.vhd"
        file_path = util.create_test_file(filename, 100 * 1024)
        destination_sas = util.get_resource_sas_from_premium_container_sas(filename)
        result = util.Command("copy").add_arguments(file_path).add_arguments(destination_sas). \
            add_flags("log-level", "info").add_flags("blob-type","PageBlob").add_flags("page-blob-tier", "P50").execute_azcopy_copy_command()
        self.assertTrue(result)

        # execute azcopy validate order.
        # added the expected blob-tier "P50"
        result = util.Command("testBlob").add_arguments(file_path).add_arguments(destination_sas). \
            add_flags("blob-type", "PageBlob").add_flags("blob-tier", "P50").execute_azcopy_verify()
        self.assertTrue(result)

        # test for P6 Page Blob Access Tier
        filename = "test_page_P6_blob_tier.vhd"
        file_path = util.create_test_file(filename, 100 * 1024)
        destination_sas = util.get_resource_sas_from_premium_container_sas(filename)
        result = util.Command("copy").add_arguments(file_path).add_arguments(destination_sas). \
            add_flags("log-level", "info").add_flags("blob-type","PageBlob").add_flags("page-blob-tier", "P6").execute_azcopy_copy_command()
        self.assertTrue(result)

        # execute azcopy validate order.
        # added the expected blob-tier "P50"
        result = util.Command("testBlob").add_arguments(file_path).add_arguments(destination_sas). \
            add_flags("blob-type", "PageBlob").add_flags("blob-tier", "P6").execute_azcopy_verify()
        self.assertTrue(result)