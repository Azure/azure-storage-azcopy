import utility as util
import unittest
import os


class PageBlob_Upload_User_Scenarios(unittest.TestCase):
    # test_page_range_for_complete_sparse_file verifies the number of Page ranges for
    # complete empty file i.e each character is Null character.
    def test_page_range_for_complete_sparse_file(self):
        # step 1: uploading a sparse file should be optimized
        # create test file.
        file_name = "sparse_file.vhd"
        file_path = util.create_complete_sparse_file(file_name, 16 * 1024 * 1024)

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

        download_dest = util.test_directory_path + "/sparse_file_downloaded.vhd"
        result = util.Command("copy").add_arguments(copy_destination_sas).add_arguments(download_dest).add_flags(
            "log-level", "info").add_flags("block-size-mb", "1").execute_azcopy_copy_command()
        self.assertTrue(result)

        # Verifying the downloaded blob
        result = util.Command("testBlob").add_arguments(download_dest).add_arguments(
            copy_destination_sas).add_flags("blob-type", "PageBlob").execute_azcopy_verify()
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

        download_dest = util.test_directory_path + "/partial_sparse_file_downloaded.vhd"
        result = util.Command("copy").add_arguments(copy_destination_sas).add_arguments(download_dest).add_flags(
            "log-level", "info").add_flags("block-size-mb", "1").execute_azcopy_copy_command()
        self.assertTrue(result)

        # Verifying the downloaded blob
        result = util.Command("testBlob").add_arguments(download_dest).add_arguments(copy_destination_sas)\
            .add_flags("blob-type", "PageBlob").execute_azcopy_verify()
        self.assertTrue(result)
