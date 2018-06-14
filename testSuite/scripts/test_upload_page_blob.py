import utility as util
import sys

# test_page_blob_upload_1mb verifies the azcopy upload of 1mb file
# as a page blob.
def test_page_blob_upload_1mb():
    # create the test gile.
    file_name = "test_page_blob_1mb.vhd"
    file_path = util.create_test_file(file_name, 1024 * 1024)

    # execute azcopy upload.
    destination = util.get_resource_sas(file_name)
    result = util.Command("copy").add_arguments(file_path).add_arguments(destination).add_flags("log-level", "info"). \
        add_flags("block-size", "4194304").execute_azcopy_copy_command()
    if not result:
        print("uploading file ", file_name, " failed")
        sys.exit(1)

    # execute validator.
    result = util.Command("testBlob").add_arguments(file_path).add_arguments(destination).add_flags("blob-type",
                                                                                                    "PageBlob").execute_azcopy_verify()
    if not result:
        print("test_page_blob_upload_512B test case failed")
        sys.exit(1)
    print("test_page_blob_upload_512B test case passed successfully")


# test_page_range_for_complete_sparse_file verifies the number of Page ranges for
# complete empty file i.e each character is Null character.
def test_page_range_for_complete_sparse_file():
    # create test file.
    file_name = "sparse_file.vhd"
    file_path = util.create_complete_sparse_file(file_name, 4 * 1024 * 1024)

    # execute azcopy page blob upload.
    destination_sas = util.get_resource_sas(file_name)
    result = util.Command("copy").add_arguments(file_path).add_arguments(destination_sas).add_flags("log-level", "info"). \
        add_flags("block-size", "4194304").execute_azcopy_copy_command()
    if not result:
        print("uploading file ", file_name, " failed")
        sys.exit(1)

    # execute validator.
    # no of page ranges should be 0 for the empty sparse file.
    result = util.Command("testBlob").add_arguments(file_path).add_arguments(destination_sas).add_flags("blob-type",
                                                                                                        "PageBlob").add_flags(
        "verify-block-size", "true").add_flags("number-blocks-or-pages", "0").execute_azcopy_verify()
    if not result:
        print("test_page_range_for_sparse_file test case failed")
        sys.exit(1)
    print("test_page_range_for_sparse_file test case passed successfully")


# test_page_blob_upload_partial_sparse_file verifies the number of page ranges
# for PageBlob upload by azcopy.
def test_page_blob_upload_partial_sparse_file():
    # create test file.
    file_name = "test_partial_sparse_file.vhd"
    file_path = util.create_partial_sparse_file(file_name, 16 * 1024 * 1024)

    # execute azcopy pageblob upload.
    destination_sas = util.get_resource_sas(file_name)
    result = util.Command("copy").add_arguments(file_path).add_arguments(destination_sas).add_flags("log-level", "info"). \
        add_flags("block-size", "4194304").execute_azcopy_copy_command()
    if not result:
        print("uploading file ", file_name, " failed")
        sys.exit(1)

    # number of page range for partial sparse created above will be (size/2)
    number_of_page_ranges = int((16 * 1024 * 1024 / (4 * 1024 * 1024)) / 2)
    # execute validator to verify the number of page range for uploaded blob.
    result = util.Command("testBlob").add_arguments(file_path).add_arguments(destination_sas). \
        add_flags("blob-type", "PageBlob").add_flags("verify-block-size", "true"). \
        add_flags("number-blocks-or-pages", str(number_of_page_ranges)).execute_azcopy_verify()
    if not result:
        print("test_page_blob_upload_partial_sparse_file test case failed")
        sys.exit(1)
    print("test_page_blob_upload_partial_sparse_file test case passed successfully")


def test_set_page_blob_tier():
    # test for P10 Page Blob Access Tier
    filename = "test_page_P10_blob_tier.vhd"
    file_path = util.create_test_file(filename, 100 * 1024)

    destination_sas = util.get_resource_sas_from_premium_container_sas(filename)
    result = util.Command("copy").add_arguments(file_path).add_arguments(destination_sas). \
        add_flags("log-level", "info").add_flags("page-blob-tier", "P10").execute_azcopy_copy_command()
    if not result:
        print("uploading file with page-blob-tier set to P10 failed.")
        sys.exit(1)
    # execute azcopy validate order.
    # added the expected blob-tier "P10"
    result = util.Command("testBlob").add_arguments(file_path).add_arguments(destination_sas).add_flags("blob-type",
                                                                                                        "PageBlob"). \
        add_flags("blob-tier", "P10").execute_azcopy_verify()
    if not result:
        print("test_set_page_blob_tier failed for P10 access Tier Type")
        sys.exit(1)

    # test for P20 Page Blob Access Tier
    filename = "test_page_P20_blob_tier.vhd"
    file_path = util.create_test_file(filename, 100 * 1024)
    destination_sas = util.get_resource_sas_from_premium_container_sas(filename)
    result = util.Command("copy").add_arguments(file_path).add_arguments(destination_sas). \
        add_flags("log-level", "info").add_flags("page-blob-tier", "P20").execute_azcopy_copy_command()
    if not result:
        print("uploading file with page-blob-tier set to P20 failed.")
        sys.exit(1)
    # execute azcopy validate order.
    # added the expected blob-tier "P20"
    result = util.Command("testBlob").add_arguments(file_path).add_arguments(destination_sas).add_flags("blob-type",
                                                                                                        "PageBlob") \
        .add_flags("blob-tier", "P20").execute_azcopy_verify()
    if not result:
        print("test_set_page_blob_tier failed for P20 access Tier Type")
        sys.exit(1)

    # test for P30 Page Blob Access Tier
    filename = "test_page_P30_blob_tier.vhd"
    file_path = util.create_test_file(filename, 100 * 1024)
    destination_sas = util.get_resource_sas_from_premium_container_sas(filename)
    result = util.Command("copy").add_arguments(file_path).add_arguments(destination_sas). \
        add_flags("log-level", "info").add_flags("page-blob-tier", "P30").execute_azcopy_copy_command()
    if not result:
        print("uploading file with page-blob-tier set to P30 failed.")
        sys.exit(1)
    # execute azcopy validate order.
    # added the expected blob-tier "P30"
    result = util.Command("testBlob").add_arguments(file_path).add_arguments(destination_sas). \
        add_flags("blob-type", "PageBlob").add_flags("blob-tier", "P30").execute_azcopy_verify()
    if not result:
        print("test_set_page_blob_tier failed for P30 access Tier Type")
        sys.exit(1)

    # test for P4 Page Blob Access Tier
    filename = "test_page_P4_blob_tier.vhd"
    file_path = util.create_test_file(filename, 100 * 1024)
    destination_sas = util.get_resource_sas_from_premium_container_sas(filename)
    result = util.Command("copy").add_arguments(file_path).add_arguments(destination_sas). \
        add_flags("log-level", "info").add_flags("page-blob-tier", "P4").execute_azcopy_copy_command()
    if not result:
        print("uploading file with page-blob-tier set to P4 failed.")
        sys.exit(1)
    # execute azcopy validate order.
    # added the expected blob-tier "P4"
    result = util.Command("testBlob").add_arguments(file_path).add_arguments(destination_sas). \
        add_flags("blob-type", "PageBlob").add_flags("blob-tier", "P4").execute_azcopy_verify()
    if not result:
        print("test_set_page_blob_tier failed for P4 access Tier Type")
        sys.exit(1)

    # test for P40 Page Blob Access Tier
    filename = "test_page_P40_blob_tier.vhd"
    file_path = util.create_test_file(filename, 100 * 1024)
    destination_sas = util.get_resource_sas_from_premium_container_sas(filename)
    result = util.Command("copy").add_arguments(file_path).add_arguments(destination_sas). \
        add_flags("log-level", "info").add_flags("page-blob-tier", "P40").execute_azcopy_copy_command()
    if not result:
        print("uploading file with page-blob-tier set to P40 failed.")
        sys.exit(1)
    # execute azcopy validate order.
    # added the expected blob-tier "P40"
    result = util.Command("testBlob").add_arguments(file_path).add_arguments(destination_sas). \
        add_flags("blob-type", "PageBlob").add_flags("blob-tier", "P40").execute_azcopy_verify()
    if not result:
        print("test_set_page_blob_tier failed for P40 access Tier Type")
        sys.exit(1)

    # test for P50 Page Blob Access Tier
    filename = "test_page_P50_blob_tier.vhd"
    file_path = util.create_test_file(filename, 100 * 1024)
    destination_sas = util.get_resource_sas_from_premium_container_sas(filename)
    result = util.Command("copy").add_arguments(file_path).add_arguments(destination_sas). \
        add_flags("log-level", "info").add_flags("page-blob-tier", "P50").execute_azcopy_copy_command()
    if not result:
        print("uploading file with page-blob-tier set to P50 failed.")
        sys.exit(1)
    # execute azcopy validate order.
    # added the expected blob-tier "P50"
    result = util.Command("testBlob").add_arguments(file_path).add_arguments(destination_sas). \
        add_flags("blob-type", "PageBlob").add_flags("blob-tier", "P50").execute_azcopy_verify()
    if not result:
        print("test_set_page_blob_tier failed for P50 access Tier Type")
        sys.exit(1)

    # test for P6 Page Blob Access Tier
    filename = "test_page_P6_blob_tier.vhd"
    file_path = util.create_test_file(filename, 100 * 1024)
    destination_sas = util.get_resource_sas_from_premium_container_sas(filename)
    result = util.Command("copy").add_arguments(file_path).add_arguments(destination_sas). \
        add_flags("log-level", "info").add_flags("page-blob-tier", "P6").execute_azcopy_copy_command()
    if not result:
        print("uploading file with page-blob-tier set to P6 failed.")
        sys.exit(1)
    # execute azcopy validate order.
    # added the expected blob-tier "P50"
    result = util.Command("testBlob").add_arguments(file_path).add_arguments(destination_sas). \
        add_flags("blob-type", "PageBlob").add_flags("blob-tier", "P6").execute_azcopy_verify()
    if not result:
        print("test_set_page_blob_tier failed for P6 access Tier Type")
        sys.exit(1)
    print("test_set_page_blob_tier successfully passed")
