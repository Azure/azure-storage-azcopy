from scripts.utility import *

def test_page_blob_upload_1mb():
    file_name = "test_page_blob_1mb.txt"
    file_path = create_test_file(file_name, 1024*1024)

    result = Command("copy").add_arguments(file_path).add_flags("Logging", "5").add_flags("blob-type", "PageBlob").execute_azcopy_copy_command()
    if not result:
        print("uploading file ", file_name, " failed")
        return

    result = Command("testBlob").add_arguments(file_name).add_flags("blob-type", "PageBlob").execute_azcopy_verify()
    if not result:
        print("test_page_blob_upload_512B test case failed")
        return
    print("test_page_blob_upload_512B test case passed successfully")

def test_page_range_for_sparse_file():
    file_name = "sparse_file.img"
    file_path = create_sparse_file(file_name, 4*1024*1024)
    result = Command("copy").add_arguments(file_path).add_flags("Logging", "5").add_flags("blob-type", "PageBlob").execute_azcopy_copy_command()
    if not result:
        print("uploading file ", file_name, " failed")
        return

    result = Command("testBlob").add_arguments(file_name).add_flags("blob-type", "PageBlob").add_flags("verify-block-size","true").add_flags("block-size","0").execute_azcopy_verify()
    if not result:
        print("test_page_range_for_sparse_file test case failed")
        return
    print("test_page_range_for_sparse_file test case passed successfully")

def test_page_blob_upload_100mb():
    file_name = "test_page_blob_100mb_12.txt"
    file_path = create_test_file(file_name, 100*1024*1024)

    result = Command("copy").add_arguments(file_path).add_flags("Logging", "5").add_flags("blob-type", "PageBlob").execute_azcopy_copy_command()
    if not result:
        print("uploading file ", file_name, " failed")
        return

    result = Command("testBlob").add_arguments(file_name).add_flags("blob-type", "PageBlob").add_flags("verify-block-size","true").add_flags("block-size","4194304").execute_azcopy_verify()
    if not result:
        print("test_page_blob_upload_100mb test case failed")
        return
    print("test_page_blob_upload_100mb test case passed successfully")
