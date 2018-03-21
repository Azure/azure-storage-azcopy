from scripts.utility import *
import time

# test_download_1kb_blob verifies the download of 1Kb blob using azcopy.
def test_download_1kb_blob() :
    # create file of size 1KB.
    filename = "test_download_1kb_blob.txt"
    file_path = create_test_file(filename, 1024)

    # Upload 1KB file using azcopy.
    result = Command("copy").add_arguments(file_path). \
        add_flags("Logging", "5").add_flags("recursive", "true").execute_azcopy_copy_command()
    if not result:
        print("failed uploading 1KB file to the container")
        return

    # Verifying the uploaded blob.
    result = Command("testBlob").add_arguments(filename).execute_azcopy_verify()
    if not result:
        print("test_1kb_file test failed")
        return

    # removing the uploaded file in order to download it further.
    try:
        os.remove(file_path)
    except:
        print("error removing the uploaded file")
        return

    time.sleep(5)

    # downloading the uploaded file
    result = Command("copy").add_arguments(filename).add_flags("Logging", "5").execute_azcopy_copy_command("download")

    if not result:
        print("test_download_1kb_blob test case failed")
        return

    # Verifying the downloaded blob
    result = Command("testBlob").add_arguments(filename).execute_azcopy_verify()

    if not result:
        print("test_download_1kb_blob test case failed")
        return

    print("test_download_1kb_blob successfully passed")

# test_download_perserve_last_modified_time verifies the azcopy downloaded file
# and its modified time preserved locally on disk
def test_download_perserve_last_modified_time() :
    # create a file of 2KB
    filename = "test_download_preserve_last_mtime.txt"
    file_path = create_test_file(filename, 2048)

    # upload file through azcopy.
    result = Command("copy").add_arguments(file_path). \
        add_flags("Logging", "5").add_flags("recursive", "true").execute_azcopy_copy_command()
    if not result:
        print("failed uploading 1KB file to the container")
        return

    # Verifying the uploaded blob
    result = Command("testBlob").add_arguments(filename).execute_azcopy_verify()
    if not result:
        print("test_1kb_file test failed")
        return

    # removing the uploaded file on disk to download it further.
    try:
        os.remove(file_path)
    except:
        print("error removing the uploaded file")
        return

    time.sleep(5)

    # download file through azcopy with flag preserve-last-modified-time set to true
    result = Command("copy").add_arguments(filename).add_flags("Logging", "5").add_flags("preserve-last-modified-time", "true").execute_azcopy_copy_command("download")
    if not result:
        print("test_download_1kb_blob test case failed")
        return

    # Verifying the downloaded blob and its modified with the modified time of blob.
    result = Command("testBlob").add_arguments(filename).add_flags("preserve-last-modified-time", "true").execute_azcopy_verify()
    if not result:
        print("test_download_1kb_blob test case failed")
        return

    print("test_download_1kb_blob successfully passed")

# test_blob_download_63mb_in_4mb downloads 63mb file in block of 4mb through azcopy
def test_blob_download_63mb_in_4mb():
    # create file of 63mb
    file_name = "test_63mb_in4mb_download.txt"
    file_path = create_test_file(file_name, 63*1024*1024)

    # uploading file through azcopy with flag block-size set to 4194304 i.e 4mb
    result = Command("copy").add_arguments(file_path).add_flags("Logging", "5").add_flags("block-size", "4194304").execute_azcopy_copy_command()
    if not result:
        print("error uploading 63 mb file. test_blob_download_63mb_in_4mb test case failed")
        return

    # verify the uploaded file.
    result = Command("testBlob").add_arguments(file_name).execute_azcopy_verify()
    if not result:
        print("error verifying the 63mb upload. test_blob_download_63mb_in_4mb test case failed")
        return

    # downloading the created parallely in blocks of 4mb file through azcopy.
    result = Command("copy").add_arguments(file_name).add_flags("Logging", "5").add_flags("block-size", "4194304").execute_azcopy_copy_command("download")
    if not result:
        print("error downloading the 63mb file. test_blob_download_63mb_in_4mb test case failed")
        return

    # verify the downloaded file
    result = Command("testBlob").add_arguments(file_name).execute_azcopy_verify()
    if not result:
        print("test_blob_download_63mb_in_4mb test case failed.")
        return

    print("test_blob_download_63mb_in_4mb test case successfully passed")

# test_recursive_download_blob downloads a directory recursively from container through azcopy
def test_recursive_download_blob():
    # create directory and 5 files of 1KB inside that directory.
    dir_name = "dir_"+str(10)+" _files"
    dir1_path = create_test_n_files(1024, 5, dir_name)

    # upload the directory to container through azccopy with recursive set to true.
    result = Command("copy").add_arguments(dir1_path).add_flags("Logging", "5").add_flags("recursive", "true").execute_azcopy_copy_command()
    if not result:
        print("error uploading recursive dir ", dir1_path)
        return

    # verify the uploaded file.
    result = Command("testBlob").add_arguments(dir1_path).add_flags("is-object-dir","true").execute_azcopy_verify()
    if not result:
        print("error verify the recursive dir ", dir1_path, " upload")
        return

    # remove the uploaded file on disk to download it further.
    try:
        shutil.rmtree(dir1_path)
    except:
        print("error deleting the recursive directory ")
        return

    # downloading the directory created from container through azcopy with recursive flag to true.
    result = Command("copy").add_arguments(dir_name).add_flags("Logging", "5").add_flags("recursive", "true").execute_azcopy_copy_command("download")
    if not result:
        print("error download recursive dir ", dir1_path)
        return

    # verify downloaded blob.
    result = Command("testBlob").add_arguments(dir_name).add_flags("is-object-dir","true").execute_azcopy_verify()
    if not result:
        print("error verifying the recursive download ")
        return

    print("test_recursive_download_blob successfully passed")