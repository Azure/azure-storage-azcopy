import utility as util
import time
import urllib
import shutil

# test_download_1kb_blob verifies the download of 1Kb blob using azcopy.
def test_download_1kb_blob() :
    # create file of size 1KB.
    filename = "test_1kb_blob_upload.txt"
    file_path = util.create_test_file(filename, 1024)

    # Upload 1KB file using azcopy.
    src = file_path
    dest = util.test_container_url
    result = util.Command("copy").add_arguments(src).add_arguments(dest). \
        add_flags("Logging", "5").add_flags("recursive", "true").execute_azcopy_copy_command()
    if not result:
        print("failed uploading 1KB file to the container")
        return

    # Verifying the uploaded blob.
    # the resource local path should be the first argument for the azcopy validator.
    # the resource sas should be the second argument for azcopy validator.
    resource_url = util.get_resource_sas(filename)
    result = util.Command("testBlob").add_arguments(file_path).add_arguments(resource_url).execute_azcopy_verify()
    if not result:
        print("test_1kb_file test failed")
        return

    time.sleep(5)

    # downloading the uploaded file
    src = util.get_resource_sas(filename)
    dest = util.test_directory_path + "/test_1kb_blob_download.txt"
    result = util.Command("copy").add_arguments(src).add_arguments(dest).add_flags("Logging", "5").execute_azcopy_copy_command()

    if not result:
        print("test_download_1kb_blob test case failed")
        return

    # Verifying the downloaded blob
    result = util.Command("testBlob").add_arguments(dest).add_arguments(src).execute_azcopy_verify()
    if not result:
        print("test_download_1kb_blob test case failed")
        return

    print("test_download_1kb_blob successfully passed")

# test_download_perserve_last_modified_time verifies the azcopy downloaded file
# and its modified time preserved locally on disk
def test_download_perserve_last_modified_time() :
    # create a file of 2KB
    filename = "test_upload_preserve_last_mtime.txt"
    file_path = util.create_test_file(filename, 2048)

    # upload file through azcopy.
    destination_sas = util.get_resource_sas(filename)
    result = util.Command("copy").add_arguments(file_path).add_arguments(destination_sas). \
        add_flags("Logging", "5").add_flags("recursive", "true").execute_azcopy_copy_command()
    if not result:
        print("failed uploading 1KB file to the container")
        return

    # Verifying the uploaded blob
    result = util.Command("testBlob").add_arguments(file_path).add_arguments(destination_sas).execute_azcopy_verify()
    if not result:
        print("test_download_perserve_last_modified_time test failed")
        return

    time.sleep(5)

    # download file through azcopy with flag preserve-last-modified-time set to true
    download_file_name = util.test_directory_path + "/test_download_preserve_last_mtime.txt"
    result = util.Command("copy").add_arguments(destination_sas).add_arguments(download_file_name).add_flags("Logging", "5").add_flags("preserve-last-modified-time", "true").execute_azcopy_copy_command()
    if not result:
        print("test_download_perserve_last_modified_time test case failed")
        return

    # Verifying the downloaded blob and its modified with the modified time of blob.
    result = util.Command("testBlob").add_arguments(download_file_name).add_arguments(destination_sas).add_flags("preserve-last-modified-time", "true").execute_azcopy_verify()
    if not result:
        print("test_download_perserve_last_modified_time test case failed")
        return

    print("test_download_perserve_last_modified_time successfully passed")

# test_blob_download_63mb_in_4mb downloads 63mb file in block of 4mb through azcopy
def test_blob_download_63mb_in_4mb():
    # create file of 63mb
    file_name = "test_63mb_in4mb_upload.txt"
    file_path = util.create_test_file(file_name, 63*1024*1024)

    # uploading file through azcopy with flag block-size set to 4194304 i.e 4mb
    destination_sas = util.get_resource_sas(file_name)
    result = util.Command("copy").add_arguments(file_path).add_arguments(destination_sas).add_flags("Logging", "5").add_flags("block-size", "4194304").execute_azcopy_copy_command()
    if not result:
        print("error uploading 63 mb file. test_blob_download_63mb_in_4mb test case failed")
        return

    # verify the uploaded file.
    result = util.Command("testBlob").add_arguments(file_path).add_arguments(destination_sas).execute_azcopy_verify()
    if not result:
        print("error verifying the 63mb upload. test_blob_download_63mb_in_4mb test case failed")
        return

    # downloading the created parallely in blocks of 4mb file through azcopy.
    download_file = util.test_directory_path + "/test_63mb_in4mb_download.txt"
    result = util.Command("copy").add_arguments(destination_sas).add_arguments(download_file).add_flags("Logging", "5").add_flags("block-size", "4194304").execute_azcopy_copy_command()
    if not result:
        print("error downloading the 63mb file. test_blob_download_63mb_in_4mb test case failed")
        return

    # verify the downloaded file
    result = util.Command("testBlob").add_arguments(download_file).add_arguments(destination_sas).execute_azcopy_verify()
    if not result:
        print("test_blob_download_63mb_in_4mb test case failed.")
        return

    print("test_blob_download_63mb_in_4mb test case successfully passed")

# test_recursive_download_blob downloads a directory recursively from container through azcopy
def test_recursive_download_blob():
    # create directory and 5 files of 1KB inside that directory.
    dir_name = "dir_"+str(10)+"_files"
    dir1_path = util.create_test_n_files(1024, 5, dir_name)

    # upload the directory to container through azcopy with recursive set to true.
    result = util.Command("copy").add_arguments(dir1_path).add_arguments(util.test_container_url).add_flags("Logging", "5").add_flags("recursive", "true").execute_azcopy_copy_command()
    if not result:
        print("error uploading recursive dir ", dir1_path)
        return

    # verify the uploaded file.
    destination_sas = util.get_resource_sas(dir_name)
    result = util.Command("testBlob").add_arguments(dir1_path).add_arguments(destination_sas).add_flags("is-object-dir","true").execute_azcopy_verify()
    if not result:
        print("error verify the recursive dir ", dir1_path, " upload")
        return

    try:
        shutil.rmtree(dir1_path)
    except OSError as e:
        print("error removing the uploaded files. ", e)
        return

    # downloading the directory created from container through azcopy with recursive flag to true.
    result = util.Command("copy").add_arguments(destination_sas).add_arguments(util.test_directory_path).add_flags("Logging", "5").add_flags("recursive", "true").execute_azcopy_copy_command()
    if not result:
        print("error download recursive dir ", dir1_path)
        return

    # verify downloaded blob.
    result = util.Command("testBlob").add_arguments(dir1_path).add_arguments(destination_sas).add_flags("is-object-dir","true").execute_azcopy_verify()
    if not result:
        print("error verifying the recursive download ")
        return
    print("test_recursive_download_blob successfully passed")

def test_download_file_with_special_characters():
    filename_special_characters = 'abc"|>rd*'
    resource_url = util.get_resource_sas(filename_special_characters)
    # creating the file with random characters and with file name having special characters.
    result = util.Command("create").add_arguments(resource_url).add_flags("resourceType", "blob").execute_azcopy_verify()
    if not result:
        print("error creating the file name ", filename_special_characters, " special characters")
        return
    result = util.Command("copy").add_arguments(resource_url).add_arguments(util.test_directory_path).add_flags("Logging", "5").execute_azcopy_copy_command()
    if not result:
        print("error downloading the file with special characters ", filename_special_characters)
        return
    expected_filename = urllib.parse.quote_plus(filename_special_characters)
    filepath = util.test_directory_path + "/" + expected_filename
    if not os.path.isfile(filepath):
        print("file not downloaded with expected file name")
        return
    result = util.Command("testBlob").add_arguments(filepath).add_arguments(resource_url).execute_azcopy_verify()
    if not result:
        print("error verifying the download of file ", filepath)
        return