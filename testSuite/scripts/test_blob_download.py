from scripts.utility import *
import time

def test_download_1kb_blob() :
    filename = "test_download_1kb_blob.txt"
    file_path = create_test_file(filename, 1024)

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

    try:
        os.remove(file_path)
    except:
        print("error removing the uploaded file")
        return

    time.sleep(10)

    result = Command("copy").add_arguments(filename).add_flags("Logging", "5").add_flags("preserve-last-modified-time", "true").execute_azcopy_copy_command("download")

    if not result:
        print("test_download_1kb_blob test case failed")
        return

    # Verifying the uploaded blob
    result = Command("testBlob").add_arguments(filename).execute_azcopy_verify()

    if not result:
        print("test_download_1kb_blob test case failed")
        return

    print("test_download_1kb_blob successfully passed")



def test_download_perserve_last_modified_time() :
    filename = "test_download_preserve_last_mtime.txt"
    file_path = create_test_file(filename, 2048)

    result = Command("copy").add_arguments(file_path). \
        add_flags("Logging", "5").add_flags("recursive", "true").execute_azcopy_copy_command()

    #executing the azcopy command
    if not result:
        print("failed uploading 1KB file to the container")
        return

    # Verifying the uploaded blob
    result = Command("testBlob").add_arguments(filename).execute_azcopy_verify()

    if not result:
        print("test_1kb_file test failed")
        return

    try:
        os.remove(file_path)
    except:
        print("error removing the uploaded file")
        return

    time.sleep(10)

    result = Command("copy").add_arguments(filename).add_flags("Logging", "5").add_flags("preserve-last-modified-time", "true").execute_azcopy_copy_command("download")

    if not result:
        print("test_download_1kb_blob test case failed")
        return

    # Verifying the uploaded blob
    result = Command("testBlob").add_arguments(filename).add_flags("preserve-last-modified-time", "true").execute_azcopy_verify()

    if not result:
        print("test_download_1kb_blob test case failed")
        return

    print("test_download_1kb_blob successfully passed")


