from scripts.utility import *

def test_1kb_blob_upload():
    # Creating a single File Of size 1 KB
    filename = "test1KB.txt"
    file_path = create_test_file(filename, 1024)

    result = Command("copy").add_arguments(file_path).\
                add_flags("Logging", "5").add_flags("recursive", "true").execute_azcopy_command()

    #executing the azcopy command
    if not result:
        print("failed uploading 1KB file to the container")
        return

    # Verifying the uploaded blob
    result = Command("testBlob").add_arguments(filename).execute_azcopy_verify()

    if not result:
        print("test_1kb_file test failed")
    else:
        print("test_1kb_file successfully passed")

def test_63mb_blob_upload():
    filename = "test63Mb_blob.txt"
    file_path = create_test_file(filename, 63 * 1024 * 1024)

    result = Command("copy").add_arguments(file_path).add_flags("Logging", "5").\
        add_flags("block-size", "104857600").add_flags("recursive", "true").execute_azcopy_command()

    if not result:
        print("failed uploading file", filename, " to the container")
    else:
        print("successfully uploaded file ", filename)
    # Verifying the uploaded blob

    # calling the testblob validator to verify whether blob has been successfully uploaded or not
    result = Command("testBlob").add_arguments(filename).execute_azcopy_verify()

    if not result:
        print("test_63MB_file test failed")
    else:
        print("test_63MB_file successfully passed")


def test_n_1kb_blob_upload(number_of_files):

    dir_n_files_path = create_test_n_files(1024, number_of_files)

    result = Command("copy").add_arguments(dir_n_files_path).add_flags("recursive", "true").add_flags("Logging", "5").execute_azcopy_command()

    if not result:
        print("test_n_1kb_blob_upload failed while uploading ", number_of_files, " files to the container")
        return

    result = Command("testBlob").add_arguments(dir_n_files_path).add_flags("is-object-dir","true").execute_azcopy_verify()

    if not result:
        print("test_n_1kb_blob_upload test case failed")
    else:
        print("test_n_1kb_blob_upload passed successfully")


def test_metaData_content_encoding_content_type():
    filename = "test_mcect.txt"
    file_path = create_test_file(filename, 2048)
    result = Command("copy").add_arguments(file_path).add_flags("Logging", "5"). \
                        add_flags("recursive", "true").add_flags("metadata", "author=prjain;viewport=width;description=test file").\
                        add_flags("content-type", "testctype").add_flags("content-encoding", "testenc").add_flags("no-guess-mime-type", "true").execute_azcopy_command()
    if not result:
        print("uploading 2KB file with metadata, content type and content-encoding failed")
        return
    print("Successfully uploaded 2KB file with meta data, content-type and content-encoding")
    result = Command("testBlob").add_arguments(filename).add_flags("metadata", "author=prjain;viewport=width;description=test file"). \
        add_flags("content-type", "testctype").add_flags("content-encoding", "testenc").add_flags("no-guess-mime-type", "true").execute_azcopy_verify()

    if not result:
        print("test_metaData_content_encoding_content_type failed")
    else:
        print("test_metaData_content_encoding_content_type successfully passed")


def test_1G_blob_upload():
    filename = "test_1G_blob.txt"
    file_path =create_test_file(filename, 1*1024*1024*1024)
    result = Command("copy").add_arguments(file_path).add_flags("Logging", "5"). \
        add_flags("block-size", "104857600").add_flags("recursive", "true").execute_azcopy_command()
    if not result:
        print("failed uploading 1G file", filename, " to the container")
        return
    print("successfully uploaded 1G file ", filename)

    # Verifying the uploaded blob
    # calling the testblob validator to verify whether blob has been successfully uploaded or not
    result = Command("testBlob").add_arguments(filename).execute_azcopy_verify()

    if not result:
        print("test_1G_blob_upload test failed")
        return
    print("test_1G_blob_upload successfully passed")


def test_block_size(block_size):
    filename = "test63Mb_blob.txt"
    file_path = create_test_file(filename, 63 * 1024 * 1024)

    result = Command("copy").add_arguments(file_path).add_flags("Logging", "5"). \
        add_flags("block-size", str(block_size)).add_flags("recursive", "true").execute_azcopy_command()

    if not result:
        print("failed uploading file", filename, " with block size 4MB to the container")
        return
    print("successfully uploaded file ", filename, "with block size 4MB")
    # Verifying the uploaded blob

    # calling the testblob validator to verify whether blob has been successfully uploaded or not
    result = Command("testBlob").add_arguments(filename).add_flags("verify-block-size", "true").add_flags("block-size", "4194304").execute_azcopy_verify()

    if not result:
        print("test_block_size test failed")
        return
    print("test_block_size successfully passed")


def test_guess_mime_type():
    filename = "test_guessmimetype.html"
    file_path = create_test_html_file(filename)

    result = Command("copy").add_arguments(file_path).add_flags("Logging", "5").\
        add_flags("recursive", "true").execute_azcopy_command()

    if not result:
        print("uploading file ", filename, " failed")
        return

    result = Command("testBlob").add_arguments(file_path).add_flags("Logging", "5").\
        add_flags("recursive", "true")

    if not result:
        print("test_guess_mime_type test failed")
    else:
        print("test_guess_mime_type successfully passed")
