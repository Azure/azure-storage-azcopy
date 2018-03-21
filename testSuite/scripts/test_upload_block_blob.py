from scripts.utility import *

# test_1kb_blob_upload verifies the 1KB blob upload by azcopy.
def test_1kb_blob_upload():
    # Creating a single File Of size 1 KB
    filename = "test1KB.txt"
    file_path = create_test_file(filename, 1024)

    # executing the azcopy command
    result = Command("copy").add_arguments(file_path).\
                add_flags("Logging", "5").add_flags("recursive", "true").execute_azcopy_copy_command()
    if not result:
        print("failed uploading 1KB file to the container")
        return

    # Verifying the uploaded blob
    result = Command("testBlob").add_arguments(filename).execute_azcopy_verify()
    if not result:
        print("test_1kb_file test failed")
    else:
        print("test_1kb_file successfully passed")

# test_63mb_blob_upload verifies the azcopy upload of 63mb blob upload.
def test_63mb_blob_upload():
    # creating file of 63mb size.
    filename = "test63Mb_blob.txt"
    file_path = create_test_file(filename, 8 * 1024 * 1024)

    # execute azcopy copy upload.
    result = Command("copy").add_arguments(file_path).add_flags("Logging", "5").\
        add_flags("block-size", "104857600").add_flags("recursive", "true").execute_azcopy_copy_command()
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

# test_n_1kb_blob_upload verifies the upload of n 1kb blob to the container.
def test_n_1kb_blob_upload(number_of_files):
    # create dir dir_n_files and 1 kb files inside the dir.
    dir_name = "dir_"+str(number_of_files)+" _files"
    dir_n_files_path = create_test_n_files(1024, number_of_files, dir_name)

    # execute azcopy command
    result = Command("copy").add_arguments(dir_n_files_path).add_flags("recursive", "true").add_flags("Logging", "5").execute_azcopy_copy_command()
    if not result:
        print("test_n_1kb_blob_upload failed while uploading ", number_of_files, " files to the container")
        return

    # execute the validator.
    result = Command("testBlob").add_arguments(dir_name).add_flags("is-object-dir","true").execute_azcopy_verify()
    if not result:
        print("test_n_1kb_blob_upload test case failed")
    else:
        print("test_n_1kb_blob_upload passed successfully")

# test_metaData_content_encoding_content_type verifies the meta data, content type,
# content encoding of 2kb upload to container through azcopy.
def test_metaData_content_encoding_content_type():
    # create 2kb file test_mcect.txt
    filename = "test_mcect.txt"
    file_path = create_test_file(filename, 2048)

    # execute azcopy upload command.
    result = Command("copy").add_arguments(file_path).add_flags("Logging", "5"). \
                        add_flags("recursive", "true").add_flags("metadata", "author=prjain;viewport=width;description=test file").\
                        add_flags("content-type", "testctype").add_flags("content-encoding", "testenc").add_flags("no-guess-mime-type", "true").execute_azcopy_copy_command()
    if not result:
        print("uploading 2KB file with metadata, content type and content-encoding failed")
        return

    print("Successfully uploaded 2KB file with meta data, content-type and content-encoding")

    # execute azcopy validate order.
    result = Command("testBlob").add_arguments(filename).add_flags("metadata", "author=prjain;viewport=width;description=test file"). \
        add_flags("content-type", "testctype").add_flags("content-encoding", "testenc").add_flags("no-guess-mime-type", "true").execute_azcopy_verify()
    if not result:
        print("test_metaData_content_encoding_content_type failed")
    else:
        print("test_metaData_content_encoding_content_type successfully passed")

# test_1G_blob_upload verifies the azcopy upload of 1Gb blob upload in blocks of 100 Mb
def test_1G_blob_upload():
    # create 1Gb file
    filename = "test_1G_blob.txt"
    file_path =create_test_file(filename, 1*1024*1024*1024)

    # execute azcopy upload
    result = Command("copy").add_arguments(file_path).add_flags("Logging", "5"). \
        add_flags("block-size", "104857600").add_flags("recursive", "true").execute_azcopy_copy_command()
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


# test_block_size verifies azcopy upload of blob in blocks of given block-size
# performs the upload, verify the blob and number of blocks.
def test_block_size(block_size):

    #create file of size 63 Mb
    filename = "test63Mb_blob.txt"
    file_path = create_test_file(filename, 63 * 1024 * 1024)

    # execute azcopy upload of 63 Mb file.
    result = Command("copy").add_arguments(file_path).add_flags("Logging", "5"). \
        add_flags("block-size", str(block_size)).add_flags("recursive", "true").execute_azcopy_copy_command()
    if not result:
        print("failed uploading file", filename, " with block size 4MB to the container")
        return
    print("successfully uploaded file ", filename, "with block size 4MB")

    # Verifying the uploaded blob
    # calling the testblob validator to verify whether blob has been successfully uploaded or not
    if (63*1024*1024) % block_size == 0:
        number_of_blocks = int(63*1024*1024 / block_size)
    else:
        number_of_blocks = int(63*1024*1024 / block_size) + 1
    result = Command("testBlob").add_arguments(filename).add_flags("verify-block-size", "true").add_flags("number-blocks-or-pages", str(number_of_blocks)).execute_azcopy_verify()
    if not result:
        print("test_block_size test failed")
        return
    print("test_block_size successfully passed")


# test_guess_mime_type verifies the mime type detection by azcopy while uploading the blob
def test_guess_mime_type():
    # create a test html file
    filename = "test_guessmimetype.html"
    file_path = create_test_html_file(filename)

    # execute azcopy upload of html file.
    result = Command("copy").add_arguments(file_path).add_flags("Logging", "5").\
        add_flags("recursive", "true").execute_azcopy_copy_command()
    if not result:
        print("uploading file ", filename, " failed")
        return

    # execute the validator to verify the content-type.
    result = Command("testBlob").add_arguments(file_path).add_flags("Logging", "5").\
        add_flags("recursive", "true")
    if not result:
        print("test_guess_mime_type test failed")
    else:
        print("test_guess_mime_type successfully passed")