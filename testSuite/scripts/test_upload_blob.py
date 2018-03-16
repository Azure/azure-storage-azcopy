from scripts.utility import *

def test_1kb_blob_upload(test_dir_path, container_sas):
    # Creating a single File Of size 1 KB
    filename = "test1KB.txt"
    file_path = create_test_file(test_dir_path, filename, 1024)

    # Uploading the single file of size 1KB
    command = "copy " + file_path + " " + '"' +container_sas + '"' +" --Logging 5 --recursive"
    result = execute_azcopy_command(test_dir_path, command)
    if (result == False):
        print("failed uploading 1KB file to the container")
        return

    # Verifying the uploaded blob
    # getting the file shared access signature from the container sas
    filesas = get_resource_sas(container_sas, filename)

    # calling the testblob validator to verify whether blob has been successfully uploaded or not
    command = "testBlob " + '"' + filesas + '" ' + file_path
    result = verify_operation(test_dir_path, command)
    if result == False:
        print("test_1kb_file test failed")
        return
    print("test_1kb_file successfully passed")

def test_63mb_blob_upload(test_dir_path, container_sas):
    filename = "test63Mb_blob.txt"
    file_path = create_test_file(test_dir_path, filename, 63 * 1024 * 1024)
    cmnd = "copy " + file_path + " " + ' "' + container_sas + '"' + " --Logging 5 --recursive --block-size " + str(100 * 1024 * 1024)
    result = execute_azcopy_command(test_dir_path, cmnd)

    if (result == False):
        print("failed uploading 1KB file to the container")
        return

    # Verifying the uploaded blob
    # geting the file shared access signature from the container sas
    filesas = get_resource_sas(container_sas, filename)

    # calling the testblob validator to verify whether blob has been successfully uploaded or not
    cmnd = "testBlob " + '"' + filesas + '" ' + file_path
    result = verify_operation(test_dir_path, cmnd)
    if result == False:
        print("test_63MB_file test failed")
        return
    print("test_63MB_file successfully passed")