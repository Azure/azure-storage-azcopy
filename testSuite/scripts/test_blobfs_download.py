import json
import os
import shutil
from collections import namedtuple
from stat import *

import utility as util

def test_blobfs_download_1Kb_file():
    # create file of size 1KB
    filename = "test_blob_d_1kb_file.txt"
    file_path = util.create_test_file(filename, 1024)
    # upload the file using Azcopy
    result = util.Command("copy").add_arguments(file_path).add_arguments(util.test_bfs_account_url). \
        add_flags("Logging", "Info").execute_azcopy_copy_command()
    if not result:
        print("test_blobfs_download_1Kb_file failed uploading the 1kb file ", filename, " to the filesystem")
        return
    # Validate the file uploaded file
    fileUrl = util.test_bfs_account_url + filename
    result = util.Command("testBlobFS").add_arguments(file_path).add_arguments(fileUrl).execute_azcopy_verify()
    if not result:
        print("test_blobfs_download_1Kb_file failed while validating the 1kb file upload")
        return
    # delete the file locally
    try:
        os.remove(file_path)
    except:
        print("test_blobfs_download_1Kb_file failed while deleting the locally created file")
        return

    # download the file using Azcopy
    result = util.Command("copy").add_arguments(fileUrl).add_arguments(util.test_directory_path). \
        add_flags("Logging", "Info").execute_azcopy_copy_command()
    if not result:
        print("test_blobfs_download_1Kb_file failed while downloading the 1kb file")
        return
    # validate the downloaded file
    result = util.Command("testBlobFS").add_arguments(file_path).add_arguments(fileUrl).execute_azcopy_verify()
    if not result:
        print("test_blobfs_download_1Kb_file failed while validating the 1kb file download")
        return
    print("test_blobfs_download_1Kb_file successfully passed")

def test_blobfs_download_64MB_file():
    # create test file of size 64MB
    filename = "test_blob_d_64MB_file.txt"
    file_path = util.create_test_file(filename, 64*1024*1024)
    # Upload the file using Azcopy
    result = util.Command("copy").add_arguments(file_path).add_arguments(util.test_bfs_account_url). \
        add_flags("Logging", "Info").execute_azcopy_copy_command()
    if not result:
        print("test_blobfs_download_64MB_file failed uploading the 64MB file ", filename, " to the filesystem")
        return
    # Validate the file uploaded
    fileUrl = util.test_bfs_account_url + filename
    result = util.Command("testBlobFS").add_arguments(file_path).add_arguments(fileUrl).execute_azcopy_verify()
    if not result:
        print("test_blobfs_download_64MB_file failed while validating the 64MB file upload")
        return

    # delete the file locally
    try:
        os.remove(file_path)
    except:
        print("test_blobfs_download_64MB_file failed while deleting the file locally")
        return

    # download the file using azcopy
    result = util.Command("copy").add_arguments(fileUrl).add_arguments(util.test_directory_path). \
        add_flags("Logging", "Info").execute_azcopy_copy_command()
    if not result:
        print("test_blobfs_download_64MB_file failed downloading the 64MB file ", filename)
        return

    # validate the downloaded file
    result = util.Command("testBlobFS").add_arguments(file_path).add_arguments(fileUrl).execute_azcopy_verify()
    if not result:
        print("test_blobfs_download_64MB_file failed while validating the 64MB downloaded file")
        return
    print("test_blobfs_download_64MB_file successfully passed")

def test_blobfs_download_100_1Kb_file():
    # create dir with 100 1KB files inside it
    dir_name = "dir_blobfs_d_100_1K"
    dir_n_file_path = util.create_test_n_files(1024, 100, dir_name)

    # Upload the directory with 100 files inside it
    result = util.Command("copy").add_arguments(dir_n_file_path).add_arguments(util.test_bfs_account_url). \
        add_flags("Logging", "Info").add_flags("recursive","true").execute_azcopy_copy_command()
    if not result:
        print("test_blobfs_download_100_1Kb_file failed uploading the dir ", dir_name, " to the filesystem")
        return

    # Validate the uploaded directory
    dirUrl = util.test_bfs_account_url + dir_name
    result = util.Command("testBlobFS").add_arguments(dir_n_file_path).add_arguments(dirUrl). \
        add_flags("is-object-dir", "true").execute_azcopy_verify()
    if not result:
        print("test_blobfs_download_100_1Kb_file failed while validating the uploaded directory")
        return

    # delete the local directory created
    try:
        shutil.rmtree(dir_n_file_path)
    except:
        print("test_blobfs_download_100_1Kb_file failed while deleting the local directory")
        return

    # download the directory
    result = util.Command("copy").add_arguments(dirUrl).add_arguments(util.test_directory_path).\
        add_flags("Logging", "Info").add_flags("recursive", "true").execute_azcopy_copy_command()
    if not result:
        print("test_blobfs_download_100_1Kb_file failed while downloading the directory")
        return

    # validate the downloaded directory
    result = util.Command("testBlobFS").add_arguments(dir_n_file_path).add_arguments(dirUrl).\
            add_flags("is-object-dir", "true").execute_azcopy_verify()
    if not result:
        print("test_blobfs_download_100_1Kb_file failed while validating the downloaded directory ")
        return
    print("test_blobfs_download_100_1Kb_file successfully passed")

def test_blobfs_download_2000_1Kb_file():
    # create dir with 100 1KB files inside it
    dir_name = "dir_blobfs_2000_1K"
    dir_n_file_path = util.create_test_n_files(1024, 2000, dir_name)

    # Upload the directory with 2000 files inside it
    result = util.Command("copy").add_arguments(dir_n_file_path).add_arguments(util.test_bfs_account_url). \
        add_flags("Logging", "Info").add_flags("recursive","true").execute_azcopy_copy_command()
    if not result:
        print("test_blobfs_upload_2000_1Kb_file failed uploading the dir ", dir_name, " to the filesystem")
        return

    # Validate the uploaded directory
    dirUrl = util.test_bfs_account_url + dir_name
    result = util.Command("testBlobFS").add_arguments(dir_n_file_path).add_arguments(dirUrl). \
        add_flags("is-object-dir", "true").execute_azcopy_verify()
    if not result:
        print("test_blobfs_upload_2000_1Kb_file failed while validating the uploaded directory")
        return
    print("test_blobfs_upload_2000_1Kb_file successfully passed")