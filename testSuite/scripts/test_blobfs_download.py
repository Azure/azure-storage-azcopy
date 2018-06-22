import json
import os
import shutil
from collections import namedtuple
from stat import *
import sys
import utility as util

def test_blobfs_download_1Kb_file(
    explictFromTo=False,
    forceOAuthLogin=False,
    tenantID="",
    aadEndpoint=""):
    # create file of size 1KB
    filename = "test_blob_d_1kb_file.txt"
    file_path = util.create_test_file(filename, 1024)
    # upload the file using Azcopy
    cmd = util.Command("copy").add_arguments(file_path).add_arguments(util.test_bfs_account_url). \
        add_flags("log-level", "Info")
    util.process_oauth_command(
        cmd,
        "LocalBlobFS" if explictFromTo else "",
        forceOAuthLogin,
        tenantID,
        aadEndpoint)
    if forceOAuthLogin:
        result = cmd.execute_azcopy_command_interactive()
    else:
        result = cmd.execute_azcopy_copy_command()
        
    if not result:
        print("test_blobfs_download_1Kb_file failed uploading the 1kb file ", filename, " to the filesystem")
        sys.exit(1)
    # Validate the file uploaded file
    fileUrl = util.test_bfs_account_url + filename
    result = util.Command("testBlobFS").add_arguments(file_path).add_arguments(fileUrl).execute_azcopy_verify()
    if not result:
        print("test_blobfs_download_1Kb_file failed while validating the 1kb file upload")
        sys.exit(1)
    # delete the file locally
    try:
        os.remove(file_path)
    except:
        print("test_blobfs_download_1Kb_file failed while deleting the locally created file")
        sys.exit(1)

    # download the file using Azcopy
    cmd = util.Command("copy").add_arguments(fileUrl).add_arguments(util.test_directory_path). \
        add_flags("log-level", "Info")
    util.process_oauth_command(
        cmd,
        "BlobFSLocal" if explictFromTo else "",
        forceOAuthLogin,
        tenantID,
        aadEndpoint)
    if forceOAuthLogin:
        result = cmd.execute_azcopy_command_interactive()
    else:
        result = cmd.execute_azcopy_copy_command()
    if not result:
        print("test_blobfs_download_1Kb_file failed while downloading the 1kb file")
        sys.exit(1)
    # validate the downloaded file
    result = util.Command("testBlobFS").add_arguments(file_path).add_arguments(fileUrl).execute_azcopy_verify()
    if not result:
        print("test_blobfs_download_1Kb_file failed while validating the 1kb file download")
        sys.exit(1)
    print("test_blobfs_download_1Kb_file successfully passed")

def test_blobfs_download_64MB_file(
    explictFromTo=False,
    forceOAuthLogin=False,
    tenantID="",
    aadEndpoint=""):
    # create test file of size 64MB
    filename = "test_blob_d_64MB_file.txt"
    file_path = util.create_test_file(filename, 64*1024*1024)
    # Upload the file using Azcopy
    cmd = util.Command("copy").add_arguments(file_path).add_arguments(util.test_bfs_account_url). \
        add_flags("log-level", "Info")
    util.process_oauth_command(
        cmd,
        "LocalBlobFS" if explictFromTo else "",
        forceOAuthLogin,
        tenantID,
        aadEndpoint)
    if forceOAuthLogin:
        result = cmd.execute_azcopy_command_interactive()
    else:
        result = cmd.execute_azcopy_copy_command()
    if not result:
        print("test_blobfs_download_64MB_file failed uploading the 64MB file ", filename, " to the filesystem")
        sys.exit(1)
    # Validate the file uploaded
    fileUrl = util.test_bfs_account_url + filename
    result = util.Command("testBlobFS").add_arguments(file_path).add_arguments(fileUrl).execute_azcopy_verify()
    if not result:
        print("test_blobfs_download_64MB_file failed while validating the 64MB file upload")
        sys.exit(1)

    # delete the file locally
    try:
        os.remove(file_path)
    except:
        print("test_blobfs_download_64MB_file failed while deleting the file locally")
        sys.exit(1)

    # download the file using azcopy
    cmd = util.Command("copy").add_arguments(fileUrl).add_arguments(util.test_directory_path). \
        add_flags("log-level", "Info")
    util.process_oauth_command(
        cmd,
        "BlobFSLocal" if explictFromTo else "",
        forceOAuthLogin,
        tenantID,
        aadEndpoint)
    if forceOAuthLogin:
        result = cmd.execute_azcopy_command_interactive()
    else:
        result = cmd.execute_azcopy_copy_command()
    if not result:
        print("test_blobfs_download_64MB_file failed downloading the 64MB file ", filename)
        sys.exit(1)

    # validate the downloaded file
    result = util.Command("testBlobFS").add_arguments(file_path).add_arguments(fileUrl).execute_azcopy_verify()
    if not result:
        print("test_blobfs_download_64MB_file failed while validating the 64MB downloaded file")
        sys.exit(1)
    print("test_blobfs_download_64MB_file successfully passed")

def test_blobfs_download_100_1Kb_file(
    explictFromTo=False,
    forceOAuthLogin=False,
    tenantID="",
    aadEndpoint=""):
    # create dir with 100 1KB files inside it
    dir_name = "dir_blobfs_d_100_1K"
    dir_n_file_path = util.create_test_n_files(1024, 100, dir_name)

    # Upload the directory with 100 files inside it
    cmd = util.Command("copy").add_arguments(dir_n_file_path).add_arguments(util.test_bfs_account_url). \
        add_flags("log-level", "Info").add_flags("recursive","true")
    util.process_oauth_command(
        cmd,
        "LocalBlobFS" if explictFromTo else "",
        forceOAuthLogin,
        tenantID,
        aadEndpoint)
    if forceOAuthLogin:
        result = cmd.execute_azcopy_command_interactive()
    else:
        result = cmd.execute_azcopy_copy_command()
    if not result:
        print("test_blobfs_download_100_1Kb_file failed uploading the dir ", dir_name, " to the filesystem")
        sys.exit(1)

    # Validate the uploaded directory
    dirUrl = util.test_bfs_account_url + dir_name
    result = util.Command("testBlobFS").add_arguments(dir_n_file_path).add_arguments(dirUrl). \
        add_flags("is-object-dir", "true").execute_azcopy_verify()
    if not result:
        print("test_blobfs_download_100_1Kb_file failed while validating the uploaded directory")
        sys.exit(1)

    # delete the local directory created
    try:
        shutil.rmtree(dir_n_file_path)
    except:
        print("test_blobfs_download_100_1Kb_file failed while deleting the local directory")
        sys.exit(1)

    # download the directory
    cmd = util.Command("copy").add_arguments(dirUrl).add_arguments(util.test_directory_path).\
        add_flags("log-level", "Info").add_flags("recursive", "true")
    util.process_oauth_command(
        cmd,
        "BlobFSLocal" if explictFromTo else "",
        forceOAuthLogin,
        tenantID,
        aadEndpoint)
    if forceOAuthLogin:
        result = cmd.execute_azcopy_command_interactive()
    else:
        result = cmd.execute_azcopy_copy_command()
    if not result:
        print("test_blobfs_download_100_1Kb_file failed while downloading the directory")
        sys.exit(1)

    # validate the downloaded directory
    result = util.Command("testBlobFS").add_arguments(dir_n_file_path).add_arguments(dirUrl).\
            add_flags("is-object-dir", "true").execute_azcopy_verify()
    if not result:
        print("test_blobfs_download_100_1Kb_file failed while validating the downloaded directory ")
        sys.exit(1)
    print("test_blobfs_download_100_1Kb_file successfully passed")

def test_blobfs_download_2000_1Kb_file():
    # create dir with 100 1KB files inside it
    dir_name = "dir_blobfs_2000_1K"
    dir_n_file_path = util.create_test_n_files(1024, 2000, dir_name)

    # Upload the directory with 2000 files inside it
    result = util.Command("copy").add_arguments(dir_n_file_path).add_arguments(util.test_bfs_account_url). \
        add_flags("log-level", "Info").add_flags("recursive","true").execute_azcopy_copy_command()
    if not result:
        print("test_blobfs_upload_2000_1Kb_file failed uploading the dir ", dir_name, " to the filesystem")
        sys.exit(1)

    # Validate the uploaded directory
    dirUrl = util.test_bfs_account_url + dir_name
    result = util.Command("testBlobFS").add_arguments(dir_n_file_path).add_arguments(dirUrl). \
        add_flags("is-object-dir", "true").execute_azcopy_verify()
    if not result:
        print("test_blobfs_upload_2000_1Kb_file failed while validating the uploaded directory")
        sys.exit(1)
    print("test_blobfs_upload_2000_1Kb_file successfully passed")