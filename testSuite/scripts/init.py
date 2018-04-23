from test_blob_download import *
from test_upload_block_blob import *
from test_upload_page_blob import *
from test_file_download import *
from test_file_upload import *
import sys

def execute_user_scenario_1() :
    test_1kb_blob_upload()
    test_63mb_blob_upload()
    test_n_1kb_blob_upload(5)
    test_1GB_blob_upload()
    test_metaData_content_encoding_content_type()
    test_block_size(4 * 1024 * 1024)
    test_guess_mime_type()
    test_download_1kb_blob()
    test_download_perserve_last_modified_time()
    test_blob_download_63mb_in_4mb()
    test_recursive_download_blob()
    # test_cancel_job()
    # test_blob_download_63mb_in_4mb()
    # #test_pause_resume_job_200Mb_file()
    # #test_pause_resume_job_95Mb_file()
    test_page_blob_upload_1mb()
    test_page_range_for_complete_sparse_file()
    test_page_blob_upload_partial_sparse_file()

def temp_adhoc_scenario() :
    #test_3_1kb_file_in_dir_upload_download_azure_directory_recursive()
    #test_8_1kb_file_in_dir_upload_download_azure_directory_non_recursive()
    test_3_1kb_file_in_dir_upload_download_azure_directory_recursive()
    #test_upload_download_1kb_file_wildcard_several_files()

def execute_user_scenario_file_1() :
    ###
    # download
    ###
    # single context
    test_upload_download_1kb_file_fullname()

    # wildcard context
    # Using /*, which actually upload/download everything in a directory
    test_upload_download_1kb_file_wildcard_all_files()

    # Using /pattern*, which actually upload/download matched files in specific directory
    test_upload_download_1kb_file_wildcard_several_files()

    # directory context
    test_6_1kb_file_in_dir_upload_download_share()
    test_3_1kb_file_in_dir_upload_download_azure_directory_recursive()
    #test_8_1kb_file_in_dir_upload_download_azure_directory_non_recursive()

    # modified time
    test_download_perserve_last_modified_time()

    # different sizes
    test_file_download_63mb_in_4mb()

    # directory context
    #test_recursive_download_file()

    ###
    # upload
    ###
    # single context
    test_file_upload_1mb_fullname()

    # wildcard context
    #test_file_upload_1mb_wildcard()

    # single sparse file and range
    test_file_range_for_complete_sparse_file()
    test_file_upload_partial_sparse_file()
    
    # directory context
    #test_6_1kb_file_in_dir_upload_to_share()
    #test_3_1kb_file_in_dir_upload_to_azure_directory_recursive()
    #test_8_1kb_file_in_dir_upload_to_azure_directory_non_recursive()

    # metadata and mime-type
    test_metaData_content_encoding_content_type()
    test_guess_mime_type()

    # different sizes
    test_9mb_file_upload()
    test_1GB_file_upload()
    


# todo one config file with creds for each os.
def init():
    # test_dir = input("please enter the location directory where you want to execute the test \n")
    # container_sas = input ("please enter the container shared access signature where you want to perform the test \n")
    # azcopy_exec_location = input ("please enter the location of azcopy v2 executable location \n")
    # test_suite_exec_location = input ("please enter the location of test suite executable location \n")

    # test_dir_path is the location where test_data folder will be created and test files will be created further.
    test_dir_path = "C:\\Users\\jiacfan\\testdir"

    # container_sas is the shared access signature of the container where test data will be uploaded to and downloaded from.
    container_sas = "https://jiacteststgje001.blob.core.windows.net/atestcontainer01?sv=2017-07-29&ss=bfqt&srt=sco&sp=rwdlacup&se=2018-04-29T11:07:05Z&st=2018-04-21T03:07:05Z&spr=https,http&sig=NchRx8q%2FnFsoS8M0wjRN0GRIRrW5RqYgqb9Q0at6tm0%3D"

    # share_sas_url is the URL with SAS of the share where test data will be uploaded to and downloaded from.
    # TODO: always remove useful info
    share_sas_url = "https://jiacteststgje001.file.core.windows.net/atestshare01?sv=2017-07-29&ss=bfqt&srt=sco&sp=rwdlacup&se=2018-04-29T11:07:05Z&st=2018-04-21T03:07:05Z&spr=https,http&sig=NchRx8q%2FnFsoS8M0wjRN0GRIRrW5RqYgqb9Q0at6tm0%3D"

    # azcopy_exec_location is the location of the azcopy executable
    # azcopy executable will be copied to test data folder.
    azcopy_exec_location = "C:\\Users\\jiacfan\\go\\src\\github.com\\Azure\\azure-storage-azcopy\\azs.exe"

    # test_suite_exec_location is the location of the test suite executable
    # test suite executable will be copied to test data folder.
    test_suite_exec_location = "C:\\Users\\jiacfan\\go\\src\\github.com\\Azure\\azure-storage-azcopy\\testSuite\\testSuite.exe"

    if not util.initialize_test_suite(test_dir_path, container_sas, share_sas_url, azcopy_exec_location, test_suite_exec_location):
        print("failed to initialize the test suite with given user input")
        return
    else:
        test_dir_path += "\\test_data"


def main():
    init()
    #execute_user_scenario_1()
    execute_user_scenario_file_1()
    #temp_adhoc_scenario()

main()
    

