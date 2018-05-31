from test_blob_download import *
from test_upload_block_blob import *
from test_upload_page_blob import *
from test_file_download import *
from test_file_upload import *
from test_azcopy_operations import *
import glob, os
import configparser
import platform

def execute_user_scenario_blob_1() :
    test_1kb_blob_upload()
    test_63mb_blob_upload()
    test_n_1kb_blob_upload(5)
    test_1GB_blob_upload()
    test_blob_metaData_content_encoding_content_type()
    test_block_size(4 * 1024 * 1024)
    test_guess_mime_type()
    test_download_1kb_blob()
    test_blob_download_preserve_last_modified_time()
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

def execute_user_scenario_azcopy_op():
    test_remove_virtual_directory()
    test_set_block_blob_tier()
    test_set_page_blob_tier()
    test_force_flag_set_to_false_upload()
    test_force_flag_set_to_false_download()

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
    
def execute_user_scenario_2():
    test_blob_download_with_special_characters()

def init():
    # initializing config parser to read the testsuite_config file.
    config = configparser.RawConfigParser()
    files_read = config.read('../test_suite_config.ini')
    if len(files_read) != 1:
        raise "Failed to find/open test_suite_config.ini file"

    # get the platform type since config file has property for platform respectively.
    osType = platform.system()
    osType = osType.upper()

    # check if the config are defined for current os type.
    platform_list =  config.sections()
    try:
        platform_list.index(osType)
    except:
        raise "not able to find the config defined for ostype " + osType

    # test_dir_path is the location where test_data folder will be created and test files will be created further.
    test_dir_path = config[osType]['TEST_DIRECTORY_PATH']

    # azcopy_exec_location is the location of the azcopy executable
    # azcopy executable will be copied to test data folder.
    azcopy_exec_location = config[osType]['AZCOPY_EXECUTABLE_PATH']

    # test_suite_exec_location is the location of the test suite executable
    # test suite executable will be copied to test data folder.
    test_suite_exec_location = config[osType]['TEST_SUITE_EXECUTABLE_LOCATION']

    # container_sas is the shared access signature of the container where test data will be uploaded to and downloaded from.
    container_sas = config['CREDENTIALS']['CONTAINER_SAS_URL']

    # share_sas_url is the URL with SAS of the share where test data will be uploaded to and downloaded from.
    share_sas_url = config['CREDENTIALS']['SHARE_SAS_URL']

    # container sas of the premium storage account.
    premium_container_sas = config['CREDENTIALS']['PREMIUM_CONTAINER_SAS_URL']

    # deleting the log files.
    for f in glob.glob('*.log'):
        try:
            os.remove(f)
        except OSError as e:
            if e.errno != errno.ENOENT: # errno.ENOENT = no such file or directory
                raise

    if not util.initialize_test_suite(test_dir_path, container_sas, share_sas_url, premium_container_sas, azcopy_exec_location, test_suite_exec_location):
        print("failed to initialize the test suite with given user input")
        return
    else:
        test_dir_path += "\\test_data"

def cleanup():
    # deleting the log files.
    for f in glob.glob('*.log'):
        try:
            os.remove(f)
        except OSError as e:
            if e.errno != errno.ENOENT: # errno.ENOENT = no such file or directory
                raise

def main():
    init()
    execute_user_scenario_azcopy_op()
    execute_user_scenario_blob_1()
    #execute_user_scenario_2()
    #execute_user_scenario_file_1()
    #temp_adhoc_scenario()
    cleanup()

main()
