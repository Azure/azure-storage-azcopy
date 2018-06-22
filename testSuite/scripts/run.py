from test_blob_download import *
from test_upload_block_blob import *
from test_upload_page_blob import *
from test_file_download import *
from test_file_upload import *
from test_azcopy_operations import *
from test_blobfs_upload import *
from test_blobfs_download import *
import glob, os
import configparser
import platform


def execute_user_scenario_blob_1():
    test_0KB_blob_upload()
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

def execute_user_scenario_wildcards_op():
    test_remove_files_with_Wildcard()

def execute_bfs_user_scenario():
    test_blobfs_upload_1Kb_file()
    test_blobfs_upload_64MB_file()
    test_blobfs_upload_100_1Kb_file()
    test_blobfs_download_1Kb_file()
    test_blobfs_download_64MB_file()
    test_blobfs_download_100_1Kb_file()

def execute_sync_user_scenario():
    test_sync_local_to_blob_without_wildCards()
    test_sync_local_to_blob_with_wildCards()
    test_sync_blob_download_with_wildcards()
    test_sync_blob_download_without_wildcards()


def execute_user_scenario_azcopy_op():
    test_download_blob_exclude_flag()
    test_download_blob_include_flag()
    test_upload_block_blob_include_flag()
    test_upload_block_blob_exclude_flag()
    test_remove_virtual_directory()
    test_set_block_blob_tier()
    test_set_page_blob_tier()
    test_force_flag_set_to_false_upload()
    test_force_flag_set_to_false_download()


def execute_user_scenario_file_1():
    #########################
    # download
    #########################
    # single file download scenario
    test_upload_download_1kb_file_fullname()
    # single file download with wildcard scenarios
    # Using /*, which actually upload/download everything in a directory
    test_upload_download_1kb_file_wildcard_all_files()
    # Using /pattern*, which actually upload/download matched files in specific directory
    test_upload_download_1kb_file_wildcard_several_files()
    # directory download scenario
    test_6_1kb_file_in_dir_upload_download_share()
    test_3_1kb_file_in_dir_upload_download_azure_directory_recursive()
    # test_8_1kb_file_in_dir_upload_download_azure_directory_non_recursive()
    # modified time
    test_download_perserve_last_modified_time()
    # different sizes
    test_file_download_63mb_in_4mb()
    # directory transfer scenarios
    # test_recursive_download_file()

    #########################
    # upload
    #########################
    # single file upload scenario
    test_file_upload_1mb_fullname()
    # wildcard scenario, already coverred in download scenario, as file would be uploaded first
    # test_file_upload_1mb_wildcard()
    # single sparse file and range
    test_file_range_for_complete_sparse_file()
    test_file_upload_partial_sparse_file()
    # directory transfer scenarios, already covered during download
    # test_6_1kb_file_in_dir_upload_to_share()
    # test_3_1kb_file_in_dir_upload_to_azure_directory_recursive()
    # test_8_1kb_file_in_dir_upload_to_azure_directory_non_recursive()
    # metadata and mime-type
    test_metaData_content_encoding_content_type()
    test_guess_mime_type()
    # different sizes
    test_9mb_file_upload()
    test_1GB_file_upload()


def execute_user_scenario_2():
    test_blob_download_with_special_characters()


def parse_config_file_set_env():
    config = configparser.RawConfigParser()
    files_read = config.read('../test_suite_config.ini')
    if len(files_read) != 1:
        raise Exception("Failed to find/open test_suite_config.ini file")

    # get the platform type since config file has property for platform respectively.
    os_type = platform.system()
    os_type = os_type.upper()

    # check if the config are defined for current os type.
    platform_list = config.sections()
    try:
        platform_list.index(os_type)
    except:
        raise Exception("not able to find the config defined for ostype " + os_type)
    # set all the environment variables
    # TEST_DIRECTORY_PATH is the location where test_data folder will be created and test files will be created further.
    # set the environment variable TEST_DIRECTORY_PATH
    os.environ['TEST_DIRECTORY_PATH'] = config[os_type]['TEST_DIRECTORY_PATH']

    # AZCOPY_EXECUTABLE_PATH is the location of the azcopy executable
    # azcopy executable will be copied to test data folder.
    # set the environment variables
    os.environ['AZCOPY_EXECUTABLE_PATH'] = config[os_type]['AZCOPY_EXECUTABLE_PATH']

    # TEST_SUITE_EXECUTABLE_LOCATION is the location of the test suite executable
    # test suite executable will be copied to test data folder.
    # set the environment variable TEST_SUITE_EXECUTABLE_LOCATION
    os.environ['TEST_SUITE_EXECUTABLE_LOCATION'] = config[os_type]['TEST_SUITE_EXECUTABLE_LOCATION']

    # CONTAINER_SAS_URL is the shared access signature of the container
    # where test data will be uploaded to and downloaded from.
    os.environ['CONTAINER_SAS_URL'] = config['CREDENTIALS']['CONTAINER_SAS_URL']

    # share_sas_url is the URL with SAS of the share where test data will be uploaded to and downloaded from.
    os.environ['SHARE_SAS_URL'] = config['CREDENTIALS']['SHARE_SAS_URL']

    # container sas of the premium storage account.
    os.environ['PREMIUM_CONTAINER_SAS_URL'] = config['CREDENTIALS']['PREMIUM_CONTAINER_SAS_URL']

    # set the account name for blob fs service operation
    os.environ['ACCOUNT_NAME'] = config['CREDENTIALS']['BFS_ACCOUNT_NAME']

    # set the account key for blob fs service operation
    os.environ['ACCOUNT_KEY'] = config['CREDENTIALS']['BFS_ACCOUNT_KEY']

    # set the filesystem url in the environment
    os.environ['FILESYSTEM_URL'] = config['CREDENTIALS']['FILESYSTEM_URL']

def init():
    # Check the environment variables.
    # If they are not set, then parse the config file and set
    # environment variables. If any of the env variable is not set
    # test_config_file is parsed and env variables are reset.
    if os.environ.get('TEST_DIRECTORY_PATH', '-1') == '-1' or \
            os.environ.get('AZCOPY_EXECUTABLE_PATH', '-1') == '-1' or \
            os.environ.get('TEST_SUITE_EXECUTABLE_LOCATION', '-1') == '-1' or \
            os.environ.get('CONTAINER_SAS_URL', '-1') == '-1' or \
            os.environ.get('SHARE_SAS_URL', '-1') == '-1' or \
            os.environ.get('PREMIUM_CONTAINER_SAS_URL', '-1') == '-1' or \
            os.environ.get('FILESYSTEM_URL' '-1') == '-1' or \
            os.environ.get('ACCOUNT_NAME', '-1') == '-1' or \
            os.environ.get('ACCOUNT_KEY', '-1') == '-1':
        parse_config_file_set_env()

    # Get the environment variables value
    # test_dir_path is the location where test_data folder will be created and test files will be created further.
    test_dir_path = os.environ.get('TEST_DIRECTORY_PATH')

    # azcopy_exec_location is the location of the azcopy executable
    # azcopy executable will be copied to test data folder.
    azcopy_exec_location = os.environ.get('AZCOPY_EXECUTABLE_PATH')

    # test_suite_exec_location is the location of the test suite executable
    # test suite executable will be copied to test data folder.
    test_suite_exec_location = os.environ.get('TEST_SUITE_EXECUTABLE_LOCATION')

    # container_sas is the shared access signature of the container
    # where test data will be uploaded to and downloaded from.
    container_sas = os.environ.get('CONTAINER_SAS_URL')

    # share_sas_url is the URL with SAS of the share where test data will be uploaded to and downloaded from.
    share_sas_url = os.environ.get('SHARE_SAS_URL')

    # container sas of the premium storage account.
    premium_container_sas = os.environ.get('PREMIUM_CONTAINER_SAS_URL')

    # get the filesystem url
    filesystem_url = os.environ.get('FILESYSTEM_URL')

    # deleting the log files.
    cleanup()

    if not util.initialize_test_suite(test_dir_path, container_sas, share_sas_url, premium_container_sas,
                                      filesystem_url, azcopy_exec_location, test_suite_exec_location):
        print("failed to initialize the test suite with given user input")
        return
    else:
        test_dir_path += "\\test_data"


def cleanup():
    # delete the log files
    for f in glob.glob('*.log'):
        try:
            os.remove(f)
        except OSError:
            pass


def main():
    init()
    execute_bfs_user_scenario()
    execute_sync_user_scenario()
    execute_user_scenario_wildcards_op()
    execute_user_scenario_azcopy_op()
    execute_user_scenario_blob_1()
    execute_user_scenario_2()
    # execute_user_scenario_file_1()
    # temp_adhoc_scenario()
    cleanup()


if __name__ == '__main__':
    main()
