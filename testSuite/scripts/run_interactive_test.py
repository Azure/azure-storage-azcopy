from test_interactive_blob_download_oauth import *
from test_interactive_blobfs_upload_oauth import *
from test_interactive_blobfs_download_oauth import *
from test_interactive_operation_oauth import *
import glob, os
import configparser
import platform
import utility as util

def execute_interactively_copy_blob_oauth_session_scenario():
    #login to get session
    test_login_with_default()
    #execute copy commands
    test_download_1kb_blob_oauth()
    test_recursive_download_blob_oauth()
    #logout
    test_logout()

def execute_interactively_copy_bfs_oauth_session_scenario():
    #login to get session
    test_login(util.test_oauth_tenant_id, util.test_oauth_aad_endpoint)
    #execute copy commands
    test_blobfs_upload_1Kb_file(True)
    test_blobfs_upload_64MB_file(True)
    test_blobfs_upload_100_1Kb_file(True)
    test_blobfs_download_1Kb_file(True)
    test_blobfs_download_64MB_file(True)
    test_blobfs_download_100_1Kb_file(True)
    #logout
    test_logout()

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

    # container whose storage account has been configured properly for the interactive testing user.
    os.environ['CONTAINER_OAUTH_URL'] = config['CREDENTIALS']['CONTAINER_OAUTH_URL']

    # container which should be same to CONTAINER_OAUTH_URL, while with SAS for validation purpose.
    os.environ['CONTAINER_OAUTH_VALIDATE_SAS_URL'] = config['CREDENTIALS']['CONTAINER_OAUTH_VALIDATE_SAS_URL']

    # set the account name for blob fs service operation
    os.environ['ACCOUNT_NAME'] = config['CREDENTIALS']['ACCOUNT_NAME']

    # set the account key for blob fs service operation
    os.environ['ACCOUNT_KEY'] = config['CREDENTIALS']['ACCOUNT_KEY']

    # set the filesystem url in the environment
    os.environ['FILESYSTEM_URL'] = config['CREDENTIALS']['FILESYSTEM_URL']

    # set oauth tenant ID
    os.environ['OAUTH_TENANT_ID'] = config['CREDENTIALS']['OAUTH_TENANT_ID']

    # set oauth aad endpoint
    os.environ['OAUTH_AAD_ENDPOINT'] = config['CREDENTIALS']['OAUTH_AAD_ENDPOINT']

def check_env_not_exist(key):
    if os.environ.get(key, '-1') == '-1':
        print('Environment variable: ' + key + ' not set.')
        return True
    return False

def init():
    # Check the environment variables.
    # If they are not set, then parse the config file and set
    # environment variables. If any of the env variable is not set
    # test_config_file is parsed and env variables are reset.
    if check_env_not_exist('TEST_DIRECTORY_PATH') or check_env_not_exist('AZCOPY_EXECUTABLE_PATH') or \
            check_env_not_exist('TEST_SUITE_EXECUTABLE_LOCATION') or check_env_not_exist('CONTAINER_SAS_URL') or \
            check_env_not_exist('CONTAINER_OAUTH_URL') or check_env_not_exist('CONTAINER_OAUTH_VALIDATE_SAS_URL') or \
            check_env_not_exist('SHARE_SAS_URL') or check_env_not_exist('PREMIUM_CONTAINER_SAS_URL') or \
            check_env_not_exist('FILESYSTEM_URL') or check_env_not_exist('ACCOUNT_NAME') or \
            check_env_not_exist('ACCOUNT_KEY'):
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

    # container_oauth is container for oauth testing.
    container_oauth = os.environ.get('CONTAINER_OAUTH_URL')

    # container_oauth_validate is the URL with SAS for oauth validation.
    container_oauth_validate = os.environ.get('CONTAINER_OAUTH_VALIDATE_SAS_URL')

    # get the filesystem url
    filesystem_url = os.environ.get('FILESYSTEM_URL')

    # oauth tenant ID
    oauth_tenant_id = os.environ.get('OAUTH_TENANT_ID')

    # oauth aad encpoint
    oauth_aad_endpoint = os.environ.get('OAUTH_AAD_ENDPOINT')

    # deleting the log files.
    cleanup()

    if not util.initialize_interactive_test_suite(test_dir_path, container_oauth, container_oauth_validate, 
        filesystem_url, oauth_tenant_id, oauth_aad_endpoint, azcopy_exec_location, test_suite_exec_location):
        print("failed to initialize the interactive test suite with given user input")
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
    execute_interactively_copy_blob_oauth_session_scenario()
    execute_interactively_copy_bfs_oauth_session_scenario()
    cleanup()


if __name__ == '__main__':
    main()
