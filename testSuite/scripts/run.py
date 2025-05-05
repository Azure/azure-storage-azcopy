from test_blob_download import *
from test_upload_block_blob import *
from test_upload_page_blob import *
from test_blobfs_upload_sharedkey import *
from test_blobfs_download_sharedkey import *
from test_blob_piping import *
from test_blob_sync import *
from test_service_to_service_copy import *
from test_google_cloud_storage_copy import *
from test_hash_download import *
from test_hash_upload import *
import glob, os
import configparser
import platform
import sys
import unittest
import re


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

    # container whose storage account has been configured properly for the interactive testing user.
    os.environ['CONTAINER_OAUTH_URL'] = config['CREDENTIALS']['CONTAINER_OAUTH_URL']

    # container which should be same to CONTAINER_OAUTH_URL, while with SAS for validation purpose.
    os.environ['CONTAINER_OAUTH_VALIDATE_SAS_URL'] = config['CREDENTIALS']['CONTAINER_OAUTH_VALIDATE_SAS_URL']

    # share_sas_url is the URL with SAS of the share where test data will be uploaded to and downloaded from.
    os.environ['SHARE_SAS_URL'] = config['CREDENTIALS']['SHARE_SAS_URL']

    # container sas of the premium storage account.
    os.environ['PREMIUM_CONTAINER_SAS_URL'] = config['CREDENTIALS']['PREMIUM_CONTAINER_SAS_URL']

    # set the account name for blob fs service operation
    os.environ['ACCOUNT_NAME'] = config['CREDENTIALS']['ACCOUNT_NAME']

    # set the account key for blob fs service operation
    os.environ['ACCOUNT_KEY'] = config['CREDENTIALS']['ACCOUNT_KEY']

    # set the filesystem url in the environment
    os.environ['FILESYSTEM_URL'] = config['CREDENTIALS']['FILESYSTEM_URL']
    os.environ['FILESYSTEM_SAS_URL'] = config['CREDENTIALS']['FILESYSTEM_SAS_URL']

    # set env var for service-2-service copy source blob account
    os.environ['S2S_SRC_BLOB_ACCOUNT_SAS_URL'] = config['CREDENTIALS']['S2S_SRC_BLOB_ACCOUNT_SAS_URL']

    # set env var for service-2-service copy destination blob account
    os.environ['S2S_DST_BLOB_ACCOUNT_SAS_URL'] = config['CREDENTIALS']['S2S_DST_BLOB_ACCOUNT_SAS_URL']

    # set env var for service-2-service copy source file account
    os.environ['S2S_SRC_FILE_ACCOUNT_SAS_URL'] = config['CREDENTIALS']['S2S_SRC_FILE_ACCOUNT_SAS_URL']

    # set env var for service-2-service copy source s3 and gcp
    os.environ['S2S_SRC_S3_SERVICE_URL'] = config['CREDENTIALS']['S2S_SRC_S3_SERVICE_URL']
    os.environ['S2S_SRC_GCP_SERVICE_URL'] = config['CREDENTIALS']['S2S_SRC_GCP_SERVICE_URL']
    os.environ['AWS_ACCESS_KEY_ID'] = config['CREDENTIALS']['AWS_ACCESS_KEY_ID']
    os.environ['AWS_SECRET_ACCESS_KEY'] = config['CREDENTIALS']['AWS_SECRET_ACCESS_KEY']
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = config['CREDENTIALS']['GOOGLE_APPLICATION_CREDENTIALS']
    os.environ['GOOGLE_CLOUD_PROJECT'] = config['CREDENTIALS']['GOOGLE_CLOUD_PROJECT']

    os.environ['OAUTH_AAD_ENDPOINT'] = config['CREDENTIALS']['OAUTH_AAD_ENDPOINT']
    os.environ['S3_TESTS_OFF'] = config['CREDENTIALS']['S3_TESTS_OFF']
    os.environ['GCP_TESTS_OFF'] = config['CREDENTIALS']['GCP_TESTS_OFF']

    # set env var for tamper-proof endpoint
    os.environ['TAMPER_PROOF_ENDPOINT'] = config['CREDENTIALS']['TAMPER_PROOF_ENDPOINT']

def check_env_not_exist(key):
    if os.environ.get(key, '-1') == '-1':
        print('Environment variable: ' + key + ' not set.')
        return True
    return False

def get_env_logged(key):
    value = os.environ.get(key)
    if value is None:
        print(key + " = None")
    else:
        print(key + " = " + re.sub("(?i)(?P<key>sig[ \t]*[:=][ \t]*)(?P<value>[^& ,;\t\n\r]+)", "sig=REDACTED", value))
    return value

def init():
    # Check the environment variables.
    # If they are not set, then parse the config file and set
    # environment variables. If any of the env variable is not set
    # test_config_file is parsed and env variables are reset.
    if check_env_not_exist('TEST_DIRECTORY_PATH') or check_env_not_exist('AZCOPY_EXECUTABLE_PATH') or \
            check_env_not_exist('TEST_SUITE_EXECUTABLE_LOCATION') or check_env_not_exist('CONTAINER_SAS_URL') or \
            check_env_not_exist('CONTAINER_OAUTH_URL') or check_env_not_exist('CONTAINER_OAUTH_VALIDATE_SAS_URL') or \
            check_env_not_exist('SHARE_SAS_URL') or check_env_not_exist('PREMIUM_CONTAINER_SAS_URL') or \
            check_env_not_exist('FILESYSTEM_URL') or check_env_not_exist('FILESYSTEM_SAS_URL') or \
            check_env_not_exist('ACCOUNT_NAME') or check_env_not_exist('ACCOUNT_KEY') or \
            check_env_not_exist('S2S_SRC_BLOB_ACCOUNT_SAS_URL') or check_env_not_exist('S2S_DST_BLOB_ACCOUNT_SAS_URL') \
            or check_env_not_exist('S2S_SRC_FILE_ACCOUNT_SAS_URL') or check_env_not_exist('S2S_SRC_S3_SERVICE_URL') or \
            check_env_not_exist('S2S_SRC_GCP_SERVICE_URL') or check_env_not_exist('TAMPER_PROOF_ENDPOINT'):
        parse_config_file_set_env()

    # Get the environment variables value
    # test_dir_path is the location where test_data folder will be created and test files will be created further.
    test_dir_path = get_env_logged('TEST_DIRECTORY_PATH')

    # azcopy_exec_location is the location of the azcopy executable
    # azcopy executable will be copied to test data folder.
    azcopy_exec_location = get_env_logged('AZCOPY_EXECUTABLE_PATH')

    # test_suite_exec_location is the location of the test suite executable
    # test suite executable will be copied to test data folder.
    test_suite_exec_location = get_env_logged('TEST_SUITE_EXECUTABLE_LOCATION')

    # container_sas is the shared access signature of the container
    # where test data will be uploaded to and downloaded from.
    container_sas = get_env_logged('CONTAINER_SAS_URL')

    # container_oauth is container for oauth testing.
    container_oauth = get_env_logged('CONTAINER_OAUTH_URL')

    # container_oauth_validate is the URL with SAS for oauth validation.
    container_oauth_validate = get_env_logged('CONTAINER_OAUTH_VALIDATE_SAS_URL')

    # share_sas_url is the URL with SAS of the share where test data will be uploaded to and downloaded from.
    share_sas_url = get_env_logged('SHARE_SAS_URL')

    # container sas of the premium storage account.
    premium_container_sas = get_env_logged('PREMIUM_CONTAINER_SAS_URL')

    # get the filesystem url
    filesystem_url = get_env_logged('FILESYSTEM_URL')
    filesystem_sas_url = get_env_logged('FILESYSTEM_SAS_URL')

    # get the s2s copy src URLs
    s2s_src_blob_account_url = get_env_logged('S2S_SRC_BLOB_ACCOUNT_SAS_URL')
    s2s_src_file_account_url = get_env_logged('S2S_SRC_FILE_ACCOUNT_SAS_URL')
    s2s_src_s3_service_url = get_env_logged('S2S_SRC_S3_SERVICE_URL')
    s2s_src_gcp_service_url = get_env_logged('S2S_SRC_GCP_SERVICE_URL')

    # get the s2s copy dest account URLs
    s2s_dst_blob_account_url = get_env_logged('S2S_DST_BLOB_ACCOUNT_SAS_URL')

    tamper_proof_endpoint = get_env_logged('TAMPER_PROOF_ENDPOINT')

    get_env_logged("ACCOUNT_NAME")
    # do NOT log ACCOUNT_KEY

    # don't log, it will just get redacted by DevOps logging system: get_env_logged("AWS_ACCESS_KEY_ID")
    # do NOT log AWS_SECRET_ACCESS_KEY

    get_env_logged("OAUTH_AAD_ENDPOINT")
    # don't log, it will just get redacted by DevOps logging system: get_env_logged("OAUTH_TENANT_ID")

    get_env_logged("S3_TESTS_OFF")

    get_env_logged("GCP_TESTS_OFF")


    # deleting the log files.
    cleanup()

    if not util.initialize_test_suite(test_dir_path, container_sas, container_oauth, container_oauth_validate, share_sas_url, premium_container_sas,
                                      filesystem_url, filesystem_sas_url, s2s_src_blob_account_url, s2s_src_file_account_url, s2s_src_s3_service_url, s2s_src_gcp_service_url, s2s_dst_blob_account_url, tamper_proof_endpoint, azcopy_exec_location, test_suite_exec_location):
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
    print("Smoke tests starting...")
    init()

    test_class_to_run = [
        # BlobPipingTests,
        #                  Blob_Sync_User_Scenario,
        #                  Block_Upload_User_Scenarios,
        #                  Blob_Download_User_Scenario,
        #                  PageBlob_Upload_User_Scenarios,
        #                  BlobFs_Upload_ShareKey_User_Scenarios,
        #                  BlobFs_Download_SharedKey_User_Scenarios,
        #                  Service_2_Service_Copy_User_Scenario,
        #                  Google_Cloud_Storage_Copy_User_Scenario,
                         Hash_Upload_User_Scenarios,
                         Hash_Download_User_Scenarios]
    suites_list = []

    loader = unittest.TestLoader()
    for test_class in test_class_to_run:
        suite = loader.loadTestsFromTestCase(test_class)
        suites_list.append(suite)
    big_suite = unittest.TestSuite(suites_list)
    runner = unittest.TextTestRunner(verbosity=2)
    results = runner.run(big_suite)
    if results.wasSuccessful() is False:
        sys.exit(1)

    # suite = unittest.TestLoader().loadTestsFromTestCase(Block_Upload_User_Scenarios)
    # unittest.TextTestRunner(verbosity=2).run(suite)
    #
    # suite = unittest.TestLoader().loadTestsFromTestCase(Blob_Download_User_Scenario)
    # unittest.TextTestRunner(verbosity=2).run(suite)
    #
    # suite = unittest.TestLoader().loadTestsFromTestCase(PageBlob_Upload_User_Scenarios)
    # unittest.TextTestRunner(verbosity=2).run(suite)
    #
    # suite = unittest.TestLoader().loadTestsFromTestCase(BlobFs_Upload_OAuth_User_Scenarios)
    # unittest.TextTestRunner(verbosity=2).run(suite)
    #
    # suite = unittest.TestLoader().loadTestsFromTestCase(BlobFs_Download_OAuth_User_Scenarios)
    # unittest.TextTestRunner(verbosity=2).run(suite)
    #
    # suite = unittest.TestLoader().loadTestsFromTestCase(Azcopy_Operation_User_Scenario)
    # unittest.TextTestRunner(verbosity=2).run(suite)
    #
    # suite = unittest.TestLoader().loadTestsFromTestCase(FileShare_Download_User_Scenario)
    # unittest.TextTestRunner(verbosity=2).run(suite)
    #
    # suite = unittest.TestLoader().loadTestsFromTestCase(FileShare_Upload_User_Scenario)
    # unittest.TextTestRunner(verbosity=2).run(suite)
    #
    # suite = unittest.TestLoader().loadTestsFromTestCase(BlobFs_Upload_ShareKey_User_Scenarios)
    # unittest.TextTestRunner(verbosity=2).run(suite)
    #
    # suite = unittest.TestLoader().loadTestsFromTestCase(BlobFs_Download_SharedKey_User_Scenarios)
    # unittest.TextTestRunner(verbosity=2).run(suite)
    #
    # suite = unittest.TestLoader().loadTestsFromTestCase(Service_2_Service_Copy_User_Scenario)
    # unittest.TextTestRunner(verbosity=2).run(suite)

    cleanup()

if __name__ == '__main__':
    main()
