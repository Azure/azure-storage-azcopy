from scripts.test_upload_block_blob import *
from scripts.utility import *
from scripts.test_azcopy_operations import *
from scripts.test_upload_page_blob import *

def execute_user_scenario_1() :
    #test_1kb_blob_upload()
    #test_63mb_blob_upload()
    #test_n_1kb_blob_upload(10)
    #test_1G_blob_upload()
    #test_metaData_content_encoding_content_type()
    #test_block_size(4 * 1024 * 1024)
    #test_1G_blob_upload()
    #test_cancel_job()
    #test_pause_resume_job_200Mb_file()
    #test_pause_resume_job_20Mb_file()
    #test_page_blob_upload_1mb()
    #test_page_range_for_sparse_file()
    test_page_blob_upload_100mb()
    

def init():
    # test_dir = input("please enter the location directory where you want to execute the test \n")
    # container_sas = input ("please enter the container shared access signature where you want to perform the test \n")
    # azcopy_exec_location = input ("please enter the location of azcopy v2 executable location \n")
    # test_suite_exec_location = input ("please enter the location of test suite executable location \n")

    # test_dir_path is the location where test_data folder will be created and test files will be created further.
    test_dir_path = "C:\\Users\\prjain\\Documents\\Sample_Files"

    # container_sas is the shared access signature of the container where test data will be uploaded to and downloaded from.
    container_sas = "https://azcopynextgendev1.blob.core.windows.net/test-container-1?st=2018-03-18T18%3A11%3A00Z&se=2018-03-30T18%3A11%3A00Z&sp=rwdl&sv=2017-04-17&sr=c&sig=6zF5e2WZZqeI1Nmgo9hQRuYrLbxsD1uWq2%2BFPtn1eGM%3D"

    # azcopy_exec_location is the location of the azcopy executable
    # azcopy executable will be copied to test data folder.
    azcopy_exec_location = "C:\\Go\\externals\\src\\github.com\\Azure\\azure-storage-azcopy\\azs.exe"

    # test_suite_exec_location is the location of the test suite executable
    # test suite executable will be copied to test data folder.
    test_suite_exec_location = "C:\\Go\\externals\\src\\github.com\\Azure\\azure-storage-azcopy\\testSuite\\testSuite.exe"

    if not initialize_test_suite(test_dir_path, container_sas, azcopy_exec_location, test_suite_exec_location):
        print("failed to initialize the test suite with given user input")
        return
    else:
        test_dir_path += "\\test_data"

    # call the scenario's
    execute_user_scenario_1()

def main():
    init()

main()
    

