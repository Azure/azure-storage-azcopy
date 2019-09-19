import ctypes
import os
import platform
import shutil
import subprocess
import shlex
import uuid
import random
import json
from pathlib import Path
from collections import namedtuple


# Command Class is used to create azcopy commands and validator commands.
class Command(object):
    def __init__(self, command_type):
        self.command_type = command_type
        # initializing dictionary to store flags and its values.
        self.flags = dict()
        # initializing list to store arguments for azcopy and validator.
        self.args = list()

    # this api is used by command class instance to add arguments.
    def add_arguments(self, argument):
        if argument == None:
            return
        self.args.append(argument)
        return self

    def add_flags(self, flag, value):
        self.flags[flag] = value
        return self

    # returns the command by combining arguments and flags.
    def string(self):
        command = self.command_type
        if len(self.args) > 0:
            for arg in self.args:
                if (len(arg) > 0):
                    # add '"' at start and end of each argument.
                    command += " " + '"' + arg + '"'
            # iterating through all the values in dict and combining them.
        if len(self.flags) > 0:
            for key, value in self.flags.items():
                command += " --" + key + "=" + '"' + str(value) + '"'
        return command

    # this api is used to execute a azcopy copy command.
    # by default, command execute a upload command.
    # return true or false for success or failure of command.
    def execute_azcopy_copy_command(self):
        return execute_azcopy_command(self.string())

    # this api is used to execute a azcopy copy command.
    # by default, command execute a upload command.
    # return azcopy console output on successful execution.
    def execute_azcopy_copy_command_get_output(self):
        return execute_azcopy_command_get_output(self.string())

    def execute_azcopy_command_interactive(self):
        return execute_azcopy_command_interactive(self.string())

    # api execute other azcopy commands like cancel, pause, resume or list.
    def execute_azcopy_operation_get_output(self):
        return execute_azcopy_command_get_output(self.string())

    # api executes the azcopy validator to verify the azcopy operation.
    def execute_azcopy_verify(self):
        return verify_operation(self.string())

    # api executes the clean command to delete the blob/container/file/share contents.
    def execute_azcopy_clean(self):
        return verify_operation(self.string())

    # api executes the create command to create the blob/container/file/share/directory contents.
    def execute_azcopy_create(self):
        return verify_operation(self.string())

    # api executes the info command to get AzCopy binary embedded infos.
    def execute_azcopy_info(self):
        return verify_operation_get_output(self.string())

    # api executes the testSuite's upload command to upload(prepare) data to source URL.
    def execute_testsuite_upload(self):
        return verify_operation(self.string())

# processes oauth command according to swtiches
def process_oauth_command(
    cmd,
    fromTo=""):
    if fromTo!="":
        cmd.add_flags("from-to", fromTo)

# api executes the clean command on validator which deletes all the contents of the container.
def clean_test_container(container):
    # execute the clean command.
    result = Command("clean").add_arguments(container).add_flags("serviceType", "Blob").add_flags("resourceType", "Bucket").execute_azcopy_clean()
    if not result:
        print("error cleaning the container. please check the container sas provided")
        return False
    return True

def clean_test_blob_account(account):
    result = Command("clean").add_arguments(account).add_flags("serviceType", "Blob").add_flags("resourceType", "Account").execute_azcopy_clean()
    if not result:
        print("error cleaning the blob account. please check the account sas provided")
        return False
    return True

def clean_test_s3_account(account):
    result = Command("clean").add_arguments(account).add_flags("serviceType", "S3").add_flags("resourceType", "Account").execute_azcopy_clean()
    if not result:
        print("error cleaning the S3 account.")
        return False
    return True

def clean_test_file_account(account):
    result = Command("clean").add_arguments(account).add_flags("serviceType", "File").add_flags("resourceType", "Account").execute_azcopy_clean()
    if not result:
        print("error cleaning the file account. please check the account sas provided")
        return False
    return True

# api executes the clean command on validator which deletes all the contents of the container.
def clean_test_share(shareURLStr):
    # execute the clean command.
    result = Command("clean").add_arguments(shareURLStr).add_flags("serviceType", "File").add_flags("resourceType", "Bucket").execute_azcopy_clean()
    if not result:
        print("error cleaning the share. please check the share sas provided")
        return False
    return True

def clean_test_filesystem(fileSystemURLStr):
    result = Command("clean").add_arguments(fileSystemURLStr).add_flags("serviceType", "BlobFS").add_flags("resourceType", "Bucket").execute_azcopy_clean()
    if not result:
        print("error cleaning the filesystem. please check the filesystem URL, user and key provided")
        return False
    return True

# initialize_test_suite initializes the setup for executing test cases.
def initialize_test_suite(test_dir_path, container_sas, container_oauth, container_oauth_validate, share_sas_url, premium_container_sas, filesystem_url, filesystem_sas_url,
                          s2s_src_blob_account_url, s2s_src_file_account_url, s2s_src_s3_service_url, s2s_dst_blob_account_url, azcopy_exec_location, test_suite_exec_location):
    # test_directory_path is global variable holding the location of test directory to execute all the test cases.
    # contents are created, copied, uploaded and downloaded to and from this test directory only
    global test_directory_path

    # test_container_url is a global variable used in the entire testSuite holding the user given container shared access signature.
    # all files / directory are uploaded and downloaded to and from this container.
    global test_container_url

    # test_oauth_container_url is a global variable used in the entire testSuite holding the user given container for oAuth testing.
    # all files / directory are uploaded and downloaded to and from this container.
    global test_oauth_container_url

    # test_container_oauth_validate_sas_url is same container as test_oauth_container_url, while for validation purpose. 
    global test_oauth_container_validate_sas_url

    # test_premium_account_contaier_url is a global variable used in the entire test suite holding the user given container sas of premium storage account container.
    global test_premium_account_contaier_url

    # test_share_url is a global variable used in the entire testSuite holding the user given share URL with shared access signature.
    # all files / directory are uploaded and downloaded to and from this share.
    global test_share_url

    # holds the name of the azcopy executable
    global azcopy_executable_name

    # holds the name of the test suite executable
    global test_suite_executable_name

    # holds the filesystem url to perform the operations for blob fs service
    global test_bfs_account_url
    global test_bfs_sas_account_url

    # holds account for s2s copy tests
    global test_s2s_src_blob_account_url
    global test_s2s_dst_blob_account_url
    global test_s2s_src_file_account_url
    global test_s2s_src_s3_service_url

    # creating a test_directory in the location given by user.
    # this directory will be used to created and download all the test files.
    new_dir_path = os.path.join(test_dir_path, "test_data")
    # todo finally
    try:
        # removing the directory and its contents, if directory exists
        shutil.rmtree(new_dir_path)
        os.mkdir(new_dir_path)
    except:
        os.mkdir(new_dir_path)

    # copying the azcopy executable to the newly created test directory.
    # this copying is done to avoid using the executables at location which might be used by the user
    # while test suite is running.
    if os.path.isfile(azcopy_exec_location):
        shutil.copy2(azcopy_exec_location, new_dir_path)
        azcopy_executable_name = parse_out_executable_name(azcopy_exec_location)
    else:
        print("please verify the azcopy executable location")
        return False

    # copying the test executable to the newly created test directory.
    # this copying is done to avoid using the executables at location which might be used by the user
    # while test suite is running.
    if os.path.isfile(test_suite_exec_location):
        shutil.copy2(test_suite_exec_location, new_dir_path)
        test_suite_executable_name = parse_out_executable_name(test_suite_exec_location)
    else:
        print("please verify the test suite executable location")
        return False

    test_directory_path = new_dir_path

    # set the filesystem url
    test_bfs_account_url = filesystem_url
    test_bfs_sas_account_url = filesystem_sas_url
    # test_bfs_sas_account_url is the same place as test_bfs_sas_account_url in CI
    if not clean_test_filesystem(test_bfs_account_url):
        print("failed to clean test filesystem.")
    if not (test_bfs_account_url.endswith("/") and test_bfs_account_url.endwith("\\")):
        test_bfs_account_url = test_bfs_account_url + "/"

    # cleaning the test container provided
    # all blob inside the container will be deleted.
    test_container_url = container_sas
    if not clean_test_container(test_container_url):
        print("failed to clean container.")

    test_oauth_container_url = container_oauth
    if not (test_oauth_container_url.endswith("/") and test_oauth_container_url.endwith("\\")):
        test_oauth_container_url = test_oauth_container_url + "/"
    if not clean_test_container(test_oauth_container_url):
        print("failed to clean test blob container.")
    
    # No need to do cleanup on oauth validation URL.
    # Removed this cleanup step because we use a container SAS.
    # Therefore, we'd delete the container successfully with the container level SAS
    # and just not be able to re-make it with the container SAS
    test_oauth_container_validate_sas_url = container_oauth_validate
    if not clean_test_container(test_oauth_container_url):
        print("failed to clean OAuth container.")

    test_premium_account_contaier_url = premium_container_sas
    if not clean_test_container(test_premium_account_contaier_url):
        print("failed to clean premium container.")

    test_s2s_src_blob_account_url = s2s_src_blob_account_url
    if not clean_test_blob_account(test_s2s_src_blob_account_url):
        print("failed to clean s2s blob container.")

    test_s2s_src_file_account_url = s2s_src_file_account_url
    if not clean_test_file_account(test_s2s_src_file_account_url):
        print("failed to clean s2s file share.")

    test_s2s_dst_blob_account_url = s2s_dst_blob_account_url
    if not clean_test_blob_account(test_s2s_dst_blob_account_url):
        print("failed to clean s2s blob destination container.")

    test_s2s_src_s3_service_url = s2s_src_s3_service_url
    if not clean_test_s3_account(test_s2s_src_s3_service_url):
        print("failed to clean s3 account.")

    # cleaning the test share provided
    # all files and directories inside the share will be deleted.
    test_share_url = share_sas_url
    if not clean_test_share(test_share_url):
        print("failed to clean test share.")

    return True

# initialize_test_suite initializes the setup for executing test cases.
def initialize_interactive_test_suite(test_dir_path, container_oauth, container_oauth_validate, 
    filesystem_url, oauth_tenant_id, oauth_aad_endpoint, azcopy_exec_location, test_suite_exec_location):
    # test_directory_path is global variable holding the location of test directory to execute all the test cases.
    # contents are created, copied, uploaded and downloaded to and from this test directory only
    global test_directory_path

    # test_oauth_container_url is a global variable used in the entire testSuite holding the user given container for oAuth testing.
    # all files / directory are uploaded and downloaded to and from this container.
    global test_oauth_container_url

    # test_container_oauth_validate_sas_url is same container as test_oauth_container_url, while for validation purpose. 
    global test_oauth_container_validate_sas_url

    # holds the name of the azcopy executable
    global azcopy_executable_name

    # holds the name of the test suite executable
    global test_suite_executable_name

    # holds the filesystem url to perform the operations for blob fs service
    global test_bfs_account_url

    # holds the oauth tenant id
    global test_oauth_tenant_id

    # holds the oauth aad encpoint
    global test_oauth_aad_endpoint

    # creating a test_directory in the location given by user.
    # this directory will be used to created and download all the test files.
    new_dir_path = os.path.join(test_dir_path, "test_data")
    # todo finally
    try:
        # removing the directory and its contents, if directory exists
        shutil.rmtree(new_dir_path)
        os.mkdir(new_dir_path)
    except:
        os.mkdir(new_dir_path)

    # copying the azcopy executable to the newly created test directory.
    # this copying is done to avoid using the executables at location which might be used by the user
    # while test suite is running.
    if os.path.isfile(azcopy_exec_location):
        shutil.copy2(azcopy_exec_location, new_dir_path)
        azcopy_executable_name = parse_out_executable_name(azcopy_exec_location)
    else:
        print("please verify the azcopy executable location")
        return False

    # copying the test executable to the newly created test directory.
    # this copying is done to avoid using the executables at location which might be used by the user
    # while test suite is running.
    if os.path.isfile(test_suite_exec_location):
        shutil.copy2(test_suite_exec_location, new_dir_path)
        test_suite_executable_name = parse_out_executable_name(test_suite_exec_location)
    else:
        print("please verify the test suite executable location")
        return False

    test_directory_path = new_dir_path

    test_oauth_tenant_id = oauth_tenant_id
    test_oauth_aad_endpoint = oauth_aad_endpoint

    # set the filesystem url
    test_bfs_account_url = filesystem_url
    if not clean_test_filesystem(test_bfs_account_url):
        return False
    if not (test_bfs_account_url.endswith("/") and test_bfs_account_url.endwith("\\")):
        test_bfs_account_url = test_bfs_account_url + "/"

    test_oauth_container_url = container_oauth
    if not (test_oauth_container_url.endswith("/") and test_oauth_container_url.endwith("\\")):
        test_oauth_container_url = test_oauth_container_url + "/"
    
    # as validate container URL point to same URL as oauth container URL, do clean up with validate container URL
    test_oauth_container_validate_sas_url = container_oauth_validate
    if not clean_test_container(test_oauth_container_validate_sas_url):
        return False

    return True


# given a path, parse out the name of the executable
def parse_out_executable_name(full_path):
    head, tail = os.path.split(full_path)
    return tail

# todo : find better way
# create_test_file creates a file with given file name and of given size inside the test directory.
# returns the local file path.
def create_test_file(filename, size):
    # creating the file path
    file_path = os.path.join(test_directory_path, filename)
    # if file already exists, then removing the file.
    if os.path.isfile(file_path):
        os.remove(file_path)
    f = open(file_path, 'w')
    # since size of file can very large and size variable can overflow while holding the file size
    # file is written in blocks of 1MB.
    if size > 1024 * 1024:
        total_size = size
        while total_size > 0:
            num_chars = 1024 * 1024
            if total_size < num_chars:
                num_chars = total_size
            f.write('0' * num_chars)
            total_size = total_size - num_chars
    else:
        num_chars = size
        f.write('0' * num_chars)
    f.close()
    return file_path

def create_json_file(filename, jsonData):
    # creating the file path
    file_path = os.path.join(test_directory_path, filename + ".json")
    # if file already exists, then removing the file.
    if os.path.isfile(file_path):
        os.remove(file_path)
    with open(file_path, 'w') as outfile:
        json.dump(jsonData, outfile)
    outfile.close()
    return file_path

def create_new_list_of_files(filename, list):
    # creating the file path
    file_path = os.path.join(test_directory_path, filename + ".txt")
    if os.path.isfile(file_path):
        os.remove(file_path)
    with open(file_path, 'w') as outfile:
        outfile.writelines(list)
    outfile.close()
    return file_path

# creates the a test html file inside the test directory.
# returns the local file path.
def create_test_html_file(filename):
    # creating the file path
    file_path = os.path.join(test_directory_path, filename)
    # if file already exists, then removing the file.
    if os.path.isfile(file_path):
        os.remove(file_path)

    f = open(file_path, 'w')
    message = """<html>
                    <head></head>
                        <body><p>Hello World!</p></body>
                </html>"""
    f.write(message)
    f.close()
    return file_path


# creates a dir with given inside test directory
def create_test_dir(dir_name):
    # If the directory exists, remove it.
    dir_path = os.path.join(test_directory_path, dir_name)
    if os.path.isdir(dir_path):
        shutil.rmtree(dir_path)
    try:
        os.mkdir(dir_path)
    except:
        raise Exception("error creating directory ", dir_path)
    return dir_path


# create_test_n_files creates given number of files for given size
# inside directory inside test directory.
# returns the path of directory in which n files are created.
def create_test_n_files(size, n, dir_name):
    # creating directory inside test directory.
    dir_n_files_path = os.path.join(test_directory_path, dir_name)
    try:
        shutil.rmtree(dir_n_files_path)
        os.mkdir(dir_n_files_path)
    except:
        os.mkdir(dir_n_files_path)
    # creating file prefix
    filesprefix = "test" + str(n) + str(size)
    # creating n files.
    for index in range(0, n):
        filename = filesprefix + '_' + str(index) + ".txt"
        # creating the file path
        file_path = os.path.join(dir_n_files_path, filename)
        # if file already exists, then removing the file.
        if os.path.isfile(file_path):
            os.remove(file_path)
        f = open(file_path, 'w')
        # since size of file can very large and size variable can overflow while holding the file size
        # file is written in blocks of 1MB.
        if size > 1024 * 1024:
            total_size = size
            while total_size > 0:
                num_chars = 1024 * 1024
                if total_size < num_chars:
                    num_chars = total_size
                f.write('0' * num_chars)
                total_size = total_size - num_chars
        else:
            num_chars = size
            f.write('0' * num_chars)
        f.close()
    return dir_n_files_path


# create_complete_sparse_file creates an empty used to
# test the page blob operations of azcopy
def create_complete_sparse_file(filename, filesize):
    file_path = os.path.join(test_directory_path, filename)
    sparse = Path(file_path)
    sparse.touch()
    os.truncate(str(sparse), filesize)
    return file_path


# create_partial_sparse_file create a sparse file in test directory
# of size multiple of 8MB. for each 8MB, first 4MB is '0'
# and next 4MB is '\0'.
# return the local file path of created file.
def create_partial_sparse_file(filename, filesize):
    file_path = os.path.join(test_directory_path, filename)
    if os.path.isfile(file_path):
        os.remove(file_path)
    f = open(file_path, 'w')
    # file size is less than 8MB or given size is not multiple of 8MB,
    # no file is created.
    if filesize < 8 * 1024 * 1024 or filesize % (8 * 1024 * 1024) != 0:
        return None
    else:
        total_size = filesize
        while total_size > 0:
            num_chars = 4 * 1024 * 1024
            f.write('0' * num_chars)
            total_size = total_size - num_chars
            if total_size <= 0:
                break
            f.write('\0' * num_chars)
            total_size = total_size - num_chars
    return file_path


# execute_azcopy_command executes the given azcopy command.
# returns true / false on success / failure of command.
def execute_azcopy_command(command):
    # azcopy executable path location.
    azspath = os.path.join(test_directory_path, azcopy_executable_name)
    cmnd = azspath + " " + command

    try:
        # executing the command with timeout to set 3 minutes / 180 sec.
        subprocess.check_output(
            cmnd, stderr=subprocess.STDOUT, shell=True, timeout=360,
            universal_newlines=True)
    except subprocess.CalledProcessError as exec:
        # todo kill azcopy command in case of timeout
        print("command failed with error code " , exec.returncode , " and message " + exec.output)
        return False
    else:
        return True

# execute_azcopy_command_interactive executes the given azcopy command in "inproc" mode.
# returns azcopy console output or none on success / failure of command.
def execute_azcopy_command_interactive(command):
    # azcopy executable path location concatenated with inproc keyword.
    azspath = os.path.join(test_directory_path, azcopy_executable_name)
    cmnd = azspath + " " + command
    if os.name == "nt":
        process = subprocess.Popen(cmnd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    else:
        process = subprocess.Popen(shlex.split(cmnd), stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    for line in iter(process.stdout.readline, b''):
        print(line.decode('utf-8'))
    process.wait()
    if process.poll() == 0:
        return True
    else:
        return False


# execute_azcopy_command_get_output executes the given azcopy command in "inproc" mode.
# returns azcopy console output or none on success / failure of command.
def execute_azcopy_command_get_output(command):
    # azcopy executable path location concatenated with inproc keyword.
    azspath = os.path.join(test_directory_path, azcopy_executable_name)
    cmnd = azspath + " " + command
    output = ""
    try:
        # executing the command with timeout set to 4 minutes / 240 sec.
        output = subprocess.check_output(
            cmnd, stderr=subprocess.STDOUT, shell=True, timeout=240,
            universal_newlines=True)
    except subprocess.CalledProcessError as exec:
        # print("command failed with error code ", exec.returncode, " and message " + exec.output)
        return exec.output
    else:
        return output


# verify_operation executes the validator command to verify the azcopy operations.
# return true / false on success / failure of command.
def verify_operation(command):
    # testSuite executable local path inside the test directory.
    test_suite_path = os.path.join(test_directory_path, test_suite_executable_name)
    command = test_suite_path + " " + command
    try:
        # executing the command with timeout set to 4 minutes / 240 sec.
        subprocess.check_output(
            command, stderr=subprocess.STDOUT, shell=True, timeout=240,
            universal_newlines=True)
    except subprocess.CalledProcessError as exec:
        # print("command failed with error code ", exec.returncode, " and message " + exec.output)
        return False
    else:
        return True

# verify_operation_get_output executes the validator command and returns output.
def verify_operation_get_output(command):
    # testSuite executable local path inside the test directory.
    test_suite_path = os.path.join(test_directory_path, test_suite_executable_name)
    command = test_suite_path + " " + command
    try:
        # executing the command with timeout set to 3 minutes / 180 sec.
        output = subprocess.check_output(
            command, stderr=subprocess.STDOUT, shell=True, timeout=600,
            universal_newlines=True)
    except subprocess.CalledProcessError as exec:
        #print("command failed with error code ", exec.returncode, " and message " + exec.output)
        return None
    else:
        return output

def get_object_sas(url_with_sas, object_name):
    # Splitting the container URL to add the uploaded blob name to the SAS
    url_parts = url_with_sas.split("?")
    # adding the blob name after the container name
    if url_parts[0].endswith("/"):
        resource_sas = url_parts[0] +  object_name + '?' + url_parts[1]
    else:
        resource_sas = url_parts[0] + "/" + object_name + '?' + url_parts[1]
    return resource_sas

def get_object_without_sas(url, object_name):
    # Splitting the container URL to add the uploaded blob name to the SAS
    url_parts = url.split("?")
    # adding the blob name after the container name
    if url_parts[0].endswith("/"):
        resource_sas = url_parts[0] + object_name
    else:
        resource_sas = url_parts[0] + "/" + object_name
    return resource_sas

# get_resource_sas return the shared access signature for the given resource
# using the container url.
def get_resource_sas(resource_name):
    # Splitting the container URL to add the uploaded blob name to the SAS
    url_parts = test_container_url.split("?")
    # adding the blob name after the container name
    resource_sas = url_parts[0] + "/" + resource_name + '?' + url_parts[1]
    return resource_sas

def get_resource_from_oauth_container_validate(resource_name):
    # Splitting the container URL to add the uploaded blob name to the SAS
    url_parts = test_oauth_container_validate_sas_url.split("?")
    # adding the blob name after the container name
    resource_sas = url_parts[0] + "/" + resource_name + '?' + url_parts[1]
    return resource_sas

def get_resource_from_oauth_container(resource_name):
    return test_oauth_container_url + resource_name


def append_text_path_resource_sas(resource_sas, text):
    # Splitting the resource sas to add the text to the SAS
    url_parts = resource_sas.split("?")

    # adding the text to the blob name of the resource sas
    if url_parts[0].endswith("/"):
        # If there is a separator at the end of blob name
        # no need to append "/" before the text after the blob name
        resource_sas = url_parts[0] + text + '?' + url_parts[1]
    else:
        resource_sas = url_parts[0] + "/" + text + '?' + url_parts[1]
    return resource_sas


# get_resource_sas_from_share return the shared access signature for the given resource
# based on the share url.
def get_resource_sas_from_share(resource_name):
    # Splitting the share URL to add the file or directory name to the SAS
    url_parts = test_share_url.split("?")
    # adding the file or directory name after the share name
    resource_sas = url_parts[0] + "/" + resource_name + '?' + url_parts[1]
    return resource_sas

def get_resource_sas_from_bfs(resource_name):
    # Splitting the share URL to add the file or directory name to the SAS
    url_parts = test_bfs_sas_account_url.split("?")
    # adding the file or directory name after the share name
    resource_sas = url_parts[0] + "/" + resource_name + '?' + url_parts[1]
    return resource_sas


# get_resource_sas return the shared access signature for the given resource
# using the premium storage account container url.
def get_resource_sas_from_premium_container_sas(resource_name):
    # Splitting the container URL to add the uploaded blob name to the SAS
    url_parts = test_premium_account_contaier_url.split("?")
    # adding the blob name after the container name
    resource_sas = url_parts[0] + "/" + resource_name + '?' + url_parts[1]
    return resource_sas


# parseAzcopyOutput parses the Azcopy Output in JSON format to give the final Azcopy Output in JSON Format
# Final Azcopy Output is the last JobSummary for the Job
# Azcopy Output can have more than one Summary for the Job
# parseAzcopyOutput returns the final JobSummary in JSON format.
def parseAzcopyOutput(s):
    count = 0
    output = ""
    final_output = ""
    # Split the lines
    lines = s.split('\n')
    # Iterating through the output in reverse order since last summary has to be considered.
    # Increment the count when line is "}"
    # Reduce the count when line is "{"
    # append the line to final output
    # When the count is 0, it means the last Summary has been traversed
    for line in reversed(lines):
        # If the line is empty, then continue
        if line == "":
            continue
        elif line is '}':
            count = count + 1
        elif line is "{":
            count = count - 1
        if count >= 0:
            if len(output) > 0:
                output = output + '\n' + line
            else:
                output = line
        if count == 0:
            break
    lines = output.split('\n')
    # Since the lines were iterated in reverse order revering them again and
    # concatenating the lines to get the final JobSummary
    for line in reversed(lines):
        if len(final_output) > 0:
            final_output = final_output + '\n' + line
        else:
            final_output = line

    x = json.loads(final_output, object_hook=lambda d: namedtuple('X', d.keys())(*d.values()))
    return x.MessageContent

def get_resource_name(prefix=''):
    return prefix + str(uuid.uuid4()).replace('-', '')
    
def get_random_bytes(size):
    rand = random.Random()
    result = bytearray(size)
    for i in range(size):
        result[i] = int(rand.random()*255)  # random() is consistent between python 2 and 3
    return bytes(result)

def create_hidden_file(path, file_name, data):
    FILE_ATTRIBUTE_HIDDEN = 0x02
    os_type = platform.system()
    os_type = os_type.upper()

    # For *nix add a '.' prefix.
    prefix = '.' if os_type != "WINDOWS" else ''
    file_name = prefix + file_name

    file_path = os.path.join(path, file_name)
    # Write file.
    with open(file_path, 'w') as f:
        f.write(data)

    # For windows set file attribute.
    if os_type == "WINDOWS":
        ret = ctypes.windll.kernel32.SetFileAttributesW(file_path,
                                                        FILE_ATTRIBUTE_HIDDEN)
        if not ret: # There was an error.
            raise ctypes.WinError()

def create_file_in_path(path, file_name, data):
    file_path = os.path.join(path, file_name)
    with open(file_path, 'w') as f:
        f.write(data)
    return file_path