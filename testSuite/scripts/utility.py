import os
import subprocess
import shutil
from pathlib import Path

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
                command += " --" + key + "=" + '"' +value +'"'
        return command

    # this api is used to execute a azcopy copy command.
    # by default, command execute a upload command.
    # return true or false for success or failure of command.
    def execute_azcopy_copy_command(self, download = None):
        if download is None:
            # adding test_container url as a second argument.
            self.add_arguments(test_container_url)
        else:
            # if the copy is to download from container, then container url needs to be added first.
            # resource_sas is the sas of resource to be downloaded.
            resource_sas = get_resource_sas(self.args[0])

            # local_path is the location where resouce needs to be downloaded.
            local_path = test_directory_path

            # since reosource is added first, changing the position of resource_sas and local_path in args list.
            self.args[0] = resource_sas
            self.add_arguments(local_path)
        return execute_azcopy_command(self.string())

    # this api is used to execute a azcopy copy command.
    # by default, command execute a upload command.
    # return azcopy console output on successful execution.
    def execute_azcopy_copy_command_get_output(self, download=None):
        if download is None:
            # adding test_container url as a second argument.
            self.add_arguments(test_container_url)
        else:

            # if the copy is to download from container, then container url needs to be added first.
            # resource_sas is the sas of resource to be downloaded.
            resource_sas = get_resource_sas(self.args[0])

            # local_path is the location where resouce needs to be downloaded.
            local_path = test_directory_path

            # since reosource is added first, changing the position of resource_sas and local_path in args list.
            self.args[0] = resource_sas
            self.add_arguments(local_path)
        return execute_azcopy_command_get_output(self.string())

    # api execute other azcopy commands like cancel, pause, resume or list.
    def execute_azcopy_operation_get_output(self):
        return  execute_azcopy_command_get_output(self.string())

    # api executes the azcopy validator to verify the azcopy operation.
    def execute_azcopy_verify(self):
        # adds the test_directory.
        self.add_arguments(test_directory_path)
        # add the test container url.
        self.add_arguments(test_container_url)
        return verify_operation(self.string())

    # api executes the clean command to delete the blob or container contents.
    def execute_azcopy_clean(self):
        return verify_operation(self.string())

# api executes the clean command on validator which deletes all the contents of the container.
def clean_test_container():
    # execute the clean command.
    result = Command("clean").add_arguments(test_container_url).execute_azcopy_clean()
    if not result:
        print("error cleaning the container. please check the container sas provided")
        return False
    return True

# initialize_test_suite initializes the setup for executing test cases.
def initialize_test_suite(test_dir_path, container_sas, azcopy_exec_location, test_suite_exec_location):

    # test_directory_path is global variable holding the location of test directory to execute all the test cases.
    # contents are created, copied, uploaded and downloaded to and from this test directory only
    global test_directory_path

    # test_container_url is a global variable used in the entire testSuite holding the user given container shared access signature.
    # all files / directory are uploaded and downloaded to and from this container.
    global test_container_url

    # creating a test_directory in the location given by user.
    # this directory will be used to created and download all the test files.
    new_dir_path = os.path.join(test_dir_path, "test_data")
    try:
        #removing the directory and its contents, if directory exists
        shutil.rmtree(new_dir_path)
        os.mkdir(new_dir_path)
    except:
        os.mkdir(new_dir_path)

    # copying the azcopy executable to the newly created test directory.
    # this copying is done to avoid using the executables at location which might be used by the user
    # while test suite is running.
    if os.path.isfile(azcopy_exec_location):
        shutil.copy2(azcopy_exec_location, new_dir_path)
    else:
        print("please verify the azcopy executable location")
        return False

    # copying the test executable to the newly created test directory.
    # this copying is done to avoid using the executables at location which might be used by the user
    # while test suite is running.
    if os.path.isfile(test_suite_exec_location):
        shutil.copy2(test_suite_exec_location, new_dir_path)
    else:
        print("please verify the test suite executable location")
        return False

    test_directory_path = new_dir_path

    #cleaning the test container provided
    # all blob inside the container will be deleted.
    test_container_url = container_sas
    if not clean_test_container():
        return False

    return True

# create_test_file creates a file with given file name and of given size inside the test directory.
# returns the local file path.
def create_test_file(filename , size):
    # creating the file path
    file_path = os.path.join(test_directory_path , filename)
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

# creates the a test html file inside the test directory.
# returns the local file path.
def create_test_html_file(filename):
    # creating the file path
    file_path = os.path.join(test_directory_path , filename)
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
        file_path = os.path.join(dir_n_files_path , filename)
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
    file_path = os.path.join(test_directory_path , filename)
    sparse = Path(file_path)
    sparse.touch()
    os.truncate(str(sparse), filesize)
    return file_path

# create_partial_sparse_file create a sparse file in test directory
# of size multiple of 8MB. for each 8MB, first 4MB is '0'
# and next 4MB is '\0'.
# return the local file path of created file.
def create_partial_sparse_file(filename, filesize):
    file_path = os.path.join(test_directory_path , filename)
    if os.path.isfile(file_path):
        os.remove(file_path)
    f = open(file_path, 'w')
    # file size is less than 8MB or given size is not multiple of 8MB,
    # no file is created.
    if filesize < 8*1024*1024 or filesize % (8 * 1024 * 1024) != 0:
        return None
    else:
        total_size = filesize
        while total_size > 0:
            num_chars = 4* 1024 * 1024
            f.write('0' *  num_chars)
            total_size = total_size - num_chars
            if total_size <= 0:
                break
            f.write('\0' * num_chars)
            total_size = total_size - num_chars
    return file_path

# execute_azcopy_command executes the given azcopy command in "inproc" mode.
# returns true / false on success / failure of command.
def execute_azcopy_command(command):
    # azcopy executable path location concatenated with inproc keyword.
    azspath = os.path.join(test_directory_path, "azs.exe inproc")
    cmnd = azspath + " " + command
    try:
        # executing the command with timeout to set 3 minutes / 180 sec.
        subprocess.check_output(
            cmnd, stderr=subprocess.STDOUT, shell=True, timeout=180,
            universal_newlines=True)
    except subprocess.CalledProcessError as exec:
        print("command failed with error code " , exec.returncode , " and message " + exec.output)
        return False
    else:
        return True

# execute_azcopy_command_get_output executes the given azcopy command in "inproc" mode.
# returns azcopy console output or none on success / failure of command.
def execute_azcopy_command_get_output(command):
    # azcopy executable path location concatenated with inproc keyword.
    azspath = os.path.join(test_directory_path, "azs.exe inproc")
    cmnd = azspath + " " + command
    output = ""
    try:
        # executing the command with timeout set to 3 minutes / 180 sec.
        output = subprocess.check_output(
            cmnd, stderr=subprocess.STDOUT, shell=True, timeout=180,
            universal_newlines=True)
    except subprocess.CalledProcessError as exec:
        print("command failed with error code " , exec.returncode , " and message " + exec.output)
        return None
    else:
        return output

# verify_operation executes the validator command to verify the azcopy operations.
# return true / false on success / failure of command.
def verify_operation(command):
    # testSuite executable local path inside the test directory.
    test_suite_path = os.path.join(test_directory_path, "testSuite.exe")
    command = test_suite_path + " " + command
    try:
        # executing the command with timeout set to 3 minutes / 180 sec.
        subprocess.check_output(
            command, stderr=subprocess.STDOUT, shell=True, timeout=600,
            universal_newlines=True)
    except subprocess.CalledProcessError as exec:
        print("command failed with error code " , exec.returncode , " and message " + exec.output)
        return False
    else:
        return True

# get_resource_sas return the shared access signature for the given resource
# using the container url.
def get_resource_sas(resource_name):
    # Splitting the container URL to add the uploaded blob name to the SAS
    url_parts = test_container_url.split("?")
    # adding the blob name after the container name
    resource_sas = url_parts[0] + "/" +resource_name + '?' + url_parts[1]
    return resource_sas



