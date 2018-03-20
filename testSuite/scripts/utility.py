import os
import subprocess
import shutil
from pathlib import Path

class Command(object):
    def __init__(self, command_type):
        self.command_type = command_type
        self.flags = dict()
        self.args = list()

    def add_arguments(self, argument):
        if argument == None:
            return
        self.args.append(argument)
        return self

    def add_flags(self, flag, value):
        self.flags[flag] = value
        return self

    def string(self):
        command = self.command_type
        if len(self.args) > 0:
            for arg in self.args:
                if (len(arg) > 0):
                    command += " " + '"' + arg + '"'
            if len(self.flags) > 0:
                for key, value in self.flags.items():
                    command += " --" + key + "=" + '"' +value +'"'
        return command

    def execute_azcopy_copy_command(self, download = None):
        if download is None:
            self.add_arguments(test_container_url)
        else:
            resource_sas = get_resource_sas(self.args[0])
            local_path = test_directory_path + "/" + self.args[0]
            self.args[0] = resource_sas
            self.add_arguments(local_path)
        return execute_azcopy_command(self.string())

    def execute_azcopy_copy_command_get_output(self, download=None):
        if download is None:
            self.add_arguments(test_container_url)
        else:
            resource_sas = get_resource_sas(self.args[0])
            local_path = test_directory_path + "/" + self.args[0]
            self.args[0] = resource_sas
            self.add_arguments(local_path)
        return execute_azcopy_command_get_output(self.string())

    def execute_azcopy_operation_get_output(self):
        return  execute_azcopy_command_get_output(self.string())

    def execute_azcopy_verify(self):
        self.add_arguments(test_directory_path)
        self.add_arguments(test_container_url)
        return verify_operation(self.string())

    def execute_azcopy_clean(self):
        return verify_operation(self.string())

def clean_test_container():
    result = Command("clean").add_arguments(test_container_url).execute_azcopy_clean()
    if not result:
        print("error cleaning the container. please check the container sas provided")
        return False
    return True


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

def create_test_file(filename , size):
    file_path = os.path.join(test_directory_path , filename)
    if os.path.isfile(file_path):
        os.remove(file_path)
    f = open(file_path, 'w')
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

def create_test_html_file(filename):
    file_path = os.path.join(test_directory_path , filename)
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

def execute_azcopy_command(command):
    azspath = os.path.join(test_directory_path, "azs.exe inproc")
    cmnd = azspath + " " + command
    try:
        subprocess.check_output(
            cmnd, stderr=subprocess.STDOUT, shell=True, timeout=180,
            universal_newlines=True)
    except subprocess.CalledProcessError as exec:
        print("command failed with error code " , exec.returncode , " and message " + exec.output)
        return False
    else:
        return True

def execute_azcopy_command_get_output(command):
    azspath = os.path.join(test_directory_path, "azs.exe inproc")
    cmnd = azspath + " " + command
    output = ""
    try:
        output = subprocess.check_output(
            cmnd, stderr=subprocess.STDOUT, shell=True, timeout=180,
            universal_newlines=True)
    except subprocess.CalledProcessError as exec:
        print("command failed with error code " , exec.returncode , " and message " + exec.output)
        return None
    else:
        return output

def verify_operation(command):
    test_suite_path = os.path.join(test_directory_path, "testSuite.exe")
    command = test_suite_path + " " + command
    try:
        subprocess.check_output(
            command, stderr=subprocess.STDOUT, shell=True, timeout=600,
            universal_newlines=True)
    except subprocess.CalledProcessError as exec:
        print("command failed with error code " , exec.returncode , " and message " + exec.output)
        return False
    else:
        return True

def get_resource_sas(container_sas, resource_name):
    # Splitting the container URL to add the uploaded blob name to the SAS
    url_parts = container_sas.split("?")
    # adding the blob name after the container name
    resource_sas = url_parts[0] + "/" +resource_name + '?' + url_parts[1]
    return resource_sas

def create_test_n_files(size, numberOfFiles):

    dir_n_files_path = os.path.join(test_directory_path, "dir_n_1kb_files")
    try:
        shutil.rmtree(dir_n_files_path)
        os.mkdir(dir_n_files_path)
    except:
        os.mkdir(dir_n_files_path)

    filesprefix = "test" +str(numberOfFiles) + str(size)
    for index in range(0, numberOfFiles):
        filepath = os.path.join(dir_n_files_path, filesprefix + '_' + str(index) + ".txt")
        if os.path.isfile(filepath):
            print("file already exists")
            continue
        f = open(filepath, 'w')
        num_chars = size
        f.write('0' * num_chars)
        f.close()
    return dir_n_files_path

def create_complete_sparse_file(filename, filesize):
    file_path = os.path.join(test_directory_path , filename)
    sparse = Path(file_path)
    sparse.touch()
    os.truncate(str(sparse), filesize)
    return file_path

def create_partial_sparse_file(filename, filesize):
    file_path = os.path.join(test_directory_path , filename)
    if os.path.isfile(file_path):
        os.remove(file_path)
    f = open(file_path, 'w')
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

def get_resource_sas(resource_name):
    parts = test_container_url.split("?")
    return parts[0] + "/" + resource_name + "?" + parts[1]


