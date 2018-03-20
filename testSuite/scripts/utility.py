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

    def execute_azcopy_copy_command(self):
        self.add_arguments(test_container_url)
        return execute_azcopy_command(self.string())

    def execute_azcopy_copy_command_get_output(self):
        self.add_arguments(test_container_url)
        return execute_azcopy_command_get_output(self.string())

    def execute_azcopy_operation_get_output(self):
        return  execute_azcopy_command_get_output(self.string())

    def execute_azcopy_verify(self):
        self.add_arguments(test_directory_path)
        self.add_arguments(test_container_url)
        return verify_operation(self.string())

def initialize_test_suite(test_dir_path, container_sas, azcopy_exec_location, test_suite_exec_location):
    global test_directory_path
    global test_container_url
    new_dir_path = os.path.join(test_dir_path, "test_data")
    try:
        #removing the directory and its contents, if directory exists
        shutil.rmtree(new_dir_path)
        os.mkdir(new_dir_path)
    except:
        # if the directory exists and cannot be removed, then we need to ask for another directory to execute the test
        os.mkdir(new_dir_path)

    test_container_url = container_sas
    # TODO validate the continer url

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
    azspath = os.path.join(test_directory_path, "azs.exe")
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
    azspath = os.path.join(test_directory_path, "azs.exe")
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
        filepath = os.path.join(dir_n_files_path, filesprefix + '_' + str(index) + "txt")
        if os.path.isfile(filepath):
            print("file already exists")
            continue
        f = open(filepath, 'w')
        num_chars = size
        f.write('0' * num_chars)
        f.close()
    return dir_n_files_path

def create_sparse_file(filename, filesize):
    file_path = os.path.join(test_directory_path , filename)
    sparse = Path(file_path)
    sparse.touch()
    os.truncate(str(sparse), filesize)
    return file_path