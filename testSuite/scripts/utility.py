import os
import subprocess
import shutil

def initialize_test_suite(test_dir_path, container_sas, azcopy_exec_location, test_suite_exec_location):
    try:
        new_dir_path = os.path.join(test_dir_path, "test_data")
        #removing the directory and its contents, if directory exists
        shutil.rmtree(new_dir_path)
        os.mkdir(new_dir_path)
    except:
        # if the directory exists and cannot be removed, then we need to ask for another directory to execute the test
        os.mkdir(new_dir_path)

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

    return True

def create_test_file(filepath, filename , size):
    file_path = os.path.join(filepath , filename)
    if os.path.isfile(file_path):
        os.remove(file_path)
    f = open(file_path, 'w')
    num_chars = size
    print(f.write('0' * num_chars))
    f.close()
    return file_path

def execute_azcopy_command(test_dir_path, command):
    azspath = os.path.join(test_dir_path, "azs.exe")
    cmnd = azspath + " " + command
    print("command ", command)
    try:
        subprocess.check_output(
            cmnd, stderr=subprocess.STDOUT, shell=True, timeout=180,
            universal_newlines=True)
    except subprocess.CalledProcessError as exec:
        print("command failed with error code " , exec.returncode , " and message " + exec.output)
        return False
    else:
        return True

def verify_operation(test_dir_path , command):
    test_suite_path = os.path.join(test_dir_path, "testSuite.exe")
    command = test_suite_path + " " + command
    print("command " , command)
    try:
        subprocess.check_output(
            command, stderr=subprocess.STDOUT, shell=True, timeout=180,
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

def create_test_n_files(filespath, size, numberOfFiles):
    filesprefix = "test" +str(numberOfFiles) + (size)
    for index in range(0, numberOfFiles):
        filepath = os.path.join(filespath, filesprefix + '_' + str(index) + "txt")
        if os.path.isfile(filepath):
            print("file already exists")
            continue
        f = open(filepath, 'w')
        num_chars = size
        print(f.write('0' * num_chars))
        f.close()
