import shutil
import os
import sys
import subprocess

def main():
    text = input("Enter location of directory where you want to execute test: \n")
    container_sas = input("please provide the container's shared access signature: \n")
    dir_path = clear_and_create_test_data_dir(text)
    create_test_1kb_file(dir_path, container_sas)

# This api clears an existing test_data dir inside the location provided.
# The dir test_data will be used to create test files and perform operations.
# test_data directory will be created only for the use AZCopy test and will be deleted after the test is done
def clear_and_create_test_data_dir(directory):
    try:
        # performing stat system call on the directory
        # if directory does not exists it will create the directory
        dir_path = os.path.join(directory, "test_data")
        os.stat(dir_path)
        try:
            #removing the directory and its contents. if directory is not complete
            shutil.rmtree(dir_path)
            os.mkdir(dir_path)
        except:
            print("directory given is currently used or not accessible. Please provide another directory")
            sys.exit(1)
    except:
        # if test_data directory doesn't exists, it creates the directory
        os.mkdir(dir_path)
    return dir_path

def create_test_1kb_file(dir_path, sas):
    # Creating a single File Of size 1 KB
    file_path = os.path.join(dir_path , "test1KB.txt")
    f = open(file_path, 'w')
    num_chars = 1024
    print(f.write('0' * num_chars))
    f.close()

    # Uploading the single file of size 1KB
    dest =  sas
    src = file_path
    cmnd = "C:\\Go\\externals\\src\\github.com\\Azure\\azure-storage-azcopy\\azs.exe copy " + src + " " + dest + " --Logging 5 --recursive"
    print("command ", cmnd)
    try:
        output = subprocess.check_output(
        cmnd, stderr=subprocess.STDOUT, shell=True, timeout=3,
        universal_newlines=True)
    except subprocess.CalledProcessError as exc:
        print("Status : FAIL", exc.returncode, exc.output)
    else:
        print("created_1kb_file")

    # Verifying the uploaded blob
    # Splitting the container URL to add the uploaded blob name to the SAS
    url_parts = sas.split("?")
    # adding the blob name after the container name
    blob_url = url_parts[0] + "\\test1KB.txt" + '?' + url_parts[1]

    # calling the testblob validator to verify whether blob has been successfully uploaded or not
    cmnd = "testSuite.exe testBlob " + blob_url + ' ' + file_path
    print("command ", cmnd)
    try:
        output = subprocess.check_output(
            cmnd, stderr=subprocess.STDOUT, shell=True, timeout=3,
            universal_newlines=True)
    except subprocess.CalledProcessError as exc:
        print("Status : FAIL", exc.returncode, exc.output)
    else:
        print("verified upload of 1KB file")
main()
