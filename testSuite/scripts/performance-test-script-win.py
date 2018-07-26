import subprocess
import os
import glob
import time
import shutil

number_iterations = 1

# location of the executable relative to the the location of the script
azcopy_v2_exe_name = "azcopy-v2-win.exe"
azcopy_v1_exe_name = "AzCopy\\AzCopy.exe"
bporter_exec_name  = "BlobPorter.exe"

# Credentials for BlobPorter
account_name = "azcopyv2htbbtest"
account_key = "oYlWbyps8vsKc4X6uCJ0sSfpY+DuCvyFAav6UjV8Fpoa9Vsh9vUgqku8NvQdWfX12rrEEG2c0X7YZqfe6aOr3g=="
container_name = "cont-1"

# location of dataset and remote url
local_dir = "C:\\Users\\prjain\\Documents\\Sample_Files"
remote_container_sas = "https://azcopyv2htbbtest.blob.core.windows.net/cont-1?st=2018-07-24T04%3A59%3A00Z&se=2018-12-31T04%3A59%3A00Z&sp=rwdl&sv=2017-04-17&sr=c&sig=ghNvjg9vr%2BY5Omjioc9WYm2IbOgjdTaiLxwM%2B5uHlks%3D"

#datasets = dict({"100KX3K" : True, "100X100MB" : True})
datasets = dict(
    {"10kb.txt" : False}
     # "100KX3K" : True}
)

#datasets = dict()
exec_list = [azcopy_v2_exe_name, azcopy_v1_exe_name, bporter_exec_name]


def write_text_to_file(file, message):
    #file.write('\n')
    file.write(message + '\n')
    file.write('\n')

# Clean up the Azcopy-v1 Journals
def azcopy_v1_clean_up():
    journal_location = os.path.join(os.environ.get('AppData'), "Microsoft", "Azure", "Azcopy")
    try:
        shutil.rmtree(journal_location)
    except:
        return

# Clean Azcopy-v2 JobPartPlanFiles and log files.
def azcopy_v2_clean_up():
    # delete the log files
    for f in glob.glob('*.log'):
        try:
            os.remove(f)
        except OSError:
            continue
    partplanfilelocation = os.path.join(os.environ.get('LocalAppData'), "Azcopy")
    try:
        shutil.rmtree(partplanfilelocation)
    except:
        return

def bporter_clean_init():
    os.environ["ACCOUNT_NAME"] = account_name
    os.environ["ACCOUNT_KEY"] = account_key

def get_bporter_upload_copy_command(source, destination):
    command = bporter_exec_name
    command += " -f"
    command += " " + source
    command += " -c " + destination
    return command

def get_bporter_download_copy_command(source, destination):
    command = bporter_exec_name
    command += " -c"
    command += " " + source
    command += " -f " + destination
    return command

def append_resource_to_given_sas(source, resource):
    # Splitting the resource sas to add the text to the SAS
    url_parts = source.split("?")
    # adding the text to the blob name of the resource sas
    if url_parts[0].endswith("/"):
        # If there is a separator at the end of blob name
        # no need to append "/" before the text after the blob name
        resource_sas = url_parts[0] + resource + '?' + url_parts[1]
    else:
        resource_sas = url_parts[0] + "/" + resource + '?' + url_parts[1]
    return resource_sas

def get_azcopy_v1_upload_copy_command(source, destination, datasetname, isdir = False):
    command = azcopy_v1_exe_name
    command += " " + "/Source:" + get_argument_string(source)
    command += " " + "/Dest:" +  get_argument_string(append_resource_to_given_sas(destination, datasetname))
    # if the source is a directory, then add /S add the end of the command
    if isdir:
        return command + " /S"
    return command

def get_azcopy_v2_upload_copy_command(source, destination, isdir = False):
    command = azcopy_v2_exe_name
    command += " cp "
    command += get_argument_string(source)
    command += " " + get_argument_string(destination)
    command += " --log-level " + get_argument_string("Info")
    if isdir:
        return command + " --recursive"
    return command

def get_azcopy_v2_download_copy_command(source, destination, datasetname, isdir = False):
    command = azcopy_v2_exe_name
    command += " cp "
    command += get_argument_string(append_resource_to_given_sas(source, datasetname))
    command += " " + get_argument_string(os.path.join(destination, "azcopy-v2" +datasetname))
    command += " --log-level " + get_argument_string("Info")
    if isdir:
        command += " --recursive"
    try:
        os.mkdir(os.path.join(destination, "azcopy-v2" +datasetname))
    except:
        return ""
    return command

def get_azcopy_v1_download_copy_command(source, destination, datasetname, isdir = False):
    command = azcopy_v1_exe_name
    command += " " + "/Source:" + get_argument_string(append_resource_to_given_sas(source, datasetname))
    command += " " + "/Dest:" +  get_argument_string(os.path.join(destination, "azcopy-v1" + datasetname))
    # if the source is a directory, then add /S add the end of the command
    if isdir:
        command += " /S"
    try:
        os.mkdir(os.path.join(destination, "azcopy-v1" +datasetname))
    except:
        print("")
    return command



def get_azcopy_v2_remove_command(source):
    remove_command = azcopy_v2_exe_name
    remove_command += " rm "
    remove_command += get_argument_string(source)
    remove_command += " --log-level " + get_argument_string("Info")
    remove_command += " --recursive=true"
    return remove_command

def get_argument_string(arg):
    if len(arg) is 0 :
        return arg
    return '"' + arg + '"'

def parse_and_write_executable_result(result, file):
    lines = result.split('\n')
    lines = reversed(lines)
    lineCount = 0
    for line in lines:
        write_text_to_file(file, line)
        lineCount += 1
        if lineCount is 10:
            break

def remove_contents_in_container(file):
    # delete the data after uploading the dataset
    try:
        remove_command = get_azcopy_v2_remove_command(remote_container_sas)
        subprocess.check_output(remove_command, stderr=subprocess.STDOUT, shell=True, universal_newlines=True)
    except subprocess.CalledProcessError as e :
        write_text_to_file(file,"failed with error" + str(e))

def execute_performance_test(file) :
    # Iterate through each dataset in datasets dictionary
    for dataset in datasets :
        # source is the local directory from where the data will be uploaded
        source = os.path.join(local_dir, dataset)
        # source_d is the local directory where the data from blob storage will be download to
        source_d = os.path.join(local_dir, "perf_download")
        # create the source_d local directory
        try:
            shutil.rmtree(source_d)
        except:
            print("error deleting the source directory ", source_d, "created for download")
        finally:
            os.mkdir(source_d)
        # destination is the remote container sas
        destination = remote_container_sas
        # Is dataset directory or not
        isDir = datasets[dataset]
        # startTime is the start of the operation
        startTime = time.time()
        write_text_to_file(file, " =============== DataSet: " + dataset + "===============")
        # Iterate through each executable mentioned in the exec_list
        # Get the command for respective executable and current dataset first for upload and then downloaded
        # run the command with the executable
        # log the result and operation time to file
        # delete the uploaded dataset remotely and download dataset locally
        for exe in exec_list[:]  :
            print("------ the exec ", exe, "------ \n")
            result = ""
            write_text_to_file(file, "------------ " + exe + " -----------------")
            # Execute the Azcopy-v2 command
            if exe == azcopy_v2_exe_name:
                # Upload Operation
                try:
                    # Get the Azcopy-v2 executable command and run the command
                    command = get_azcopy_v2_upload_copy_command(source, destination, isDir)
                    startTime = time.time()
                    result = subprocess.check_output(command, stderr=subprocess.STDOUT, shell=True, universal_newlines=True)
                except subprocess.CalledProcessError as e :
                    # In case of exception log the exception to file and continue to another executable
                    write_text_to_file(file, "failed with error: " + str(e))
                    continue
                operation_time = time.time() - startTime
                parse_and_write_executable_result(result, file)
                write_text_to_file(file, "Upload Operation Time " + str(operation_time))

                # Download Operation
                try:
                    # Get the Azcopy-v2 Command Output and execute the Output
                    command = get_azcopy_v2_download_copy_command(destination, source_d, dataset, isDir)
                    startTime = time.time()
                    result = subprocess.check_output(command, stderr=subprocess.STDOUT, shell=True, universal_newlines=True)
                except subprocess.CalledProcessError as e :
                    # In case of exception, delete the above uploaded dataset and log the error to the file
                    write_text_to_file(file, "failed with error: " + str(e))
                    remove_contents_in_container(file)
                    continue
                # Calculate the Operation Time and log to the file
                operation_time = time.time() - startTime
                parse_and_write_executable_result(result, file)
                write_text_to_file(file, "Download Operation Time " + str(operation_time))

            # Azcopy-v1 operation
            elif exe == azcopy_v1_exe_name:
                # Upload operation
                try:
                    # Get the command string and execute the output
                    command = get_azcopy_v1_upload_copy_command(source, destination, dataset, isDir)
                    startTime = time.time()
                    result = subprocess.check_output(command, stderr=subprocess.STDOUT, shell=True, universal_newlines=True)
                except subprocess.CalledProcessError as e :
                    # In case of exception, log the exception to the file
                    write_text_to_file(file,"failed with error :" + str(e))
                    continue
                # Calculate the operation and log to the file
                operation_time = time.time() - startTime
                parse_and_write_executable_result(result, file)
                write_text_to_file(file, "Upload Operation Time " + str(operation_time))

                # Download operation
                try:
                    # Get the Azcopy-v1 command string and execute the command
                    command = get_azcopy_v1_download_copy_command(destination, source_d, dataset, isDir)
                    startTime = time.time()
                    result = subprocess.check_output(command, stderr=subprocess.STDOUT, shell=True, universal_newlines=True)
                except subprocess.CalledProcessError as e :
                    # In case of exception log the error to the file
                    write_text_to_file(file,"failed with error :" + str(e))
                    remove_contents_in_container(file)
                    continue
                # Calculate the operation time and log to the file
                operation_time = time.time() - startTime
                parse_and_write_executable_result(result, file)
                write_text_to_file(file, "Download Operation Time " + str(operation_time))

            # BlobPorter Operation
            elif exe == bporter_exec_name :
                # Upload Operation
                try:
                    # Get the Blob Porter Command string and execute the operation
                    command = get_bporter_upload_copy_command(source, container_name)
                    startTime = time.time()
                    result = subprocess.check_output(command, stderr=subprocess.STDOUT, shell=True, universal_newlines=True)
                except subprocess.CalledProcessError as e :
                    # In case of exception, delete the above uploaded dataset and log the exception to the file.
                    write_text_to_file(file,"failed with error :" + str(e))
                    remove_contents_in_container(file)
                    continue
                # Calculate the operation time and log to the file.
                operation_time = time.time() - startTime
                parse_and_write_executable_result(result, file)
                write_text_to_file(file, "Upload Operation Time " + str(operation_time))
                #Download Operation
                try:
                    # Create the destination folder, Get the blob porter command string and execute it
                    dst = os.path.join(source_d, "bporter-" + dataset)
                    os.mkdir(dst)
                    command = get_bporter_download_copy_command(container_name, dst)
                    print("Command ", command)
                    startTime = time.time()
                    # execute the command using blobPorter
                    result = subprocess.check_output(command, stderr=subprocess.STDOUT, shell=True, universal_newlines=True)
                except subprocess.CalledProcessError as e :
                    write_text_to_file(file,"failed with error :" + str(e))
                    continue
                # Calculate the operation time and log to the file.
                operation_time = time.time() - startTime
                parse_and_write_executable_result(result, file)
                write_text_to_file(file, "Download Operation Time " + str(operation_time))
            else :
                continue
            write_text_to_file(file, "------------ " + exe + " --------------")
            # delete the data after uploading the dataset
            remove_contents_in_container(file)


if __name__ == '__main__':
    f = open('Performance_Testing_Result.txt', 'w')
    azcopy_v1_clean_up()
    azcopy_v2_clean_up()
    bporter_clean_init()
    remove_contents_in_container(f)
    execute_performance_test(f)
    f.close()
