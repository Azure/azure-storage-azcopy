#!/usr/bin/env python

# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

from subprocess import check_call
import argparse
import os
import shutil

DEFAULT_DESTINATION_FOLDER = "./dist"
DEFAULT_SOURCE_FOLDER = "./"
THIRD_PARTY_NOTICE_FILE_NAME = "ThirdPartyNotice.txt"

# the list of executables to package are listed here
EXECUTABLES_TO_ZIP = ["azcopy_darwin_amd64", "azcopy_windows_386.exe", "azcopy_windows_amd64.exe"]
EXECUTABLES_TO_TAR = ["azcopy_linux_amd64", "azcopy_linux_arm64"]


def create_directory(dir):
    os.mkdir(dir)


def remove_directory(dir):
    shutil.rmtree(dir)


def copy_file(src, dst):
    shutil.copy(src, dst)


def rename_file(src, dst):
    shutil.move(src, dst)


def tar_dir(dst, src, cwd):
    check_call(["tar", "--exclude='*.DS_Store'", "-czvf", dst, src], cwd=cwd)


def zip_dir(dst, src, cwd):
    check_call(["zip", "-r", "-X", "-x='*.DS_Store'", dst, src], cwd=cwd)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create packages for AzCopyV10")
    parser.add_argument("--version", "-v", help="The version of the package", default="10.0.0")
    parser.add_argument("--input-folder", "-i", help="Where the executables are located", default=DEFAULT_SOURCE_FOLDER)
    parser.add_argument("--output-folder", "-o", help="Whether the unit tests should run", default=DEFAULT_DESTINATION_FOLDER)

    # step 1: parse the command line arguments
    args = parser.parse_args()
    print("Starting package generation: version={0}, input folder={1}, output folder={2}"
          .format(args.version, args.input_folder, args.output_folder))

    # step 2: delete output folder if present
    if os.path.exists(args.output_folder):
        print("Deleting existing output folder: " + args.output_folder)
        remove_directory(args.output_folder)

    # step 3: create package for each environment
    print("Creating output folder: " + args.output_folder)
    create_directory(args.output_folder)
    for executable in EXECUTABLES_TO_ZIP + EXECUTABLES_TO_TAR:
        output_folder_name = "{}_{}".format(executable.replace('.exe', ''), args.version)
        output_folder_path = os.path.join(args.output_folder, output_folder_name)

        # each executable should be in a different folder
        create_directory(output_folder_path)

        # copy the executable into the right folder
        copy_file(os.path.join(args.input_folder, executable), output_folder_path)

        # rename executables to the standard name
        rename_file(os.path.join(output_folder_path, executable), os.path.join(output_folder_path, "azcopy.exe" if ".exe" in executable else "azcopy"))

        # copy the third party notice over
        copy_file(os.path.join(args.input_folder, THIRD_PARTY_NOTICE_FILE_NAME), output_folder_path)

        # compress the folder accordingly
        if executable in EXECUTABLES_TO_TAR:
            tar_dir("{}.tar.gz".format(output_folder_name), output_folder_name,
                    cwd=os.path.abspath(args.output_folder))
        else:
            zip_dir("{}.zip".format(output_folder_name), output_folder_name,
                    cwd=os.path.abspath(args.output_folder))

    # step 4: create version file
    with open(os.path.join(args.output_folder, "latest_version.txt"), "w+") as f:
        f.write(args.version + "\n")
