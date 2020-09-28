import unittest
import utility as util
import hashlib
import os
import shlex
import subprocess
from subprocess import CalledProcessError


class BlobPipingTests(unittest.TestCase):

    def test_piping_upload_and_download_small_file(self):
        # small means azcopy doesn't have to rotate buffers when uploading.
        # it is assumed that the default block size is much larger than 1KB.

        # create file of size 1KB
        filename = "test_1kb_blob_piping_upload.txt"
        source_file_path = util.create_test_file(filename, 1024)

        # compute source md5 to compare later
        source_file_md5 = compute_md5(source_file_path)

        # upload 1KB file using azcopy
        destination_url = util.get_resource_sas(filename)
        azcopy_cmd = util.Command("copy").add_arguments(destination_url).add_flags("from-to", "PipeBlob").string()
        self.assertTrue(execute_command_with_pipe(azcopy_cmd, source_file_to_pipe=source_file_path))

        # downloading the uploaded file
        azcopy_cmd = util.Command("copy").add_arguments(destination_url).add_flags("from-to", "BlobPipe").string()
        destination_file_path = util.test_directory_path + "/test_1kb_blob_piping_download.txt"
        self.assertTrue(execute_command_with_pipe(azcopy_cmd, destination_file_to_pipe=destination_file_path))

        # compute destination md5 to compare
        destination_file_md5 = compute_md5(destination_file_path)

        # verifying the downloaded blob
        self.assertEqual(source_file_md5, destination_file_md5)

    def test_piping_upload_and_download_large_file(self):
        # large means azcopy has to rotate buffers when uploading.

        # create file of size 8MB
        filename = "test_8mb_blob_piping_upload.txt"
        source_file_path = util.create_test_file(filename, 8 * 1024 * 1024)

        # compute source md5 to compare later
        source_file_md5 = compute_md5(source_file_path)

        # uploadfile using azcopy
        # TODO reviewers please note, this used to use a 4MB file, with a 1 KiB block size, but now we don't support block sizes
        #    smaller than 1 MB. I've compensated slightly by changing it to an 8 MB file
        destination_url = util.get_resource_sas(filename)
        azcopy_cmd = util.Command("copy").add_arguments(destination_url).add_flags("block-size-mb", '1').add_flags("from-to", "PipeBlob").string()
        self.assertTrue(execute_command_with_pipe(azcopy_cmd, source_file_to_pipe=source_file_path))

        # downloading the uploaded file
        azcopy_cmd = util.Command("copy").add_arguments(destination_url).add_flags("block-size-mb", '1').add_flags("from-to", "BlobPipe").string()
        destination_file_path = util.test_directory_path + "/test_8mb_blob_piping_download.txt"
        self.assertTrue(execute_command_with_pipe(azcopy_cmd, destination_file_to_pipe=destination_file_path))

        # compute destination md5 to compare
        destination_file_md5 = compute_md5(destination_file_path)

        # verifying the downloaded blob
        self.assertEqual(source_file_md5, destination_file_md5)


# compute the md5 of a given file
def compute_md5(file_name):
    hash_md5 = hashlib.md5()
    with open(file_name, "rb") as f:
        # read in the file 4kb at a time
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def execute_command_with_pipe(command, source_file_to_pipe=None, destination_file_to_pipe=None):
    """
    Run AzCopy either with the a source pipe, or with a destination pipe.
    This way of triggering AzCopy is only for this test suite.

    :param command: the azcopy command to be executed
    :param source_file_to_pipe: source file if applicable
    :param destination_file_to_pipe: destination file if applicable
    :return: bool(successful or not)
    """
    if (source_file_to_pipe is None and destination_file_to_pipe is None) or \
            (source_file_to_pipe is not None and destination_file_to_pipe is not None):
        raise ValueError("Either source is specified, or destination is specified")

    azcopy_path = os.path.join(util.test_directory_path, util.azcopy_executable_name)
    command = azcopy_path + " " + command

    # if piping a file to azcopy's stdin
    if source_file_to_pipe is not None:
        try:
            ps = subprocess.Popen(('cat', source_file_to_pipe), stdout=subprocess.PIPE)
            subprocess.check_output(shlex.split(command), stdin=ps.stdout, timeout=360)
            ps.wait(360)
            return True
        except CalledProcessError:
            return False

    # if piping azcopy's output to a file
    if destination_file_to_pipe is not None:
        with open(destination_file_to_pipe, "wb") as output, open('fake_input.txt', 'wb') as fake_input:
            # an emtpy file is used as stdin because if None was specified, then the subprocess would
            # inherit the parent's stdin pipe, this is a limitation of the subprocess package
            try:
                subprocess.check_call(shlex.split(command), stdin=fake_input, stdout=output, timeout=360)
                return True
            except CalledProcessError:
                return False
