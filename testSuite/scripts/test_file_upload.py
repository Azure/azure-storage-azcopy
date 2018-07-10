import utility as util
import os
import unittest

class FileShare_Upload_User_Scenario(unittest.TestCase):
    # test_file_upload_1mb verifies the azcopy upload of 1mb file as an Azure file with full file name.
    def test_file_upload_1mb_fullname(self):
        # create the test file.
        file_name = "test_file_upload_1mb_fullname.vhd"
        file_path = util.create_test_file(file_name, 1024 * 1024)

        # execute azcopy upload.
        destination = util.get_resource_sas_from_share(file_name)
        result = util.Command("copy").add_arguments(file_path).add_arguments(destination).add_flags("log-level", "info"). \
            add_flags("block-size", "4194304").execute_azcopy_copy_command()
        self.assertTrue(result)

        # execute validator.
        result = util.Command("testFile").add_arguments(file_path).add_arguments(destination).execute_azcopy_verify()
        self.assertTrue(result)


    def test_file_upload_1mb_wildcard(self):
        # create the test file.
        file_name = "test_file_upload_1mb_wildcard.vhd"
        file_path = util.create_test_file(file_name, 1024 * 1024)

        # execute azcopy upload.
        destination = util.get_resource_sas_from_share(file_name)
        wildcard_path = file_path.replace(file_name, "*")
        result = util.Command("copy").add_arguments(wildcard_path).add_arguments(destination).add_flags("log-level", "info"). \
            add_flags("block-size", "4194304").execute_azcopy_copy_command()
        self.assertTrue(result)

        # execute validator.
        result = util.Command("testFile").add_arguments(file_path).add_arguments(destination).execute_azcopy_verify()
        self.assertTrue(result)


    # test_file_range_for_complete_sparse_file verifies the number of ranges for
    # complete empty file i.e each character is Null character.
    def test_file_range_for_complete_sparse_file(self):
        # create test file.
        file_name = "sparse_file.vhd"
        file_path = util.create_complete_sparse_file(file_name, 4 * 1024 * 1024)

        # execute azcopy file upload.
        destination_sas = util.get_resource_sas_from_share(file_name)
        result = util.Command("copy").add_arguments(file_path).add_arguments(destination_sas).add_flags("log-level", "info"). \
            add_flags("block-size", "4194304").execute_azcopy_copy_command()
        self.assertTrue(result)

        # execute validator.
        # no of ranges should be 0 for the empty sparse file.
        result = util.Command("testFile").add_arguments(file_path).add_arguments(destination_sas).add_flags(
            "verify-block-size", "true").add_flags("number-blocks-or-pages", "0").execute_azcopy_verify()
        self.assertTrue(result)


    # test_file_upload_partial_sparse_file verifies the number of ranges
    # for azure file upload by azcopy.
    def test_file_upload_partial_sparse_file(self):
        # create test file.
        file_name = "test_partial_sparse_file.vhd"
        file_path = util.create_partial_sparse_file(file_name, 16 * 1024 * 1024)

        # execute azcopy file upload.
        destination_sas = util.get_resource_sas_from_share(file_name)
        result = util.Command("copy").add_arguments(file_path).add_arguments(destination_sas).add_flags("log-level", "info"). \
            add_flags("block-size", "4194304").execute_azcopy_copy_command()
        self.assertTrue(result)

        # number of range for partial sparse created above will be (size/2)
        number_of_ranges = int((16 * 1024 * 1024 / (4 * 1024 * 1024)) / 2)
        # execute validator to verify the number of range for uploaded file.
        result = util.Command("testFile").add_arguments(file_path).add_arguments(destination_sas). \
            add_flags("verify-block-size", "true"). \
            add_flags("number-blocks-or-pages", str(number_of_ranges)).execute_azcopy_verify()
        self.assertTrue(result)

    # n_1kb_file_in_dir_upload_to_share verifies the upload of n 1kb file to the share.
    def n_1kb_file_in_dir_upload_to_share(self, number_of_files):
        # create dir dir_n_files and 1 kb files inside the dir.
        dir_name = "dir_" + str(number_of_files) + "_files"
        dir_n_files_path = util.create_test_n_files(1024, number_of_files, dir_name)
        sub_dir_name = "dir subdir_" + str(number_of_files) + "_files"
        dir_n_files_path = util.create_test_n_files(1024, number_of_files, os.path.join(dir_name, sub_dir_name))

        # execute azcopy command
        result = util.Command("copy").add_arguments(dir_n_files_path).add_arguments(util.test_share_url). \
            add_flags("recursive", "true").add_flags("log-level", "info").execute_azcopy_copy_command()
        if not result:
            return result

        # execute the validator.
        destination = util.get_resource_sas_from_share(dir_name)
        result = util.Command("testFile").add_arguments(dir_n_files_path).add_arguments(destination). \
            add_flags("is-object-dir", "true").execute_azcopy_verify()
        return result

    def test_6_1kb_file_in_dir_upload_to_share(self):
        self.assertTrue(self.n_1kb_file_in_dir_upload_to_share(6))

    # n_1kb_file_in_dir_upload_to_azure_directory verifies the upload of n 1kb file to the share.
    def n_1kb_file_in_dir_upload_to_azure_directory(self, number_of_files, recursive):
        # create dir dir_n_files and 1 kb files inside the dir.
        dir_name = "dir_" + str(number_of_files) + "_files"
        dir_n_files_path = util.create_test_n_files(1024, number_of_files, dir_name)
        sub_dir_name = "dir_subdir_" + str(number_of_files) + "_files"
        dir_n_files_path = util.create_test_n_files(1024, number_of_files, os.path.join(dir_name, sub_dir_name))

        dest_dir_name = "dest_dirname"
        directory_url = util.get_resource_sas_from_share(dest_dir_name)

        # execute azcopy command
        result = util.Command("copy").add_arguments(dir_n_files_path).add_arguments(directory_url). \
            add_flags("recursive", recursive).add_flags("log-level", "info").execute_azcopy_copy_command()
        if not result:
            return result

        # execute the validator.
        destination = util.get_resource_sas_from_share(dir_name + "/" + dest_dir_name)
        result = util.Command("testFile").add_arguments(dir_n_files_path).add_arguments(destination). \
            add_flags("is-object-dir", "true").add_flags("is-recursive", recursive).execute_azcopy_verify()
        return result

    def test_3_1kb_file_in_dir_upload_to_azure_directory_recursive(self):
        self.assertTrue(self.n_1kb_file_in_dir_upload_to_azure_directory(3, "true"))


    def test_8_1kb_file_in_dir_upload_to_azure_directory_non_recursive(self):
        self.assertTrue(self.n_1kb_file_in_dir_upload_to_azure_directory(8, "false"))

    # test_metaData_content_encoding_content_type verifies the meta data, content type,
    # content encoding of 2kb upload to share through azcopy.
    def test_metaData_content_encoding_content_type(self):
        # create 2kb file test_mcect.txt
        filename = "test_mcect.txt"
        file_path = util.create_test_file(filename, 2048)

        # execute azcopy upload command.
        destination_sas = util.get_resource_sas_from_share(filename)
        result = util.Command("copy").add_arguments(file_path).add_arguments(destination_sas). \
            add_flags("log-level", "info").add_flags("recursive", "true").add_flags("metadata",
                                                                                  "author=jiac;viewport=width;description=test file"). \
            add_flags("content-type", "testctype").add_flags("content-encoding", "testenc").add_flags("no-guess-mime-type",
                                                                                                      "true").execute_azcopy_copy_command()
        self.assertTrue(result)

        # execute azcopy validate order.
        # adding the source in validator as first argument.
        # adding the destination in validator as second argument.
        result = util.Command("testFile").add_arguments(file_path).add_arguments(destination_sas).add_flags("metadata",
                                                                                                            "author=jiac;viewport=width;description=test file"). \
            add_flags("content-type", "testctype").add_flags("content-encoding", "testenc").add_flags("no-guess-mime-type",
                                                                                                      "true").execute_azcopy_verify()
        self.assertTrue(result)


    # test_guess_mime_type verifies the mime type detection by azcopy while uploading the file
    def test_guess_mime_type(self):
        # create a test html file
        filename = "test_guessmimetype.html"
        file_path = util.create_test_html_file(filename)

        # execute azcopy upload of html file.
        destination_sas = util.get_resource_sas_from_share(filename)
        result = util.Command("copy").add_arguments(file_path).add_arguments(destination_sas).add_flags("log-level", "info"). \
            add_flags("recursive", "true").execute_azcopy_copy_command()
        self.assertTrue(result)

        # execute the validator to verify the content-type.
        result = util.Command("testFile").add_arguments(file_path).add_arguments(destination_sas).add_flags("log-level",
                                                                                                            "info"). \
            add_flags("recursive", "true")
        self.assertTrue(result)


    # test_9mb_file_upload verifies the azcopy upload of 9mb file upload.
    def test_9mb_file_upload(self):
        # creating file of 8mb size.
        filename = "test_9mb_file_upload.txt"
        file_path = util.create_test_file(filename, 9 * 1024 * 1024)

        # execute azcopy copy upload.
        dest = util.get_resource_sas(filename)
        result = util.Command("copy").add_arguments(file_path).add_arguments(dest) \
            .add_flags("log-level", "info").add_flags("block-size", "104857600").add_flags("recursive", "true"). \
            execute_azcopy_copy_command()
        self.assertTrue(result)

        # Verifying the uploaded file
        # calling the testFile validator to verify whether file has been successfully uploaded or not
        result = util.Command("testFile").add_arguments(file_path).add_arguments(dest).execute_azcopy_verify()
        self.assertTrue(result)


    # test_1G_file_upload verifies the azcopy upload of 1Gb file upload in blocks of 100 Mb
    def test_1GB_file_upload(self):
        # create 1Gb file
        filename = "test_1G_file.txt"
        file_path = util.create_test_file(filename, 1 * 1024 * 1024 * 1024)

        # execute azcopy upload.
        destination_sas = util.get_resource_sas_from_share(filename)
        result = util.Command("copy").add_arguments(file_path).add_arguments(destination_sas).add_flags("log-level", "info"). \
            add_flags("block-size", "104857600").add_flags("recursive", "true").execute_azcopy_copy_command()
        self.assertTrue(result)

        # Verifying the uploaded file.
        # adding local file path as first argument.
        # adding file sas as local argument.
        # calling the testFile validator to verify whether file has been successfully uploaded or not.
        result = util.Command("testFile").add_arguments(file_path).add_arguments(destination_sas).execute_azcopy_verify()
        self.assertTrue(result)
