import json
import os
import shutil
import time
import urllib
from collections import namedtuple
import utility as util
import unittest
import filecmp
import os.path

class Service_2_Service_Copy_User_Scenario(unittest.TestCase):

    def setUp(self):
        # init bucket_name
        self.bucket_name = util.get_resource_name('s2scopybucketblobblob')
        self.bucket_name_file_blob = util.get_resource_name('s2scopybucketfileblob')

    ##################################
    # Test from file to blob copy.
    ##################################
    def test_copy_single_1kb_file_from_file_to_blob(self):
        src_share_url = util.get_object_sas(util.test_s2s_src_file_account_url, self.bucket_name_file_blob)
        dst_container_url = util.get_object_sas(util.test_s2s_dst_blob_account_url, self.bucket_name_file_blob)
        self.util_test_copy_single_file_from_x_to_x(src_share_url, "File", dst_container_url, "Blob", 1)

    def test_copy_single_0kb_file_from_file_to_blob(self):
        src_share_url = util.get_object_sas(util.test_s2s_src_file_account_url, self.bucket_name_file_blob)
        dst_container_url = util.get_object_sas(util.test_s2s_dst_blob_account_url, self.bucket_name_file_blob)
        self.util_test_copy_single_file_from_x_to_x(src_share_url, "File", dst_container_url, "Blob", 0)

    def test_copy_single_63mb_file_from_file_to_blob(self):
        src_share_url = util.get_object_sas(util.test_s2s_src_file_account_url, self.bucket_name_file_blob)
        dst_container_url = util.get_object_sas(util.test_s2s_dst_blob_account_url, self.bucket_name_file_blob)
        self.util_test_copy_single_file_from_x_to_x(src_share_url, "File", dst_container_url, "Blob", 63 * 1024 * 1024)

    def test_copy_10_files_from_file_share_to_blob_container(self):
        src_share_url = util.get_object_sas(util.test_s2s_src_file_account_url, self.bucket_name_file_blob)
        dst_container_url = util.get_object_sas(util.test_s2s_dst_blob_account_url, self.bucket_name_file_blob)
        self.util_test_copy_n_files_from_x_bucket_to_x_bucket(src_share_url, "File", dst_container_url, "Blob")

    def test_copy_file_from_file_share_to_blob_container_wildcard_recursive(self):
        src_share_url = util.get_object_sas(util.test_s2s_src_file_account_url, self.bucket_name_file_blob)
        dst_container_url = util.get_object_sas(util.test_s2s_dst_blob_account_url, self.bucket_name_file_blob)
        self.util_test_copy_file_from_x_bucket_to_x_bucket_wildcard(src_share_url, "File", dst_container_url, "Blob", True)

    def test_copy_file_from_file_share_to_blob_container_wildcard_non_recursive(self):
        src_share_url = util.get_object_sas(util.test_s2s_src_file_account_url, self.bucket_name_file_blob)
        dst_container_url = util.get_object_sas(util.test_s2s_dst_blob_account_url, self.bucket_name_file_blob)
        self.util_test_copy_file_from_x_bucket_to_x_bucket_wildcard(src_share_url, "File", dst_container_url, "Blob", False)
    
    def test_copy_n_files_from_file_dir_to_blob_dir(self):
        src_share_url = util.get_object_sas(util.test_s2s_src_file_account_url, self.bucket_name_file_blob)
        dst_container_url = util.get_object_sas(util.test_s2s_dst_blob_account_url, self.bucket_name_file_blob)
        self.util_test_copy_n_files_from_x_dir_to_x_dir(src_share_url, "File", dst_container_url, "Blob")

    def test_copy_n_files_from_file_dir_to_blob_dir_wildcard_recursive(self):
        src_share_url = util.get_object_sas(util.test_s2s_src_file_account_url, self.bucket_name_file_blob)
        dst_container_url = util.get_object_sas(util.test_s2s_dst_blob_account_url, self.bucket_name_file_blob)
        self.util_test_copy_n_files_from_x_dir_to_x_dir_wildcard(src_share_url, "File", dst_container_url, "Blob", True)

    def test_copy_n_files_from_file_dir_to_blob_dir_wildcard_non_recursive(self):
        src_share_url = util.get_object_sas(util.test_s2s_src_file_account_url, self.bucket_name_file_blob)
        dst_container_url = util.get_object_sas(util.test_s2s_dst_blob_account_url, self.bucket_name_file_blob)
        self.util_test_copy_n_files_from_x_dir_to_x_dir_wildcard(src_share_url, "File", dst_container_url, "Blob", False)

    def test_copy_files_from_file_account_to_blob_account(self):
        self.util_test_copy_files_from_x_account_to_x_account(
            util.test_s2s_src_file_account_url, 
            "File", 
            util.test_s2s_dst_blob_account_url, 
            "Blob",
            self.bucket_name_file_blob)

    def test_copy_single_file_from_file_to_blob_propertyandmetadata(self):
        src_share_url = util.get_object_sas(util.test_s2s_src_file_account_url, self.bucket_name_file_blob)
        dst_container_url = util.get_object_sas(util.test_s2s_dst_blob_account_url, self.bucket_name_file_blob)
        self.util_test_copy_single_file_from_x_to_x_propertyandmetadata(
            src_share_url, 
            "File", 
            dst_container_url, 
            "Blob")

    # Test oauth support for service to service copy, where source is authenticated with SAS
    # and destination is authenticated with OAuth token.
    @unittest.skip("test env not supported.")
    def test_copy_single_17mb_file_from_file_to_blob_oauth(self):
        src_share_url = util.get_object_sas(util.test_s2s_src_file_account_url, self.bucket_name_file_blob)
        dst_container_url = util.get_object_without_sas(util.test_s2s_dst_blob_account_url, self.bucket_name_file_blob)
        self.util_test_copy_single_file_from_x_to_x(src_share_url, "File", dst_container_url, "Blob", 17 * 1024 * 1024, True)

    ##################################
    # Test from blob to blob copy.
    ##################################
    #test_copy_single_1kb_file_from_blob_to_blob verifies copy single 1kb blob to blob using azcopy.
    # def test_copy_single_1kb_file_from_blob_to_blob(self):
    #     src_container_url = util.get_object_sas(util.test_s2s_src_blob_account_url, self.bucket_name)
    #     dst_container_url = util.get_object_sas(util.test_s2s_dst_blob_account_url, self.bucket_name)
    #     self.util_test_copy_single_file_from_x_to_x(src_container_url, "Blob", dst_container_url, "Blob", 1)

    # def test_copy_single_0kb_file_from_blob_to_blob(self):
    #     src_container_url = util.get_object_sas(util.test_s2s_src_blob_account_url, self.bucket_name)
    #     dst_container_url = util.get_object_sas(util.test_s2s_dst_blob_account_url, self.bucket_name)
    #     self.util_test_copy_single_file_from_x_to_x(src_container_url, "Blob", dst_container_url, "Blob", 0)

    # def test_copy_single_63mb_file_from_blob_to_blob(self):
    #     src_container_url = util.get_object_sas(util.test_s2s_src_blob_account_url, self.bucket_name)
    #     dst_container_url = util.get_object_sas(util.test_s2s_dst_blob_account_url, self.bucket_name)
    #     self.util_test_copy_single_file_from_x_to_x(src_container_url, "Blob", dst_container_url, "Blob", 63 * 1024 * 1024)

    # def test_copy_10_files_from_blob_container_to_blob_container(self):
    #     src_container_url = util.get_object_sas(util.test_s2s_src_blob_account_url, self.bucket_name)
    #     dst_container_url = util.get_object_sas(util.test_s2s_dst_blob_account_url, self.bucket_name)
    #     self.util_test_copy_n_files_from_x_bucket_to_x_bucket(src_container_url, "Blob", dst_container_url, "Blob")

    # def test_copy_file_from_blob_container_to_blob_container_wildcard_recursive(self):
    #     src_container_url = util.get_object_sas(util.test_s2s_src_blob_account_url, self.bucket_name)
    #     dst_container_url = util.get_object_sas(util.test_s2s_dst_blob_account_url, self.bucket_name)
    #     self.util_test_copy_file_from_x_bucket_to_x_bucket_wildcard(src_container_url, "Blob", dst_container_url, "Blob", True)

    # def test_copy_file_from_blob_container_to_blob_container_wildcard_non_recursive(self):
    #     src_container_url = util.get_object_sas(util.test_s2s_src_blob_account_url, self.bucket_name)
    #     dst_container_url = util.get_object_sas(util.test_s2s_dst_blob_account_url, self.bucket_name)
    #     self.util_test_copy_file_from_x_bucket_to_x_bucket_wildcard(src_container_url, "Blob", dst_container_url, "Blob", False)
    
    # def test_copy_n_files_from_blob_dir_to_blob_dir(self):
    #     src_container_url = util.get_object_sas(util.test_s2s_src_blob_account_url, self.bucket_name)
    #     dst_container_url = util.get_object_sas(util.test_s2s_dst_blob_account_url, self.bucket_name)
    #     self.util_test_copy_n_files_from_x_dir_to_x_dir(src_container_url, "Blob", dst_container_url, "Blob")

    # def test_copy_n_files_from_blob_dir_to_blob_dir_wildcard_recursive(self):
    #     src_container_url = util.get_object_sas(util.test_s2s_src_blob_account_url, self.bucket_name)
    #     dst_container_url = util.get_object_sas(util.test_s2s_dst_blob_account_url, self.bucket_name)
    #     self.util_test_copy_n_files_from_x_dir_to_x_dir_wildcard(src_container_url, "Blob", dst_container_url, "Blob", True)

    # def test_copy_n_files_from_blob_dir_to_blob_dir_wildcard_non_recursive(self):
    #     src_container_url = util.get_object_sas(util.test_s2s_src_blob_account_url, self.bucket_name)
    #     dst_container_url = util.get_object_sas(util.test_s2s_dst_blob_account_url, self.bucket_name)
    #     self.util_test_copy_n_files_from_x_dir_to_x_dir_wildcard(src_container_url, "Blob", dst_container_url, "Blob", False)

    # def test_copy_files_from_blob_account_to_blob_account(self):
    #     self.util_test_copy_files_from_x_account_to_x_account(
    #         util.test_s2s_src_blob_account_url, 
    #         "Blob", 
    #         util.test_s2s_dst_blob_account_url, 
    #         "Blob",
    #         self.bucket_name)

    # def test_copy_single_file_from_blob_to_blob_propertyandmetadata(self):
    #     src_container_url = util.get_object_sas(util.test_s2s_src_blob_account_url, self.bucket_name)
    #     dst_container_url = util.get_object_sas(util.test_s2s_dst_blob_account_url, self.bucket_name)
    #     self.util_test_copy_single_file_from_x_to_x_propertyandmetadata(
    #         src_container_url, 
    #         "Blob", 
    #         dst_container_url, 
    #         "Blob")

    # # Test oauth support for service to service copy, where source is authenticated with SAS
    # # and destination is authenticated with OAuth token.
    # def test_copy_single_17mb_file_from_blob_to_blob_oauth(self):
    #     src_container_url = util.get_object_sas(util.test_s2s_src_blob_account_url, self.bucket_name)
    #     dst_container_url = util.get_object_without_sas(util.test_s2s_dst_blob_account_url, self.bucket_name)
    #     self.util_test_copy_single_file_from_x_to_x(src_container_url, "Blob", dst_container_url, "Blob", 17 * 1024 * 1024, True)

    ##################################
    # Test utils and reusable functions.
    ##################################
    # common testing utils for service to service copy.
    def util_are_dir_trees_equal(self, dir1, dir2):
        dirs_cmp = filecmp.dircmp(dir1, dir2)
        if len(dirs_cmp.left_only)>0 or len(dirs_cmp.right_only)>0 or \
            len(dirs_cmp.funny_files)>0:
            return False
        (_, mismatch, errors) =  filecmp.cmpfiles(
            dir1, dir2, dirs_cmp.common_files, shallow=False)
        if len(mismatch)>0 or len(errors)>0:
            return False
        for common_dir in dirs_cmp.common_dirs:
            new_dir1 = os.path.join(dir1, common_dir)
            new_dir2 = os.path.join(dir2, common_dir)
            if not self.util_are_dir_trees_equal(new_dir1, new_dir2):
                return False
        return True

    def util_test_copy_single_file_from_x_to_x(
        self,
        srcBucketURL,
        srcType,
        dstBucketURL,
        dstType,
        sizeInKB=1,
        oAuth=False):
        # create source bucket
        result = util.Command("create").add_arguments(srcBucketURL).add_flags("serviceType", srcType). \
            add_flags("resourceType", "Bucket").execute_azcopy_create()
        self.assertTrue(result)

        # create file of size 1KB.
        filename = "test_" + str(sizeInKB) + "kb_copy.txt"
        file_path = util.create_test_file(filename, sizeInKB)
        srcFileURL = util.get_object_sas(srcBucketURL, filename)
        if oAuth:
            dstFileURL = util.get_object_without_sas(dstBucketURL, filename)
        else:
            dstFileURL = util.get_object_sas(dstBucketURL, filename)
        

        # Upload file using azcopy.
        # TODO: Note for S3/Google need special logic
        result = util.Command("copy").add_arguments(file_path).add_arguments(srcFileURL). \
            add_flags("log-level", "info").execute_azcopy_copy_command()
        self.assertTrue(result)

        # Copy file using azcopy from srcURL to destURL
        result = util.Command("copy").add_arguments(srcFileURL).add_arguments(dstFileURL). \
            add_flags("log-level", "info").execute_azcopy_copy_command()
        self.assertTrue(result)

        # Downloading the copied file for validation
        validate_dir_name = "validate_copy_single_%dKB_file_from_%s_to_%s" % (sizeInKB, srcType, dstType)
        local_validate_dest_dir = util.create_test_dir(validate_dir_name)
        local_validate_dest = os.path.join(local_validate_dest_dir, filename)
        result = util.Command("copy").add_arguments(dstFileURL).add_arguments(local_validate_dest). \
            add_flags("log-level", "info").execute_azcopy_copy_command()
        self.assertTrue(result)

        # Verifying the downloaded blob
        result = filecmp.cmp(file_path, local_validate_dest, shallow=False)
        self.assertTrue(result)

        # clean up both source and destination bucket
        # util.Command("clean").add_arguments(srcBucketURL).add_flags("serviceType", srcType). \
        #     add_flags("resourceType", "Bucket").execute_azcopy_create()

        # util.Command("clean").add_arguments(dstBucketURL).add_flags("serviceType", dstType). \
        #     add_flags("resourceType", "Bucket").execute_azcopy_create()

    def util_test_copy_n_files_from_x_bucket_to_x_bucket(
        self,
        srcBucketURL,
        srcType,
        dstBucketURL,
        dstType,
        n=10,
        sizeInKB=1):
        # create source bucket
        result = util.Command("create").add_arguments(srcBucketURL).add_flags("serviceType", srcType). \
            add_flags("resourceType", "Bucket").execute_azcopy_create()
        self.assertTrue(result)

        # create file of size n KBs in newly created directory.
        src_dir_name = "copy_%d_%dKB_files_from_%s_bucket_to_%s_bucket" % (n, sizeInKB, srcType, dstType)
        src_dir_path = util.create_test_n_files(sizeInKB*1024, n, src_dir_name)

        # Upload files using azcopy.
        # TODO: Note for S3/Google need special logic
        result = util.Command("copy").add_arguments(src_dir_path).add_arguments(srcBucketURL). \
            add_flags("log-level", "info").add_flags("recursive", "true").execute_azcopy_copy_command()
        self.assertTrue(result)

        # Copy files using azcopy from srcURL to destURL
        result = util.Command("copy").add_arguments(srcBucketURL).add_arguments(dstBucketURL). \
            add_flags("log-level", "info").add_flags("recursive", "true").execute_azcopy_copy_command()
        self.assertTrue(result)

        # Downloading the copied files for validation
        validate_dir_name = "validate_copy_%d_%dKB_files_from_%s_bucket_to_%s_bucket" % (n, sizeInKB, srcType, dstType)
        local_validate_dest = util.create_test_dir(validate_dir_name)
        dst_directory_url = util.get_object_sas(dstBucketURL, src_dir_name)
        result = util.Command("copy").add_arguments(dst_directory_url).add_arguments(local_validate_dest). \
            add_flags("log-level", "info").add_flags("recursive", "true").execute_azcopy_copy_command()
        self.assertTrue(result)

        # Verifying the downloaded blob
        result = self.util_are_dir_trees_equal(src_dir_path, os.path.join(local_validate_dest, src_dir_name))
        self.assertTrue(result)

        # clean up both source and destination bucket
        # util.Command("clean").add_arguments(srcBucketURL).add_flags("serviceType", srcType). \
        #     add_flags("resourceType", "Bucket").execute_azcopy_create()

        # util.Command("clean").add_arguments(dstBucketURL).add_flags("serviceType", dstType). \
        #     add_flags("resourceType", "Bucket").execute_azcopy_create()

    def util_test_copy_file_from_x_bucket_to_x_bucket_wildcard(
        self,
        srcBucketURL,
        srcType,
        dstBucketURL,
        dstType,
        recursive=True):
        # create source bucket
        result = util.Command("create").add_arguments(srcBucketURL).add_flags("serviceType", srcType). \
            add_flags("resourceType", "Bucket").execute_azcopy_create()
        self.assertTrue(result)

        # create file.
        filename = "copy_wildcard_file.txt"
        file_path = util.create_test_file(filename, 1)
        srcFileURL = util.get_object_sas(srcBucketURL, filename)
        srcFileWildcardURL = srcFileURL.replace(filename, "*")

        # Upload file using azcopy.
        # TODO: Note for S3/Google need special logic
        result = util.Command("copy").add_arguments(file_path).add_arguments(srcFileURL). \
            add_flags("log-level", "info").execute_azcopy_copy_command()
        self.assertTrue(result)

        # Copy file using azcopy from srcURL to destURL
        if recursive:
            result = util.Command("copy").add_arguments(srcFileWildcardURL).add_arguments(dstBucketURL). \
                add_flags("log-level", "info").add_flags("recursive", "true").execute_azcopy_copy_command()
        else:
            result = util.Command("copy").add_arguments(srcFileWildcardURL).add_arguments(dstBucketURL). \
                add_flags("log-level", "info").execute_azcopy_copy_command()
        self.assertTrue(result)

        # Downloading the copied files for validation
        validate_dir_name = "validate_copy_file_from_%s_bucket_to_%s_bucket_wildcard_recursive_%s" % (srcType, dstType, recursive)
        local_validate_dest = util.create_test_dir(validate_dir_name)
        dst_file_url = util.get_object_sas(dstBucketURL, filename)
        result = util.Command("copy").add_arguments(dst_file_url).add_arguments(local_validate_dest). \
            add_flags("log-level", "info").add_flags("recursive", "true").execute_azcopy_copy_command()
        self.assertTrue(result)

        # Verifying the downloaded file
        result = filecmp.cmp(file_path, os.path.join(local_validate_dest, filename), shallow=False)
        self.assertTrue(result)

        # clean up both source and destination bucket
        # util.Command("clean").add_arguments(srcBucketURL).add_flags("serviceType", srcType). \
        #     add_flags("resourceType", "Bucket").execute_azcopy_create()

        # util.Command("clean").add_arguments(dstBucketURL).add_flags("serviceType", dstType). \
        #     add_flags("resourceType", "Bucket").execute_azcopy_create()

    # TODO: ensure this scenario, when copy from directory to directory, src directory will be created in dest directory
    # this is similar for blob download/upload.       
    def util_test_copy_n_files_from_x_dir_to_x_dir(self,
        srcBucketURL,
        srcType,
        dstBucketURL,
        dstType,
        n=10,
        sizeInKB=1):
        # create source bucket
        result = util.Command("create").add_arguments(srcBucketURL).add_flags("serviceType", srcType). \
            add_flags("resourceType", "Bucket").execute_azcopy_create()
        self.assertTrue(result)

        # create file of size n KBs in newly created directory.
        src_dir_name = "copy_%d_%dKB_files_from_%s_dir_to_%s_dir" % (n, sizeInKB, srcType, dstType)
        src_dir_path = util.create_test_n_files(sizeInKB*1024, n, src_dir_name)

        # Upload files using azcopy.
        # TODO: Note for S3/Google need special logic
        result = util.Command("copy").add_arguments(src_dir_path).add_arguments(srcBucketURL). \
            add_flags("log-level", "info").add_flags("recursive", "true").execute_azcopy_copy_command()
        self.assertTrue(result)

        srcDirURL = util.get_object_sas(srcBucketURL, src_dir_name)
        dstDirURL = util.get_object_sas(dstBucketURL, src_dir_name)
        # Copy files using azcopy from srcURL to destURL
        result = util.Command("copy").add_arguments(srcDirURL).add_arguments(dstDirURL). \
            add_flags("log-level", "info").add_flags("recursive", "true").execute_azcopy_copy_command()
        self.assertTrue(result)

        # Downloading the copied files for validation
        validate_dir_name = "validate_copy_%d_%dKB_files_from_%s_dir_to_%s_dir" % (n, sizeInKB, srcType, dstType)
        local_validate_dest = util.create_test_dir(validate_dir_name)
        result = util.Command("copy").add_arguments(dstDirURL).add_arguments(local_validate_dest). \
            add_flags("log-level", "info").add_flags("recursive", "true").execute_azcopy_copy_command()
        self.assertTrue(result)

        # Verifying the downloaded blob
        # here is the special behavior need confirm
        result = self.util_are_dir_trees_equal(src_dir_path, os.path.join(local_validate_dest, src_dir_name, src_dir_name))
        #result = self.util_are_dir_trees_equal(src_dir_path, local_validate_dest)
        self.assertTrue(result)

        # clean up both source and destination bucket
        # util.Command("clean").add_arguments(srcBucketURL).add_flags("serviceType", srcType). \
        #     add_flags("resourceType", "Bucket").execute_azcopy_create()

        # util.Command("clean").add_arguments(dstBucketURL).add_flags("serviceType", dstType). \
        #     add_flags("resourceType", "Bucket").execute_azcopy_create()

    def util_test_copy_n_files_from_x_dir_to_x_dir_wildcard(self,
        srcBucketURL,
        srcType,
        dstBucketURL,
        dstType,
        n=10,
        sizeInKB=1,
        recursive=True):
        # create source bucket
        result = util.Command("create").add_arguments(srcBucketURL).add_flags("serviceType", srcType). \
            add_flags("resourceType", "Bucket").execute_azcopy_create()
        self.assertTrue(result)

        # create file of size n KBs in newly created directory.
        src_dir_name = "copy_%d_%dKB_files_from_%s_dir_to_%s_dir_recursive_%s" % (n, sizeInKB, srcType, dstType, recursive)
        src_dir_path = util.create_test_n_files(sizeInKB*1024, n, src_dir_name)
        src_sub_dir_name = src_dir_name + "/" + "subdir"
        util.create_test_n_files(sizeInKB*1024,1, src_sub_dir_name)

        # Upload files using azcopy.
        # TODO: Note for S3/Google need special logic
        result = util.Command("copy").add_arguments(src_dir_path).add_arguments(srcBucketURL). \
            add_flags("log-level", "info").add_flags("recursive", "true").execute_azcopy_copy_command()
        self.assertTrue(result)

        srcDirWildcardURL = util.get_object_sas(srcBucketURL, src_dir_name+"/*")
        dstDirURL = util.get_object_sas(dstBucketURL, src_dir_name)
        if recursive:
            # Copy files using azcopy from srcURL to destURL
            result = util.Command("copy").add_arguments(srcDirWildcardURL).add_arguments(dstDirURL). \
                add_flags("log-level", "info").add_flags("recursive", "true").execute_azcopy_copy_command()
            self.assertTrue(result)
        else:
            # Copy files using azcopy from srcURL to destURL
            result = util.Command("copy").add_arguments(srcDirWildcardURL).add_arguments(dstDirURL). \
                add_flags("log-level", "info").execute_azcopy_copy_command()
            self.assertTrue(result) 

        # Downloading the copied files for validation
        validate_dir_name = "validate_copy_%d_%dKB_files_from_%s_dir_to_%s_dir" % (n, sizeInKB, srcType, dstType)
        local_validate_dest = util.create_test_dir(validate_dir_name)
        result = util.Command("copy").add_arguments(dstDirURL).add_arguments(local_validate_dest). \
            add_flags("log-level", "info").add_flags("recursive", "true").execute_azcopy_copy_command()
        self.assertTrue(result)

        # Verifying the downloaded blob
        # here is the special behavior need confirm
        if recursive:
            result = self.util_are_dir_trees_equal(src_dir_path, os.path.join(local_validate_dest, src_dir_name))
        else:
            dirs_cmp = filecmp.dircmp(src_dir_path, os.path.join(local_validate_dest, src_dir_name))
            if len(dirs_cmp.left_only) > 0 and len(dirs_cmp.common_files) == n:
                result = True
            else:
                result = False
        #result = self.util_are_dir_trees_equal(src_dir_path, local_validate_dest)
        self.assertTrue(result)

        # clean up both source and destination bucket
        # util.Command("clean").add_arguments(srcBucketURL).add_flags("serviceType", srcType). \
        #     add_flags("resourceType", "Bucket").execute_azcopy_create()

        # util.Command("clean").add_arguments(dstBucketURL).add_flags("serviceType", dstType). \
        #     add_flags("resourceType", "Bucket").execute_azcopy_create()
        
    def util_test_copy_files_from_x_account_to_x_account(self,
        srcAccountURL,
        srcType,
        dstAccountURL,
        dstType,
        bucketName):
        src_bucket_url = util.get_object_sas(srcAccountURL, bucketName)
        validate_dst_container_url = util.get_object_sas(dstAccountURL, bucketName)

        # create source bucket
        result = util.Command("create").add_arguments(src_bucket_url).add_flags("serviceType", srcType). \
            add_flags("resourceType", "Bucket").execute_azcopy_create()
        self.assertTrue(result)

        # create files of size n KBs.
        src_dir_name = "copy_files_from_%s_account_to_%s_account" % (srcType, dstType)
        src_dir_path = util.create_test_n_files(1*1024, 100, src_dir_name)

        # Upload files using azcopy.
        # TODO: Note for S3/Google need special logic
        result = util.Command("copy").add_arguments(src_dir_path).add_arguments(src_bucket_url). \
            add_flags("log-level", "info").add_flags("recursive", "true").execute_azcopy_copy_command()
        self.assertTrue(result)

        # Copy files using azcopy from srcURL to destURL
        result = util.Command("copy").add_arguments(srcAccountURL).add_arguments(dstAccountURL). \
            add_flags("log-level", "info").add_flags("recursive", "true").execute_azcopy_copy_command()
        self.assertTrue(result)

        # Downloading the copied files for validation
        validate_dir_name = "validate_copy_files_from_%s_account_to_%s_account" % (srcType, dstType)
        local_validate_dest = util.create_test_dir(validate_dir_name)
        dst_container_url = util.get_object_sas(dstAccountURL, bucketName)
        dst_directory_url = util.get_object_sas(dst_container_url, src_dir_name)
        result = util.Command("copy").add_arguments(dst_directory_url).add_arguments(local_validate_dest). \
            add_flags("log-level", "info").add_flags("recursive", "true").execute_azcopy_copy_command()
        self.assertTrue(result)

        # Verifying the downloaded blob
        result = self.util_are_dir_trees_equal(src_dir_path, os.path.join(local_validate_dest, src_dir_name))
        self.assertTrue(result)

        # clean up both source and destination bucket
        # util.Command("clean").add_arguments(src_bucket_url).add_flags("serviceType", srcType). \
        #     add_flags("resourceType", "Bucket").execute_azcopy_create()

        # util.Command("clean").add_arguments(validate_dst_container_url).add_flags("serviceType", dstType). \
        #     add_flags("resourceType", "Bucket").execute_azcopy_create()

    def util_test_copy_single_file_from_x_to_x_propertyandmetadata(
        self,
        srcBucketURL,
        srcType,
        dstBucketURL,
        dstType):
        # create bucket and create file with metadata and properties
        result = util.Command("create").add_arguments(srcBucketURL).add_flags("serviceType", srcType). \
            add_flags("resourceType", "Bucket").execute_azcopy_create()
        self.assertTrue(result)

        fileName = "single_file_propertyandmetadata"
        srcFileURL = util.get_object_sas(srcBucketURL, fileName)
        dstFileURL = util.get_object_sas(dstBucketURL, fileName)
        result = util.Command("create").add_arguments(srcFileURL).add_flags("serviceType", srcType). \
            add_flags("resourceType", "SingleFile"). \
            add_flags("metadata", "author=jiac;viewport=width;description=test file"). \
            add_flags("content-type", "testctype").add_flags("content-encoding", "testenc"). \
            add_flags("content-disposition", "testcdis").add_flags("content-language", "testclang").\
            add_flags("cache-control", "testcc").\
            execute_azcopy_create()
        self.assertTrue(result)

        # Copy file using azcopy from srcURL to destURL
        result = util.Command("copy").add_arguments(srcFileURL).add_arguments(dstFileURL). \
            add_flags("log-level", "info").execute_azcopy_copy_command()
        self.assertTrue(result)

        # Downloading the copied file for validation
        validate_dir_name = "validate_copy_single_file_from_%s_to_%s_propertyandmetadata" % (srcType, dstType)
        local_validate_dest_dir = util.create_test_dir(validate_dir_name)
        local_validate_dest = local_validate_dest_dir + fileName
        result = util.Command("copy").add_arguments(srcFileURL).add_arguments(local_validate_dest). \
            add_flags("log-level", "info").execute_azcopy_copy_command()
        self.assertTrue(result)

        # TODO: test different targets according to dstType
        result = util.Command("testBlob").add_arguments(local_validate_dest).add_arguments(dstFileURL). \
        add_flags("metadata", "author=jiac;viewport=width;description=test file"). \
        add_flags("content-type", "testctype").add_flags("content-encoding", "testenc"). \
        add_flags("content-disposition", "testcdis").add_flags("content-language", "testclang"). \
        add_flags("cache-control", "testcc").add_flags("check-content-md5", "true"). \
        add_flags("no-guess-mime-type", "true").execute_azcopy_verify()
        self.assertTrue(result)
    
    # TODO def util_test_copy_n_files_form_x_bucket_to_x_bucket_resume(self):
