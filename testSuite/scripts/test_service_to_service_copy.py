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
        common_prefix = 's2scopybucket'

        # using different bucket_name to help to troubleshoot testing when checking real buckets
        self.bucket_name = util.get_resource_name(common_prefix + 'blobblob')
        self.bucket_name_blob_file = util.get_resource_name(common_prefix + 'blobfile')
        self.bucket_name_file_blob = util.get_resource_name(common_prefix + 'fileblob')
        self.bucket_name_s3_blob = util.get_resource_name(common_prefix + 's3blob')
        self.bucket_name_block_append_page = util.get_resource_name(common_prefix + 'blockappendpage')

    ##################################
    # Test from blob to blob copy.
    ##################################
    def test_copy_single_1kb_file_from_blob_to_blob_with_auth_env_var(self):
        src_container_url = util.get_object_sas(util.test_s2s_src_blob_account_url, self.bucket_name)
        dst_container_url = util.get_object_sas(util.test_s2s_dst_blob_account_url, self.bucket_name)
        self.util_test_copy_single_file_from_x_to_x(src_container_url, "Blob", dst_container_url, "Blob", 1,
                                                    oAuth=True, credTypeOverride="OAuthToken")
    # Test oauth support for service to service copy, where source is authenticated with SAS
    # and destination is authenticated with OAuth token.
    @unittest.skip("covered by blob to blob")
    def test_copy_single_17mb_file_from_file_to_blob_oauth(self):
        src_share_url = util.get_object_sas(util.test_s2s_src_file_account_url, self.bucket_name_file_blob)
        dst_container_url = util.get_object_without_sas(util.test_s2s_dst_blob_account_url, self.bucket_name_file_blob)
        self.util_test_copy_single_file_from_x_to_x(src_share_url, "File", dst_container_url, "Blob", 17 * 1024 * 1024, True)

    def test_copy_files_from_blob_account_to_blob_account(self):
        self.util_test_copy_files_from_x_account_to_x_account(
            util.test_s2s_src_blob_account_url,
            "Blob",
            util.test_s2s_dst_blob_account_url,
            "Blob",
            self.bucket_name)

    ##################################
    # Test from blob to file copy
    # Note: tests go from dst blob to src file to avoid the extra config-- Ze's suggestion
    ##################################

    def test_copy_files_from_file_account_to_blob_account(self):
        self.util_test_copy_files_from_x_account_to_x_account(
            util.test_s2s_src_file_account_url,
            "File",
            util.test_s2s_dst_blob_account_url,
            "Blob",
            self.bucket_name_file_blob)

    ##################################
    # Test from S3 to blob copy.
    ##################################
    def test_copy_single_1kb_file_from_s3_to_blob(self):
        if 'S3_TESTS_OFF' in os.environ and os.environ['S3_TESTS_OFF'] != "":
            self.skipTest('S3 testing is disabled for this smoke test run.')
        src_bucket_url = util.get_object_without_sas(util.test_s2s_src_s3_service_url, self.bucket_name_s3_blob)
        dst_container_url = util.get_object_sas(util.test_s2s_dst_blob_account_url, self.bucket_name_s3_blob)
        self.util_test_copy_single_file_from_x_to_x(src_bucket_url, "S3", dst_container_url, "Blob", 1)

    def test_copy_single_0kb_file_from_s3_to_blob(self):
        if 'S3_TESTS_OFF' in os.environ and os.environ['S3_TESTS_OFF'] != "":
            self.skipTest('S3 testing is disabled for this smoke test run.')
        src_bucket_url = util.get_object_without_sas(util.test_s2s_src_s3_service_url, self.bucket_name_s3_blob)
        dst_container_url = util.get_object_sas(util.test_s2s_dst_blob_account_url, self.bucket_name_s3_blob)
        self.util_test_copy_single_file_from_x_to_x(src_bucket_url, "S3", dst_container_url, "Blob", 0)

    def test_copy_single_63mb_file_from_s3_to_blob(self):
        if 'S3_TESTS_OFF' in os.environ and os.environ['S3_TESTS_OFF'] != "":
            self.skipTest('S3 testing is disabled for this smoke test run.')
        src_bucket_url = util.get_object_without_sas(util.test_s2s_src_s3_service_url, self.bucket_name_s3_blob)
        dst_container_url = util.get_object_sas(util.test_s2s_dst_blob_account_url, self.bucket_name_s3_blob)
        self.util_test_copy_single_file_from_x_to_x(src_bucket_url, "S3", dst_container_url, "Blob", 63 * 1024 * 1024)

    def test_copy_10_files_from_s3_bucket_to_blob_container(self):
        if 'S3_TESTS_OFF' in os.environ and os.environ['S3_TESTS_OFF'] != "":
            self.skipTest('S3 testing is disabled for this smoke test run.')
        src_bucket_url = util.get_object_without_sas(util.test_s2s_src_s3_service_url, self.bucket_name_s3_blob)
        dst_container_url = util.get_object_sas(util.test_s2s_dst_blob_account_url, self.bucket_name_s3_blob)
        self.util_test_copy_n_files_from_x_bucket_to_x_bucket(src_bucket_url, "S3", dst_container_url, "Blob")

    def test_copy_10_files_from_s3_bucket_to_blob_account(self):
        if 'S3_TESTS_OFF' in os.environ and os.environ['S3_TESTS_OFF'] != "":
            self.skipTest('S3 testing is disabled for this smoke test run.')
        src_bucket_url = util.get_object_without_sas(util.test_s2s_src_s3_service_url, self.bucket_name_s3_blob)
        self.util_test_copy_n_files_from_s3_bucket_to_blob_account(src_bucket_url, util.test_s2s_dst_blob_account_url)

    def test_copy_file_from_s3_bucket_to_blob_container_strip_top_dir_recursive(self):
        if 'S3_TESTS_OFF' in os.environ and os.environ['S3_TESTS_OFF'] != "":
            self.skipTest('S3 testing is disabled for this smoke test run.')
        src_bucket_url = util.get_object_without_sas(util.test_s2s_src_s3_service_url, self.bucket_name_s3_blob)
        dst_container_url = util.get_object_sas(util.test_s2s_dst_blob_account_url, self.bucket_name_s3_blob)
        self.util_test_copy_file_from_x_bucket_to_x_bucket_strip_top_dir(src_bucket_url, "S3", dst_container_url, "Blob", True)

    def test_copy_file_from_s3_bucket_to_blob_container_strip_top_dir_non_recursive(self):
        if 'S3_TESTS_OFF' in os.environ and os.environ['S3_TESTS_OFF'] != "":
            self.skipTest('S3 testing is disabled for this smoke test run.')
        src_bucket_url = util.get_object_without_sas(util.test_s2s_src_s3_service_url, self.bucket_name_s3_blob)
        dst_container_url = util.get_object_sas(util.test_s2s_dst_blob_account_url, self.bucket_name_s3_blob)
        self.util_test_copy_file_from_x_bucket_to_x_bucket_strip_top_dir(src_bucket_url, "S3", dst_container_url, "Blob", False)
    
    def test_copy_n_files_from_s3_dir_to_blob_dir(self):
        if 'S3_TESTS_OFF' in os.environ and os.environ['S3_TESTS_OFF'] != "":
            self.skipTest('S3 testing is disabled for this smoke test run.')
        src_bucket_url = util.get_object_without_sas(util.test_s2s_src_s3_service_url, self.bucket_name_s3_blob)
        dst_container_url = util.get_object_sas(util.test_s2s_dst_blob_account_url, self.bucket_name_s3_blob)
        self.util_test_copy_n_files_from_x_dir_to_x_dir(src_bucket_url, "S3", dst_container_url, "Blob")

    def test_copy_n_files_from_s3_dir_to_blob_dir_strip_top_dir_recursive(self):
        if 'S3_TESTS_OFF' in os.environ and os.environ['S3_TESTS_OFF'] != "":
            self.skipTest('S3 testing is disabled for this smoke test run.')
        src_bucket_url = util.get_object_without_sas(util.test_s2s_src_s3_service_url, self.bucket_name_s3_blob)
        dst_container_url = util.get_object_sas(util.test_s2s_dst_blob_account_url, self.bucket_name_s3_blob)
        self.util_test_copy_n_files_from_x_dir_to_x_dir_strip_top_dir(src_bucket_url, "S3", dst_container_url, "Blob", True)

    def test_copy_n_files_from_s3_dir_to_blob_dir_strip_top_dir_non_recursive(self):
        if 'S3_TESTS_OFF' in os.environ and os.environ['S3_TESTS_OFF'] != "":
            self.skipTest('S3 testing is disabled for this smoke test run.')
        src_bucket_url = util.get_object_without_sas(util.test_s2s_src_s3_service_url, self.bucket_name_s3_blob)
        dst_container_url = util.get_object_sas(util.test_s2s_dst_blob_account_url, self.bucket_name_s3_blob)
        self.util_test_copy_n_files_from_x_dir_to_x_dir_strip_top_dir(src_bucket_url, "S3", dst_container_url, "Blob", False)

    def test_copy_files_from_s3_service_to_blob_account(self):
        if 'S3_TESTS_OFF' in os.environ and os.environ['S3_TESTS_OFF'] != "":
            self.skipTest('S3 testing is disabled for this smoke test run.')
        self.util_test_copy_files_from_x_account_to_x_account(
            util.test_s2s_src_s3_service_url, 
            "S3", 
            util.test_s2s_dst_blob_account_url, 
            "Blob",
            self.bucket_name_s3_blob)

    def test_copy_single_file_from_s3_to_blob_propertyandmetadata(self):
        if 'S3_TESTS_OFF' in os.environ and os.environ['S3_TESTS_OFF'] != "":
            self.skipTest('S3 testing is disabled for this smoke test run.')
        src_bucket_url = util.get_object_without_sas(util.test_s2s_src_s3_service_url, self.bucket_name_s3_blob)
        dst_container_url = util.get_object_sas(util.test_s2s_dst_blob_account_url, self.bucket_name_s3_blob)
        self.util_test_copy_single_file_from_x_to_x_propertyandmetadata(
            src_bucket_url, 
            "S3", 
            dst_container_url, 
            "Blob")

    def test_copy_single_file_from_s3_to_blob_no_preserve_propertyandmetadata(self):
        if 'S3_TESTS_OFF' in os.environ and os.environ['S3_TESTS_OFF'] != "":
            self.skipTest('S3 testing is disabled for this smoke test run.')
        src_bucket_url = util.get_object_without_sas(util.test_s2s_src_s3_service_url, self.bucket_name_s3_blob)
        dst_container_url = util.get_object_sas(util.test_s2s_dst_blob_account_url, self.bucket_name_s3_blob)
        self.util_test_copy_single_file_from_x_to_x_propertyandmetadata(
            src_bucket_url, 
            "S3", 
            dst_container_url, 
            "Blob",
            False)
    
    def test_copy_file_from_s3_bucket_to_blob_container_propertyandmetadata(self):
        if 'S3_TESTS_OFF' in os.environ and os.environ['S3_TESTS_OFF'] != "":
            self.skipTest('S3 testing is disabled for this smoke test run.')
        src_bucket_url = util.get_object_without_sas(util.test_s2s_src_s3_service_url, self.bucket_name_s3_blob)
        dst_container_url = util.get_object_sas(util.test_s2s_dst_blob_account_url, self.bucket_name_s3_blob)
        self.util_test_copy_file_from_x_bucket_to_x_bucket_propertyandmetadata(
            src_bucket_url, 
            "S3", 
            dst_container_url, 
            "Blob")

    def test_copy_file_from_s3_bucket_to_blob_container_no_preserve_propertyandmetadata(self):
        if 'S3_TESTS_OFF' in os.environ and os.environ['S3_TESTS_OFF'] != "":
            self.skipTest('S3 testing is disabled for this smoke test run.')
        src_bucket_url = util.get_object_without_sas(util.test_s2s_src_s3_service_url, self.bucket_name_s3_blob)
        dst_container_url = util.get_object_sas(util.test_s2s_dst_blob_account_url, self.bucket_name_s3_blob)
        self.util_test_copy_file_from_x_bucket_to_x_bucket_propertyandmetadata(
            src_bucket_url, 
            "S3", 
            dst_container_url, 
            "Blob",
            False)

    def test_overwrite_copy_single_file_from_s3_to_blob(self):
        if 'S3_TESTS_OFF' in os.environ and os.environ['S3_TESTS_OFF'] != "":
            self.skipTest('S3 testing is disabled for this smoke test run.')
        src_bucket_url = util.get_object_without_sas(util.test_s2s_src_s3_service_url, self.bucket_name_s3_blob)
        dst_container_url = util.get_object_sas(util.test_s2s_dst_blob_account_url, self.bucket_name_s3_blob)
        self.util_test_overwrite_copy_single_file_from_x_to_x(
            src_bucket_url, 
            "S3", 
            dst_container_url, 
            "Blob",
            False,
            True)

    def test_non_overwrite_copy_single_file_from_s3_to_blob(self):
        if 'S3_TESTS_OFF' in os.environ and os.environ['S3_TESTS_OFF'] != "":
            self.skipTest('S3 testing is disabled for this smoke test run.')
        src_bucket_url = util.get_object_without_sas(util.test_s2s_src_s3_service_url, self.bucket_name_s3_blob)
        dst_container_url = util.get_object_sas(util.test_s2s_dst_blob_account_url, self.bucket_name_s3_blob)
        self.util_test_overwrite_copy_single_file_from_x_to_x(
            src_bucket_url, 
            "S3", 
            dst_container_url, 
            "Blob",
            False,
            False)

    def test_copy_single_file_from_s3_to_blob_with_url_encoded_slash_as_filename(self):
        if 'S3_TESTS_OFF' in os.environ and os.environ['S3_TESTS_OFF'] != "":
            self.skipTest('S3 testing is disabled for this smoke test run.')
        src_bucket_url = util.get_object_without_sas(util.test_s2s_src_s3_service_url, self.bucket_name_s3_blob)
        dst_container_url = util.get_object_sas(util.test_s2s_dst_blob_account_url, self.bucket_name_s3_blob)
        self.util_test_copy_single_file_from_x_to_x(
            src_bucket_url, 
            "S3", 
            dst_container_url, 
            "Blob",
            1,
            False,
            "%252F") #encoded name for %2F, as path will be decoded

    def test_copy_single_file_from_s3_to_blob_excludeinvalidmetadata(self):
        if 'S3_TESTS_OFF' in os.environ and os.environ['S3_TESTS_OFF'] != "":
            self.skipTest('S3 testing is disabled for this smoke test run.')
        self.util_test_copy_single_file_from_s3_to_blob_handleinvalidmetadata(
            "", # By default it should be ExcludeIfInvalid
            "1abc=jiac;$%^=width;description=test file",
            "description=test file"
        )

    def test_copy_single_file_from_s3_to_blob_renameinvalidmetadata(self):
        if 'S3_TESTS_OFF' in os.environ and os.environ['S3_TESTS_OFF'] != "":
            self.skipTest('S3 testing is disabled for this smoke test run.')
        self.util_test_copy_single_file_from_s3_to_blob_handleinvalidmetadata(
            "RenameIfInvalid", # By default it should be ExcludeIfInvalid
            "1abc=jiac;$%^=width;description=test file",
            "rename_1abc=jiac;rename_key_1abc=1abc;description=test file;rename____=width;rename_key____=$%^"
        )

    # Test invalid metadata handling
    def util_test_copy_single_file_from_s3_to_blob_handleinvalidmetadata(
        self, 
        invalidMetadataHandleOption,
        srcS3Metadata, 
        expectResolvedMetadata):
        if 'S3_TESTS_OFF' in os.environ and os.environ['S3_TESTS_OFF'] != "":
            self.skipTest('S3 testing is disabled for this smoke test run.')
        srcBucketURL = util.get_object_without_sas(util.test_s2s_src_s3_service_url, self.bucket_name_s3_blob)
        dstBucketURL = util.get_object_sas(util.test_s2s_dst_blob_account_url, self.bucket_name_s3_blob)
        srcType = "S3"

        # create bucket and create file with metadata and properties
        result = util.Command("create").add_arguments(srcBucketURL).add_flags("serviceType", srcType). \
            add_flags("resourceType", "Bucket").execute_azcopy_create()
        self.assertTrue(result)

        fileName = "test_copy_single_file_from_s3_to_blob_handleinvalidmetadata_%s" % invalidMetadataHandleOption

        srcFileURL = util.get_object_without_sas(srcBucketURL, fileName)

        dstFileURL = util.get_object_sas(dstBucketURL, fileName)
        result = util.Command("create").add_arguments(srcFileURL).add_flags("serviceType", srcType). \
            add_flags("resourceType", "SingleFile"). \
            add_flags("metadata", srcS3Metadata). \
            execute_azcopy_create()
        self.assertTrue(result)

        # Copy file using azcopy from srcURL to destURL
        cpCmd = util.Command("copy").add_arguments(srcFileURL).add_arguments(dstFileURL). \
            add_flags("log-level", "info")
        if invalidMetadataHandleOption == "" or invalidMetadataHandleOption == "ExcludeIfInvalid":
            cpCmd.add_flags("s2s-handle-invalid-metadata", "ExcludeIfInvalid")
        if invalidMetadataHandleOption == "FailIfInvalid":
            cpCmd.add_flags("s2s-handle-invalid-metadata", "FailIfInvalid")
        if invalidMetadataHandleOption == "RenameIfInvalid":
            cpCmd.add_flags("s2s-handle-invalid-metadata", "RenameIfInvalid")

        result = cpCmd.execute_azcopy_copy_command()
        self.assertTrue(result)

        # Downloading the copied file for validation
        validate_dir_name = "validate_copy_single_file_from_s3_to_blob_handleinvalidmetadata_%s" % invalidMetadataHandleOption
        local_validate_dest_dir = util.create_test_dir(validate_dir_name)
        local_validate_dest = local_validate_dest_dir + fileName
        result = util.Command("copy").add_arguments(dstFileURL).add_arguments(local_validate_dest). \
            add_flags("log-level", "info").execute_azcopy_copy_command()
        self.assertTrue(result)

        validateCmd = util.Command("testBlob").add_arguments(local_validate_dest).add_arguments(dstFileURL).add_flags("no-guess-mime-type", "true"). \
            add_flags("metadata", expectResolvedMetadata)

        result = validateCmd.execute_azcopy_verify()
        self.assertTrue(result)


    # Test oauth support for service to service copy, where source is authenticated with access key for S3
    # and destination is authenticated with OAuth token.
    @unittest.skip("covered by blob to blob")
    def test_copy_single_17mb_file_from_s3_to_blob_oauth(self):
        src_bucket_url = util.get_object_without_sas(util.test_s2s_src_s3_service_url, self.bucket_name_s3_blob)
        dst_container_url = util.get_object_without_sas(util.test_s2s_dst_blob_account_url, self.bucket_name_s3_blob)
        self.util_test_copy_single_file_from_x_to_x(src_bucket_url, "S3", dst_container_url, "Blob", 17 * 1024 * 1024, True)

    ##################################
    # Test scenarios related to blob type and blob tier.
    ##################################
    def test_copy_single_file_from_s3_object_to_blockblob_with_default_blobtier(self):
        if 'S3_TESTS_OFF' in os.environ and os.environ['S3_TESTS_OFF'] != "":
            self.skipTest('S3 testing is disabled for this smoke test run.')
        src_bucket_url = util.get_object_without_sas(util.test_s2s_src_s3_service_url, self.bucket_name_block_append_page)
        dst_container_url = util.get_object_sas(util.test_s2s_dst_blob_account_url, self.bucket_name_block_append_page)
        blob_sizes = [0, 1, 8*1024*1024 - 1, 8 * 1024*1024]
        for size in blob_sizes:
            self.util_test_copy_single_file_from_x_to_blob_with_blobtype_blobtier(
                src_bucket_url, "S3", dst_container_url, "Blob", size)

    @unittest.skip("override blob tier not enabled")
    def test_copy_single_file_from_s3_object_to_blockblob_with_specified_blobtier(self):
        src_bucket_url = util.get_object_without_sas(util.test_s2s_src_s3_service_url, self.bucket_name_block_append_page)
        dst_container_url = util.get_object_sas(util.test_s2s_dst_blob_account_url, self.bucket_name_block_append_page)
        self.util_test_copy_single_file_from_x_to_blob_with_blobtype_blobtier(
                src_bucket_url, "S3", dst_container_url, "Blob", 8*1024*1024+1, "", "", "BlockBlob", "Cool", "BlockBlob", "Cool")

    @unittest.skip("override blob type not enabled")
    def test_copy_single_file_from_s3_object_to_appendblob_from_source(self):
        src_bucket_url = util.get_object_without_sas(util.test_s2s_src_s3_service_url, self.bucket_name_block_append_page)
        dst_container_url = util.get_object_sas(util.test_s2s_dst_blob_account_url, self.bucket_name_block_append_page)
        blob_sizes = [0, 1, 8*1024*1024 - 1, 8 * 1024*1024, 8*1024*1024+1]
        for size in blob_sizes:
            self.util_test_copy_single_file_from_x_to_blob_with_blobtype_blobtier(
                src_bucket_url, "S3", dst_container_url, "Blob", size, "", "", "AppendBlob", "", "AppendBlob")

    @unittest.skip("override blob type not enabled")
    def test_copy_single_file_from_s3_object_to_pageblob_with_blobtier_from_source(self):
        src_bucket_url = util.get_object_without_sas(util.test_s2s_src_s3_service_url, self.bucket_name_block_append_page)
        dst_container_url = util.get_object_sas(util.test_s2s_dst_blob_account_url, self.bucket_name_block_append_page)
        blob_sizes = [0, 512, 1024, 8*1024*1024]
        for size in blob_sizes:
            self.util_test_copy_single_file_from_x_to_blob_with_blobtype_blobtier(
                src_bucket_url, "S3", dst_container_url, "Blob", size, "", "", "PageBlob", "", "PageBlob")
    
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

    def util_upload_to_src(
        self,
        localFilePath,
        srcType,
        srcURLForCopy,
        recursive=False,
        blobType="",
        blobTier=""):
        if srcType == "S3":
            cmd = util.Command("upload").add_arguments(localFilePath).add_arguments(srcURLForCopy)
        else:
            cmd = util.Command("copy").add_arguments(localFilePath).add_arguments(srcURLForCopy).add_flags("log-level", "info")
            if blobType != "" :
                cmd.add_flags("blob-type", blobType)
            if blobType == "PageBlob" and blobTier != "" :
                cmd.add_flags("page-blob-tier", blobTier)
            if blobType == "BlockBlob" and blobTier != "" :
                cmd.add_flags("block-blob-tier", blobTier)

        if recursive:
            cmd.add_flags("recursive", "true")

        if srcType == "S3":
            result = cmd.execute_testsuite_upload()
        else:
            result = cmd.execute_azcopy_copy_command()

        self.assertTrue(result)

    def util_test_copy_single_file_from_x_to_x(
        self,
        srcBucketURL,
        srcType,
        dstBucketURL,
        dstType,
        sizeInKB=1,
        oAuth=False,
        customizedFileName="",
        srcBlobType="",
        dstBlobType="",
        credTypeOverride=""):
        # create source bucket
        result = util.Command("create").add_arguments(srcBucketURL).add_flags("serviceType", srcType). \
            add_flags("resourceType", "Bucket").execute_azcopy_create()
        self.assertTrue(result)

        # create file of size 1KB.
        if customizedFileName != "":
            filename = customizedFileName
        else:
            filename = "test_" + str(sizeInKB) + "kb_copy.txt"
        file_path = util.create_test_file(filename, sizeInKB)
        if srcType == "S3":
            srcFileURL = util.get_object_without_sas(srcBucketURL, filename)
        else:
            srcFileURL = util.get_object_sas(srcBucketURL, filename)

        if oAuth:
            dstFileURL = util.get_object_without_sas(dstBucketURL, filename)
        else:
            dstFileURL = util.get_object_sas(dstBucketURL, filename)

        # Upload file.
        self.util_upload_to_src(file_path, srcType, srcFileURL, blobType=srcBlobType)

        if credTypeOverride != "":
            os.environ["AZCOPY_CRED_TYPE"] = credTypeOverride

        # Copy file using azcopy from srcURL to destURL
        result = util.Command("copy").add_arguments(srcFileURL).add_arguments(dstFileURL). \
            add_flags("log-level", "info")
        if dstBlobType != "":
            result = result.add_flags("blob-type", dstBlobType)

        r = result.execute_azcopy_copy_command()  # nice "dynamic typing"
        self.assertTrue(r)

        if credTypeOverride != "":
            os.environ["AZCOPY_CRED_TYPE"] = ""

        # Downloading the copied file for validation
        validate_dir_name = "validate_copy_single_%dKB_file_from_%s_to_%s_%s" % (sizeInKB, srcType, dstType, customizedFileName)
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

        # Upload file.
        self.util_upload_to_src(src_dir_path, srcType, srcBucketURL, True)

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

    def util_test_copy_file_from_x_bucket_to_x_bucket_strip_top_dir(
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
        filename = "copy_strip_top_dir_file.txt"
        file_path = util.create_test_file(filename, 1)
        if srcType == "S3":
            srcFileURL = util.get_object_without_sas(srcBucketURL, filename)
        else:
            srcFileURL = util.get_object_sas(srcBucketURL, filename)
        src_dir_url = srcFileURL.replace(filename, "*")

        # Upload file.
        self.util_upload_to_src(file_path, srcType, srcFileURL, False)

        # Copy file using azcopy from srcURL to destURL
        if recursive:
            result = util.Command("copy").add_arguments(src_dir_url).add_arguments(dstBucketURL). \
                add_flags("log-level", "info").add_flags("recursive", "true"). \
                execute_azcopy_copy_command()
        else:
            result = util.Command("copy").add_arguments(src_dir_url).add_arguments(dstBucketURL). \
                add_flags("log-level", "info").execute_azcopy_copy_command()
        self.assertTrue(result)

        # Downloading the copied files for validation
        validate_dir_name = "validate_copy_file_from_%s_bucket_to_%s_bucket_strip_top_dir_recursive_%s" % (srcType, dstType, recursive)
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
        # create source bucketa
        result = util.Command("create").add_arguments(srcBucketURL).add_flags("serviceType", srcType). \
            add_flags("resourceType", "Bucket").execute_azcopy_create()
        self.assertTrue(result)

        # create file of size n KBs in newly created directory.
        src_dir_name = "copy_%d_%dKB_files_from_%s_dir_to_%s_dir" % (n, sizeInKB, srcType, dstType)
        src_dir_path = util.create_test_n_files(sizeInKB*1024, n, src_dir_name)

        # Upload file.
        self.util_upload_to_src(src_dir_path, srcType, srcBucketURL, True)

        if srcType == "S3":
            srcDirURL = util.get_object_without_sas(srcBucketURL, src_dir_name)
        else:
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
        print(src_dir_path)
        print(os.path.join(local_validate_dest, src_dir_name, src_dir_name))
        result = self.util_are_dir_trees_equal(src_dir_path, os.path.join(local_validate_dest, src_dir_name, src_dir_name))
        #result = self.util_are_dir_trees_equal(src_dir_path, local_validate_dest)
        self.assertTrue(result)

        # clean up both source and destination bucket
        # util.Command("clean").add_arguments(srcBucketURL).add_flags("serviceType", srcType). \
        #     add_flags("resourceType", "Bucket").execute_azcopy_create()

        # util.Command("clean").add_arguments(dstBucketURL).add_flags("serviceType", dstType). \
        #     add_flags("resourceType", "Bucket").execute_azcopy_create()

    def util_test_copy_n_files_from_x_dir_to_x_dir_strip_top_dir(self,
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

        # Upload file.
        self.util_upload_to_src(src_dir_path, srcType, srcBucketURL, True)
        
        if srcType == "S3":
            src_dir_url = util.get_object_without_sas(srcBucketURL, src_dir_name + "/*")
        else:
            src_dir_url = util.get_object_sas(srcBucketURL, src_dir_name + "/*")

        dstDirURL = util.get_object_sas(dstBucketURL, src_dir_name)
        if recursive:
            # Copy files using azcopy from srcURL to destURL
            result = util.Command("copy").add_arguments(src_dir_url).add_arguments(dstDirURL). \
                add_flags("log-level", "info").add_flags("recursive", "true"). \
                execute_azcopy_copy_command()
            self.assertTrue(result)
        else:
            # Copy files using azcopy from srcURL to destURL
            result = util.Command("copy").add_arguments(src_dir_url).add_arguments(dstDirURL). \
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
        bucketNamePrefix):
        # More enumerating scenarios could be covered with integration testing.
        bucketName1 = bucketNamePrefix + "1"
        bucketName2 = bucketNamePrefix + "2"
        if srcType == "S3":
            src_bucket_url1 = util.get_object_without_sas(srcAccountURL, bucketName1)
            src_bucket_url2 = util.get_object_without_sas(srcAccountURL, bucketName2)
        else:
            src_bucket_url1 = util.get_object_sas(srcAccountURL, bucketName1)
            src_bucket_url2 = util.get_object_sas(srcAccountURL, bucketName2)

        # create source bucket
        createBucketResult1 = util.Command("create").add_arguments(src_bucket_url1).add_flags("serviceType", srcType). \
            add_flags("resourceType", "Bucket").execute_azcopy_create()
        self.assertTrue(createBucketResult1)

        createBucketResult2 = util.Command("create").add_arguments(src_bucket_url2).add_flags("serviceType", srcType). \
            add_flags("resourceType", "Bucket").execute_azcopy_create()
        self.assertTrue(createBucketResult2)

        # create files of size n KBs.
        src_dir_name1 = "copy_files_from_%s_account_to_%s_account_1" % (srcType, dstType)
        src_dir_path1 = util.create_test_n_files(1*1024, 100, src_dir_name1)
        src_dir_name2 = "copy_files_from_%s_account_to_%s_account_2" % (srcType, dstType)
        src_dir_path2 = util.create_test_n_files(1, 2, src_dir_name2)

        # Upload file.
        self.util_upload_to_src(src_dir_path1, srcType, src_bucket_url1, True)
        self.util_upload_to_src(src_dir_path2, srcType, src_bucket_url2, True)

        # Copy files using azcopy from srcURL to destURL
        result = util.Command("copy").add_arguments(srcAccountURL).add_arguments(dstAccountURL). \
            add_flags("log-level", "info").add_flags("recursive", "true").execute_azcopy_copy_command()
        self.assertTrue(result)

        # Downloading the copied files for validation
        validate_dir_name1 = "validate_copy_files_from_%s_account_to_%s_account_1" % (srcType, dstType)
        local_validate_dest1 = util.create_test_dir(validate_dir_name1)    
        dst_container_url1 = util.get_object_sas(dstAccountURL, bucketName1)
        dst_directory_url1 = util.get_object_sas(dst_container_url1, src_dir_name1)
        result = util.Command("copy").add_arguments(dst_directory_url1).add_arguments(local_validate_dest1). \
            add_flags("log-level", "info").add_flags("recursive", "true").execute_azcopy_copy_command()
        self.assertTrue(result)
         # Verifying the downloaded blob
        result = self.util_are_dir_trees_equal(src_dir_path1, os.path.join(local_validate_dest1, src_dir_name1))
        self.assertTrue(result)

        validate_dir_name2 = "validate_copy_files_from_%s_account_to_%s_account_2" % (srcType, dstType)
        local_validate_dest2 = util.create_test_dir(validate_dir_name2)    
        dst_container_url2 = util.get_object_sas(dstAccountURL, bucketName2)
        dst_directory_url2 = util.get_object_sas(dst_container_url2, src_dir_name2)
        result = util.Command("copy").add_arguments(dst_directory_url2).add_arguments(local_validate_dest2). \
            add_flags("log-level", "info").add_flags("recursive", "true").execute_azcopy_copy_command()
        self.assertTrue(result)
        # Verifying the downloaded blob
        result = self.util_are_dir_trees_equal(src_dir_path2, os.path.join(local_validate_dest2, src_dir_name2))
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
        dstType,
        preserveProperties=True):
        # create bucket and create file with metadata and properties
        result = util.Command("create").add_arguments(srcBucketURL).add_flags("serviceType", srcType). \
            add_flags("resourceType", "Bucket").execute_azcopy_create()
        self.assertTrue(result)

        fileName = "single_file_propertyandmetadata_%s" % (preserveProperties)

        if srcType == "S3":
            srcFileURL = util.get_object_without_sas(srcBucketURL, fileName)
        else:
            srcFileURL = util.get_object_sas(srcBucketURL, fileName)

        dstFileURL = util.get_object_sas(dstBucketURL, fileName)
        result = util.Command("create").add_arguments(srcFileURL).add_flags("serviceType", srcType). \
            add_flags("resourceType", "SingleFile"). \
            add_flags("metadata", "author=jiac;viewport=width;description=test file"). \
            add_flags("content-type", "testctype").add_flags("content-encoding", "testenc"). \
            add_flags("content-disposition", "testcdis").add_flags("content-language", "testclang").\
            add_flags("cache-control", "testcc").execute_azcopy_create()
        self.assertTrue(result)

        # Copy file using azcopy from srcURL to destURL
        cpCmd = util.Command("copy").add_arguments(srcFileURL).add_arguments(dstFileURL). \
            add_flags("log-level", "info")
        if preserveProperties == False:
            cpCmd.add_flags("s2s-preserve-properties", "false")

        result = cpCmd.execute_azcopy_copy_command()
        self.assertTrue(result)

        # Downloading the copied file for validation
        validate_dir_name = "validate_copy_single_file_from_%s_to_%s_propertyandmetadata_%s" % (srcType, dstType, preserveProperties)
        local_validate_dest_dir = util.create_test_dir(validate_dir_name)
        local_validate_dest = local_validate_dest_dir + fileName
        if srcType == "S3":
            result = util.Command("copy").add_arguments(dstFileURL).add_arguments(local_validate_dest). \
                add_flags("log-level", "info").execute_azcopy_copy_command()
        else:
            result = util.Command("copy").add_arguments(srcFileURL).add_arguments(local_validate_dest). \
                add_flags("log-level", "info").execute_azcopy_copy_command()
        self.assertTrue(result)

        # TODO: test different targets according to dstType
        testCmdName = "testBlob" if dstType.lower() == "blob" else "testFile"
        validateCmd = util.Command(testCmdName).add_arguments(local_validate_dest).add_arguments(dstFileURL).add_flags("no-guess-mime-type", "true")

        if preserveProperties == True:
            validateCmd.add_flags("metadata", "author=jiac;viewport=width;description=test file"). \
            add_flags("content-type", "testctype").add_flags("content-encoding", "testenc"). \
            add_flags("content-disposition", "testcdis").add_flags("content-language", "testclang"). \
            add_flags("cache-control", "testcc")
        else:
            validateCmd.add_flags("metadata", ""). \
            add_flags("content-type", "").add_flags("content-encoding", ""). \
            add_flags("content-disposition", "").add_flags("content-language", ""). \
            add_flags("cache-control", "")
        
        # As head object doesn't return Content-MD5: https://docs.aws.amazon.com/AmazonS3/latest/API/RESTCommonResponseHeaders.html
        # Escape Content-MD5 validation for S3
        if srcType != "S3":
            validateCmd.add_flags("check-content-md5", "true")

        result = validateCmd.execute_azcopy_verify()
        self.assertTrue(result)

    def util_test_copy_file_from_x_bucket_to_x_bucket_propertyandmetadata(
        self,
        srcBucketURL,
        srcType,
        dstBucketURL,
        dstType,
        preserveProperties=True):
        # create bucket and create file with metadata and properties
        result = util.Command("create").add_arguments(srcBucketURL).add_flags("serviceType", srcType). \
            add_flags("resourceType", "Bucket").execute_azcopy_create()
        self.assertTrue(result)

        fileName = "bucket_file_propertyandmetadata_%s" % (preserveProperties)

        if srcType == "S3":
            srcFileURL = util.get_object_without_sas(srcBucketURL, fileName)
        else:
            srcFileURL = util.get_object_sas(srcBucketURL, fileName)

        dstFileURL = util.get_object_sas(dstBucketURL, fileName)
        result = util.Command("create").add_arguments(srcFileURL).add_flags("serviceType", srcType). \
            add_flags("resourceType", "SingleFile"). \
            add_flags("metadata", "author=jiac;viewport=width;description=test file"). \
            add_flags("content-type", "testctype").add_flags("content-encoding", "testenc"). \
            add_flags("content-disposition", "testcdis").add_flags("content-language", "testclang").\
            add_flags("cache-control", "testcc").execute_azcopy_create()
        self.assertTrue(result)

        # Copy file using azcopy from srcURL to destURL
        cpCmd = util.Command("copy").add_arguments(srcBucketURL).add_arguments(dstBucketURL). \
            add_flags("log-level", "info").add_flags("recursive", "true")

        if not preserveProperties:
            cpCmd.add_flags("s2s-preserve-properties", "false")

        result = cpCmd.execute_azcopy_copy_command()
        self.assertTrue(result)

        # Downloading the copied file for validation
        validate_dir_name = "validate_copy_file_from_%s_bucket_to_%s_bucket_propertyandmetadata_%s" % (srcType, dstType, preserveProperties)
        local_validate_dest_dir = util.create_test_dir(validate_dir_name)
        local_validate_dest = local_validate_dest_dir + fileName
        # Because the MD5 is checked early, we need to clear the check-md5 flag.
        if srcType == "S3":
            result = util.Command("copy").add_arguments(dstFileURL).add_arguments(local_validate_dest). \
                add_flags("log-level", "info")  # Temporarily set result to Command for the sake of modifying the md5 check
            if not preserveProperties:
                result.flags["check-md5"] = "NoCheck"
            result = result.execute_azcopy_copy_command()  # Wrangle result to a bool for checking
        else:
            result = util.Command("copy").add_arguments(srcFileURL).add_arguments(local_validate_dest). \
                add_flags("log-level", "info")  # Temporarily set result to Command for the sake of modifying the md5 check
            if not preserveProperties:
                result.flags["check-md5"] = "NoCheck"
            result = result.execute_azcopy_copy_command()  # Wrangle result to a bool for checking
        self.assertTrue(result)

        # TODO: test different targets according to dstType
        validateCmd = util.Command("testBlob").add_arguments(local_validate_dest).add_arguments(dstFileURL).add_flags("no-guess-mime-type", "true")

        if preserveProperties == True:
            validateCmd.add_flags("metadata", "author=jiac;viewport=width;description=test file"). \
            add_flags("content-type", "testctype").add_flags("content-encoding", "testenc"). \
            add_flags("content-disposition", "testcdis").add_flags("content-language", "testclang"). \
            add_flags("cache-control", "testcc")
        else:
            validateCmd.add_flags("metadata", ""). \
            add_flags("content-type", "").add_flags("content-encoding", ""). \
            add_flags("content-disposition", "").add_flags("content-language", ""). \
            add_flags("cache-control", "")
        
        # As head object doesn't return Content-MD5: https://docs.aws.amazon.com/AmazonS3/latest/API/RESTCommonResponseHeaders.html
        # Escape Content-MD5 validation for S3
        if srcType != "S3":
            validateCmd.add_flags("check-content-md5", "true")

        result = validateCmd.execute_azcopy_verify()
        self.assertTrue(result)

    def util_test_overwrite_copy_single_file_from_x_to_x(
        self,
        srcBucketURL,
        srcType,
        dstBucketURL,
        dstType,
        oAuth=False,
        overwrite=True):
        # create source bucket
        result = util.Command("create").add_arguments(srcBucketURL).add_flags("serviceType", srcType). \
            add_flags("resourceType", "Bucket").execute_azcopy_create()
        self.assertTrue(result)
        result = util.Command("create").add_arguments(dstBucketURL).add_flags("serviceType", dstType). \
            add_flags("resourceType", "Bucket").execute_azcopy_create()
        self.assertTrue(result)

        fileSize1 = 1
        fileSize2 = 2

        # create file of size 1KB.
        destFileName = "test_copy.txt"
        localFileName1 = "test_" + str(fileSize1) + "kb_copy.txt"
        localFileName2 = "test_" + str(fileSize2) + "kb_copy.txt"
        filePath1 = util.create_test_file(localFileName1, fileSize1)
        filePath2 = util.create_test_file(localFileName2, fileSize2)
        if srcType == "S3":
            srcFileURL = util.get_object_without_sas(srcBucketURL, localFileName1)
        else:
            srcFileURL = util.get_object_sas(srcBucketURL, localFileName1)

        if oAuth:
            dstFileURL = util.get_object_without_sas(dstBucketURL, destFileName)
        else:
            dstFileURL = util.get_object_sas(dstBucketURL, destFileName)
        
        # Upload file.
        self.util_upload_to_src(filePath1, srcType, srcFileURL)
        self.util_upload_to_src(filePath2, dstType, dstFileURL)

        # Copy file using azcopy from srcURL to destURL
        cpCmd = util.Command("copy").add_arguments(srcFileURL).add_arguments(dstFileURL). \
            add_flags("log-level", "info")

        if overwrite == False:
            cpCmd.add_flags("overwrite", "false")

        result = cpCmd.execute_azcopy_copy_command()
        self.assertTrue(result)

        # Downloading the copied file for validation
        validate_dir_name = "validate_overwrite_%s_copy_single_file_from_%s_to_%s" % (overwrite, srcType, dstType)
        local_validate_dest_dir = util.create_test_dir(validate_dir_name)
        local_validate_dest = os.path.join(local_validate_dest_dir, destFileName)
        result = util.Command("copy").add_arguments(dstFileURL).add_arguments(local_validate_dest). \
            add_flags("log-level", "info").execute_azcopy_copy_command()
        self.assertTrue(result)

        # Verifying the downloaded blob
        if overwrite:
            result = filecmp.cmp(filePath1, local_validate_dest, shallow=False)
        else:
            result = filecmp.cmp(filePath2, local_validate_dest, shallow=False)

        self.assertTrue(result)

    def util_test_copy_n_files_from_s3_bucket_to_blob_account(
        self,
        srcBucketURL,
        dstAccountURL,
        n=10,
        sizeInKB=1):
        srcType = "S3"

        # create source bucket
        result = util.Command("create").add_arguments(srcBucketURL).add_flags("serviceType", srcType). \
            add_flags("resourceType", "Bucket").execute_azcopy_create()
        self.assertTrue(result)

        # create file of size n KBs in newly created directory.
        src_dir_name = "copy_%d_%dKB_files_from_s3_bucket_to_blob_account" % (n, sizeInKB)
        src_dir_path = util.create_test_n_files(sizeInKB*1024, n, src_dir_name)

        # Upload file.
        self.util_upload_to_src(src_dir_path, srcType, srcBucketURL, True)

        # Copy files using azcopy from srcURL to destURL
        result = util.Command("copy").add_arguments(srcBucketURL).add_arguments(dstAccountURL). \
            add_flags("log-level", "info").add_flags("recursive", "true").execute_azcopy_copy_command()
        self.assertTrue(result)

        # Downloading the copied files for validation
        validate_dir_name = "validate_copy_%d_%dKB_files_from_s3_bucket_to_blob_account" % (n, sizeInKB)
        local_validate_dest = util.create_test_dir(validate_dir_name)
        validateDstBucketURL = util.get_object_sas(dstAccountURL, self.bucket_name_s3_blob)
        dst_directory_url = util.get_object_sas(validateDstBucketURL, src_dir_name)
        result = util.Command("copy").add_arguments(dst_directory_url).add_arguments(local_validate_dest). \
            add_flags("log-level", "info").add_flags("recursive", "true").execute_azcopy_copy_command()
        self.assertTrue(result)

        # Verifying the downloaded blob
        result = self.util_are_dir_trees_equal(src_dir_path, os.path.join(local_validate_dest, src_dir_name))
        self.assertTrue(result)

    def util_test_copy_single_file_from_x_to_blob_with_blobtype_blobtier(
        self,
        srcBucketURL,
        srcType,
        dstBucketURL,
        dstType,
        sizeInKB=1,
        srcBlobType="",
        srcBlobTier="",
        destBlobTypeOverride="",
        destBlobTierOverride="",
        blobTypeForValidation="BlockBlob",
        blobTierForValidation="Hot",
        preserveAccessTier=True):
        # create source bucket
        result = util.Command("create").add_arguments(srcBucketURL).add_flags("serviceType", srcType). \
            add_flags("resourceType", "Bucket").execute_azcopy_create()
        self.assertTrue(result)

        # create file of size 1KB.
        filename = "test_%s_kb_%s_%s_%s_%s_%s_%s_%s_%s_copy.txt" % (str(sizeInKB), srcType, dstType, srcBlobType, srcBlobTier, destBlobTypeOverride, destBlobTierOverride, blobTypeForValidation, blobTierForValidation)
        file_path = util.create_test_file(filename, sizeInKB)
        if srcType == "S3":
            srcFileURL = util.get_object_without_sas(srcBucketURL, filename)
        else:
            srcFileURL = util.get_object_sas(srcBucketURL, filename)

        dstFileURL = util.get_object_sas(dstBucketURL, filename)

        # upload file.
        self.util_upload_to_src(file_path, srcType, srcFileURL, False, srcBlobType, srcBlobTier)

        # copy file using azcopy from srcURL to destURL
        copyCmd = util.Command("copy").add_arguments(srcFileURL).add_arguments(dstFileURL). \
            add_flags("log-level", "info")
        if destBlobTypeOverride != "":
            copyCmd.add_flags("blob-type", destBlobTypeOverride)
        if destBlobTierOverride != "":
            if destBlobTypeOverride == "PageBlob" or (srcBlobType == "PageBlob" and destBlobTypeOverride == ""):
                copyCmd.add_flags("page-blob-tier", destBlobTierOverride)
            if destBlobTypeOverride == "BlockBlob" or (srcBlobType == "BlockBlob" and destBlobTypeOverride == ""):
                copyCmd.add_flags("block-blob-tier", destBlobTierOverride)
        if preserveAccessTier == False:
            copyCmd.add_flags("s2s-preserve-access-tier", "false")

        copyCmdResult = copyCmd.execute_azcopy_copy_command()
        self.assertTrue(copyCmdResult)

        # execute validator.
        # don't check content-type, as it dependes on upload, and service behavior.
        # cover content-type check in another test.
        testBlobCmd = util.Command("testBlob").add_arguments(file_path).add_arguments(dstFileURL). \
            add_flags("check-content-type", "false")

        if blobTypeForValidation != "":
            testBlobCmd.add_flags("blob-type", blobTypeForValidation)
        if blobTierForValidation != "":
            testBlobCmd.add_flags("blob-tier", blobTierForValidation)
                
        testBlobResult = testBlobCmd.execute_azcopy_verify()
        self.assertTrue(testBlobResult)
