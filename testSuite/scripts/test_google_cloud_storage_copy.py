import os
import unittest
import utility as util
import filecmp

class Google_Cloud_Storage_Copy_User_Scenario(unittest.TestCase):

    def setUp(self):
        if 'GCP_TESTS_OFF' in os.environ and os.environ['GCP_TESTS_OFF'] != "":
            self.skipTest('GCS testing is disabled for this smoke test run')
        self.bucket_name = util.get_resource_name('s2scopybucket' + 'gcpblob')
    
    def test_copy_single_1kb_file_from_gcp_to_blob(self):
        src_bucket_url = util.get_object_without_sas(util.test_s2s_src_gcp_service_url, self.bucket_name)
        dst_container_url = util.get_object_sas(util.test_s2s_dst_blob_account_url, self.bucket_name)
        self.util_test_copy_single_file_from_x_to_x(src_bucket_url, "GCP", dst_container_url, "Blob", 1)

    def test_copy_single_0kb_file_from_gcp_to_blob(self):
        src_bucket_url = util.get_object_without_sas(util.test_s2s_src_gcp_service_url, self.bucket_name)
        dst_container_url = util.get_object_sas(util.test_s2s_dst_blob_account_url, self.bucket_name)
        self.util_test_copy_single_file_from_x_to_x(src_bucket_url, "GCP", dst_container_url, "Blob", 0)
    
    def test_copy_single_63mb_file_from_gcp_to_blob(self):
        src_bucket_url = util.get_object_without_sas(util.test_s2s_src_gcp_service_url, self.bucket_name)
        dst_container_url = util.get_object_sas(util.test_s2s_dst_blob_account_url, self.bucket_name)
        self.util_test_copy_single_file_from_x_to_x(src_bucket_url, "GCP", dst_container_url, "Blob", 63 * 1024)

    def test_copy_10_files_from_gcp_bucket_to_blob_container(self):
        src_bucket_url =  util.get_object_without_sas(util.test_s2s_src_gcp_service_url, self.bucket_name)
        dst_container_url = util.get_object_sas(util.test_s2s_dst_blob_account_url, self.bucket_name)
        self.util_test_copy_n_files_from_x_bucket_to_x_bucket(src_bucket_url, "GCP", dst_container_url, "Blob")
    
    def test_copy_10_files_from_gcp_bucket_to_blob_account(self):
        src_bucket_url = util.get_object_without_sas(util.test_s2s_src_gcp_service_url, self.bucket_name)
        self.util_test_copy_n_files_from_gcp_bucket_to_blob_account(src_bucket_url, util.test_s2s_dst_blob_account_url)
    
    def test_copy_file_from_gcp_bucket_to_blob_container_strip_top_dir_recursive(self):
        src_bucket_url = util.get_object_without_sas(util.test_s2s_src_gcp_service_url, self.bucket_name)
        dst_container_url = util.get_object_sas(util.test_s2s_dst_blob_account_url, self.bucket_name)
        self.util_test_copy_file_from_x_bucket_to_x_bucket_strip_top_dir(src_bucket_url, "GCP", dst_container_url, "Blob", True)

    def test_copy_file_from_gcp_bucket_to_blob_container_strip_top_dir_non_recursive(self):
        src_bucket_url = util.get_object_without_sas(util.test_s2s_src_gcp_service_url, self.bucket_name)
        dst_container_url = util.get_object_sas(util.test_s2s_dst_blob_account_url, self.bucket_name)
        self.util_test_copy_file_from_x_bucket_to_x_bucket_strip_top_dir(src_bucket_url, "GCP", dst_container_url, "Blob", False)

    def test_copy_n_files_from_gcp_dir_to_blob_dir(self):
        src_bucket_url = util.get_object_without_sas(util.test_s2s_src_gcp_service_url, self.bucket_name)
        dst_container_url = util.get_object_sas(util.test_s2s_dst_blob_account_url, self.bucket_name)
        self.util_test_copy_n_files_from_x_dir_to_x_dir(src_bucket_url, "GCP", dst_container_url, "Blob")

    def test_copy_n_files_from_gcp_dir_to_blob_dir_strip_top_dir_recursive(self):
        src_bucket_url = util.get_object_without_sas(util.test_s2s_src_gcp_service_url, self.bucket_name)
        dst_container_url = util.get_object_sas(util.test_s2s_dst_blob_account_url, self.bucket_name)
        self.util_test_copy_n_files_from_x_dir_to_x_dir_strip_top_dir(src_bucket_url, "GCP", dst_container_url, "Blob", True)
    
    def test_copy_n_files_from_gcp_dir_to_blob_dir_strip_top_dir_non_recursive(self):
        src_bucket_url = util.get_object_without_sas(util.test_s2s_src_gcp_service_url, self.bucket_name)
        dst_container_url = util.get_object_sas(util.test_s2s_dst_blob_account_url, self.bucket_name)
        self.util_test_copy_n_files_from_x_dir_to_x_dir_strip_top_dir(src_bucket_url, "GCP", dst_container_url, "Blob", False)
    
    def test_copy_files_from_gcp_service_to_blob_account(self):
        self.util_test_copy_files_from_x_account_to_x_account(
            util.test_s2s_src_gcp_service_url, 
            "GCP", 
            util.test_s2s_dst_blob_account_url, 
            "Blob",
            self.bucket_name)
    
    def test_copy_single_file_from_gcp_to_blob_propertyandmetadata(self):
        src_bucket_url = util.get_object_without_sas(util.test_s2s_src_gcp_service_url, self.bucket_name)
        dst_container_url = util.get_object_sas(util.test_s2s_dst_blob_account_url, self.bucket_name)
        self.util_test_copy_single_file_from_x_to_x_propertyandmetadata(
            src_bucket_url, 
            "GCP", 
            dst_container_url, 
            "Blob")

    def test_copy_single_file_from_gcp_to_blob_no_preserve_propertyandmetadata(self):
        src_bucket_url = util.get_object_without_sas(util.test_s2s_src_gcp_service_url, self.bucket_name)
        dst_container_url = util.get_object_sas(util.test_s2s_dst_blob_account_url, self.bucket_name)
        self.util_test_copy_single_file_from_x_to_x_propertyandmetadata(
            src_bucket_url, 
            "GCP", 
            dst_container_url, 
            "Blob",
            False)
    
    def test_copy_file_from_gcp_bucket_to_blob_container_propertyandmetadata(self):
        src_bucket_url = util.get_object_without_sas(util.test_s2s_src_gcp_service_url, self.bucket_name)
        dst_container_url = util.get_object_sas(util.test_s2s_dst_blob_account_url, self.bucket_name)
        self.util_test_copy_file_from_x_bucket_to_x_bucket_propertyandmetadata(
            src_bucket_url, 
            "GCP", 
            dst_container_url, 
            "Blob")
    
    def test_copy_file_from_gcp_bucket_to_blob_container_no_preserve_propertyandmetadata(self):
        src_bucket_url = util.get_object_without_sas(util.test_s2s_src_gcp_service_url, self.bucket_name)
        dst_container_url = util.get_object_sas(util.test_s2s_dst_blob_account_url, self.bucket_name)
        self.util_test_copy_file_from_x_bucket_to_x_bucket_propertyandmetadata(
            src_bucket_url, 
            "GCP", 
            dst_container_url, 
            "Blob",
            False)
    
    def test_overwrite_copy_single_file_from_gcp_to_blob(self):
        src_bucket_url = util.get_object_without_sas(util.test_s2s_src_gcp_service_url, self.bucket_name)
        dst_container_url = util.get_object_sas(util.test_s2s_dst_blob_account_url, self.bucket_name)
        self.util_test_overwrite_copy_single_file_from_x_to_x(
            src_bucket_url, 
            "GCP", 
            dst_container_url, 
            "Blob",
            False,
            True)
    
    def test_non_overwrite_copy_single_file_from_gcp_to_blob(self):
        src_bucket_url = util.get_object_without_sas(util.test_s2s_src_gcp_service_url, self.bucket_name)
        dst_container_url = util.get_object_sas(util.test_s2s_dst_blob_account_url, self.bucket_name)
        self.util_test_overwrite_copy_single_file_from_x_to_x(
            src_bucket_url, 
            "GCP", 
            dst_container_url, 
            "Blob",
            False,
            False)

    def test_copy_single_file_from_gcp_to_blob_with_url_encoded_slash_as_filename(self):
        src_bucket_url = util.get_object_without_sas(util.test_s2s_src_gcp_service_url, self.bucket_name)
        dst_container_url = util.get_object_sas(util.test_s2s_dst_blob_account_url, self.bucket_name)
        self.util_test_copy_single_file_from_x_to_x(
            src_bucket_url, 
            "GCP", 
            dst_container_url, 
            "Blob",
            1,
            False,
            "%252F")

    def test_copy_single_file_from_gcp_to_blob_excludeinvalidmetadata(self):
        self.util_test_copy_single_file_from_gcp_to_blob_handleinvalidmetadata(
            "",
            "1abc=mcdhee;$%^=width;description=test file",
            "description=test file"
        )
    
    def test_copy_single_file_from_gcp_to_blob_renameinvalidmetadata(self):
        self.util_test_copy_single_file_from_gcp_to_blob_handleinvalidmetadata(
            "RenameIfInvalid",
            "1abc=mcdhee;$%^=width;description=test file",
            "rename_1abc=jiac;rename_key_1abc=1abc;description=test file;rename____=width;rename_key____=$%^"
        )

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
        if srcType == "GCP":
            cmd = util.Command("upload").add_arguments(localFilePath).add_arguments(srcURLForCopy).add_flags("serviceType", "GCP")
        else:
            cmd = util.Command("copy").add_arguments(localFilePath).add_arguments(srcURLForCopy).add_flags("log-level", "info")
            if blobType != "" :
                cmd.add_flags("blob-type", blobType)
            if blobType == "PageBlob" and blobTier != "" :
                cmd.add_flags("page-blob-tier", blobTier)
            if blobType == "BlockBlob" and blobTier != "" :
                cmd.add_flags("block-blob-tier", blobTier)
        
        if recursive:
            cmd.add_flags("recursive", True)
        
        if srcType == "GCP":
            result = cmd.execute_testsuite_upload()
        else:
            result = cmd.execute_azcopy_copy_command()
        self.assertTrue(result)

    def util_test_copy_single_file_from_x_to_x(self, 
        srcBucketURL,
        srcType,
        dstBucketURL,
        dstType,
        sizeInKB=1,
        OAuth=False,
        customizedFileName="",
        srcBlobType="",
        dstBlobType="",
        credTypeOverride=""):
        
        result = util.Command("create").add_arguments(srcBucketURL).add_flags("serviceType", srcType).add_flags("resourceType", "Bucket").execute_azcopy_create()
        self.assertTrue(result)

        if customizedFileName != "":
            filename = customizedFileName
        else:
            filename = "test_" + str(sizeInKB) + "kb_copy.txt"
        
        file_path = util.create_test_file(filename, sizeInKB * 1024)
        
        if srcType == "GCP":
            srcFileURL = util.get_object_without_sas(srcBucketURL, filename)
        else:
            srcFileURL = util.get_object_sas(srcBucketURL, filename)

        if OAuth:
            dstFileURL = util.get_object_without_sas(dstBucketURL, filename)
        else:
            dstFileURL = util.get_object_sas(dstBucketURL, filename)

        self.util_upload_to_src(file_path, srcType, srcFileURL, blobType = srcBlobType)

        if credTypeOverride != "":
            os.environ["AZCOPY_CRED_TYPE"] = credTypeOverride
        
        result = util.Command("copy").add_arguments(srcFileURL).add_arguments(dstFileURL).add_flags("log-level", "info")
        if dstBlobType != "":
            result = result.add_flags("blob-type", dstBlobType)

        r = result.execute_azcopy_copy_command()
        self.assertTrue(r)

        if credTypeOverride != "":
            os.environ["AZCOPY_CRED_TYPE"] = ""

        validate_dir_name = "validate_copy_single_%dKB_file_from_%s_to_%s_%s" % (sizeInKB, srcType, dstType, customizedFileName)
        local_validate_dest_dir = util.create_test_dir(validate_dir_name)
        local_validate_dest = os.path.join(local_validate_dest_dir, filename)
        result = util.Command("copy").add_arguments(dstFileURL).add_arguments(local_validate_dest).add_flags("log-level", "info").execute_azcopy_copy_command()
        self.assertTrue(result)

        result = filecmp.cmp(file_path, local_validate_dest, shallow=False)
        self.assertTrue(result)

    def util_test_copy_n_files_from_x_bucket_to_x_bucket(
        self,
        srcBucketURL,
        srcType,
        dstBucketURL,
        dstType,
        n=10,
        sizeInKB = 1):
        
        result = util.Command("create").add_arguments(srcBucketURL).add_flags("serviceType", srcType). \
            add_flags("resourceType", "Bucket").execute_azcopy_create()
        self.assertTrue(result)

        src_dir_name = "copy_%d_%dKB_files_from_%s_bucket_to_%s_bucket" % (n, sizeInKB, srcType, dstType)
        src_dir_path = util.create_test_n_files(sizeInKB*1024, n, src_dir_name)

        self.util_upload_to_src(src_dir_path, srcType, srcBucketURL, True)

        result = util.Command("copy").add_arguments(srcBucketURL).add_arguments(dstBucketURL). \
            add_flags("log-level", "info").add_flags("recursive", "true").execute_azcopy_copy_command()
        self.assertTrue(result)

        validate_dir_name = "validate_copy_%d_%dKB_files_from_%s_bucket_to_%s_bucket" % (n, sizeInKB, srcType, dstType)
        local_validate_dest = util.create_test_dir(validate_dir_name)
        dst_directory_url = util.get_object_sas(dstBucketURL, src_dir_name)
        result = util.Command("copy").add_arguments(dst_directory_url).add_arguments(local_validate_dest). \
            add_flags("log-level", "info").add_flags("recursive", "true").execute_azcopy_copy_command()
        self.assertTrue(result)

        result = self.util_are_dir_trees_equal(src_dir_path, os.path.join(local_validate_dest, src_dir_name))
        self.assertTrue(result)

    def util_test_copy_n_files_from_gcp_bucket_to_blob_account(
        self,
        srcBucketURL,
        dstAccountURL,
        n=10,
        sizeInKB=1):
        srcType = "GCP"

        result = util.Command("create").add_arguments(srcBucketURL).add_flags("serviceType", srcType). \
            add_flags("resourceType", "Bucket").execute_azcopy_create()
        self.assertTrue(result)

        src_dir_name = "copy_%d_%dKB_files_from_gcp_bucket_to_blob_account" % (n, sizeInKB)
        src_dir_path = util.create_test_n_files(sizeInKB*1024, n, src_dir_name)

        self.util_upload_to_src(src_dir_path, srcType, srcBucketURL, True)

        result = util.Command("copy").add_arguments(srcBucketURL).add_arguments(dstAccountURL). \
            add_flags("log-level", "info").add_flags("recursive", "true").execute_azcopy_copy_command()
        self.assertTrue(result)

        validate_dir_name = "validate_copy_%d_%dKB_files_from_gcp_bucket_to_blob_account" % (n, sizeInKB)
        local_validate_dest = util.create_test_dir(validate_dir_name)
        validateDstBucketURL = util.get_object_sas(dstAccountURL, self.bucket_name)
        dst_directory_url = util.get_object_sas(validateDstBucketURL, src_dir_name)
        result = util.Command("copy").add_arguments(dst_directory_url).add_arguments(local_validate_dest). \
            add_flags("log-level", "info").add_flags("recursive", "true").execute_azcopy_copy_command()
        self.assertTrue(result)

        result = self.util_are_dir_trees_equal(src_dir_path, os.path.join(local_validate_dest, src_dir_name))
        self.assertTrue(result)

    def util_test_copy_file_from_x_bucket_to_x_bucket_strip_top_dir(
        self,
        srcBucketURL,
        srcType,
        dstBucketURL,
        dstType,
        recursive=True):

        result = util.Command("create").add_arguments(srcBucketURL).add_flags("serviceType", srcType). \
            add_flags("resourceType", "Bucket").execute_azcopy_create()
        self.assertTrue(result)

        filename = "copy_strip_top_dir_file.txt"
        file_path = util.create_test_file(filename, 2)
        if srcType == "GCP":
            srcFileURL = util.get_object_without_sas(srcBucketURL, filename)
        else:
            srcFileURL = util.get_object_sas(srcBucketURL, filename)
        src_dir_url = srcFileURL.replace(filename, "*")

        self.util_upload_to_src(file_path, srcType, srcFileURL, False)

        if recursive:
            result = util.Command("copy").add_arguments(src_dir_url).add_arguments(dstBucketURL). \
                add_flags("log-level", "info").add_flags("recursive", "true"). \
                execute_azcopy_copy_command()
        else:
            result = util.Command("copy").add_arguments(src_dir_url).add_arguments(dstBucketURL). \
                add_flags("log-level", "info").execute_azcopy_copy_command()
        self.assertTrue(result)

        validate_dir_name = "validate_copy_file_from_%s_bucket_to_%s_bucket_strip_top_dir_recursive_%s" % (srcType, dstType, recursive)
        local_validate_dest = util.create_test_dir(validate_dir_name)
        dst_file_url = util.get_object_sas(dstBucketURL, filename)
        result = util.Command("copy").add_arguments(dst_file_url).add_arguments(local_validate_dest). \
            add_flags("log-level", "info").add_flags("recursive", "true").execute_azcopy_copy_command()
        self.assertTrue(result)

        result = filecmp.cmp(file_path, os.path.join(local_validate_dest, filename), shallow=False)
        self.assertTrue(result)

    def util_test_copy_n_files_from_x_dir_to_x_dir(self,
        srcBucketURL,
        srcType,
        dstBucketURL,
        dstType,
        n=10,
        sizeInKB=1):
        result = util.Command("create").add_arguments(srcBucketURL).add_flags("serviceType", srcType). \
            add_flags("resourceType", "Bucket").execute_azcopy_create()
        self.assertTrue(result)

        src_dir_name = "copy_%d_%dKB_files_from_%s_dir_to_%s_dir" % (n, sizeInKB, srcType, dstType)
        src_dir_path = util.create_test_n_files(sizeInKB*1024, n, src_dir_name)

        self.util_upload_to_src(src_dir_path, srcType, srcBucketURL, True)

        if srcType == "GCP":
            srcDirURL = util.get_object_without_sas(srcBucketURL, src_dir_name)
        else:
            srcDirURL = util.get_object_sas(srcBucketURL, src_dir_name)

        dstDirURL = util.get_object_sas(dstBucketURL, src_dir_name)

        result = util.Command("copy").add_arguments(srcDirURL).add_arguments(dstDirURL). \
            add_flags("log-level", "info").add_flags("recursive", "true").execute_azcopy_copy_command()
        self.assertTrue(result)

        validate_dir_name = "validate_copy_%d_%dKB_files_from_%s_dir_to_%s_dir" % (n, sizeInKB, srcType, dstType)
        local_validate_dest = util.create_test_dir(validate_dir_name)
        result = util.Command("copy").add_arguments(dstDirURL).add_arguments(local_validate_dest). \
            add_flags("log-level", "info").add_flags("recursive", "true").execute_azcopy_copy_command()
        self.assertTrue(result)

        print(src_dir_path)
        print(os.path.join(local_validate_dest, src_dir_name, src_dir_name))
        result = self.util_are_dir_trees_equal(src_dir_path, os.path.join(local_validate_dest, src_dir_name, src_dir_name))
        self.assertTrue(result)

    def util_test_copy_n_files_from_x_dir_to_x_dir_strip_top_dir(self,
                                                                 srcBucketURL,
                                                                 srcType,
                                                                 dstBucketURL,
                                                                 dstType,
                                                                 n=10,
                                                                 sizeInKB=1,
                                                                 recursive=True):
        result = util.Command("create").add_arguments(srcBucketURL).add_flags("serviceType", srcType). \
            add_flags("resourceType", "Bucket").execute_azcopy_create()
        self.assertTrue(result)

        src_dir_name = "copy_%d_%dKB_files_from_%s_dir_to_%s_dir_recursive_%s" % (n, sizeInKB, srcType, dstType, recursive)
        src_dir_path = util.create_test_n_files(sizeInKB*1024, n, src_dir_name)
        src_sub_dir_name = src_dir_name + "/" + "subdir"
        util.create_test_n_files(sizeInKB*1024,1, src_sub_dir_name)

        self.util_upload_to_src(src_dir_path, srcType, srcBucketURL, True)
        
        if srcType == "GCP":
            src_dir_url = util.get_object_without_sas(srcBucketURL, src_dir_name + "/*")
        else:
            src_dir_url = util.get_object_sas(srcBucketURL, src_dir_name + "/*")

        dstDirURL = util.get_object_sas(dstBucketURL, src_dir_name)
        if recursive:
            result = util.Command("copy").add_arguments(src_dir_url).add_arguments(dstDirURL). \
                add_flags("log-level", "info").add_flags("recursive", "true"). \
                execute_azcopy_copy_command()
            self.assertTrue(result)
        else:
            result = util.Command("copy").add_arguments(src_dir_url).add_arguments(dstDirURL). \
                add_flags("log-level", "info").execute_azcopy_copy_command()
            self.assertTrue(result) 

        validate_dir_name = "validate_copy_%d_%dKB_files_from_%s_dir_to_%s_dir" % (n, sizeInKB, srcType, dstType)
        local_validate_dest = util.create_test_dir(validate_dir_name)
        result = util.Command("copy").add_arguments(dstDirURL).add_arguments(local_validate_dest). \
            add_flags("log-level", "info").add_flags("recursive", "true").execute_azcopy_copy_command()
        self.assertTrue(result)

        if recursive:
            result = self.util_are_dir_trees_equal(src_dir_path, os.path.join(local_validate_dest, src_dir_name))
        else:
            dirs_cmp = filecmp.dircmp(src_dir_path, os.path.join(local_validate_dest, src_dir_name))
            if len(dirs_cmp.left_only) > 0 and len(dirs_cmp.common_files) == n:
                result = True
            else:
                result = False
        self.assertTrue(result)

    def util_test_copy_files_from_x_account_to_x_account(self,
        srcAccountURL,
        srcType,
        dstAccountURL,
        dstType,
        bucketNamePrefix):

        bucketName1 = bucketNamePrefix + "1"
        bucketName2 = bucketNamePrefix + "2"
        if srcType == "GCP":
            src_bucket_url1 = util.get_object_without_sas(srcAccountURL, bucketName1)
            src_bucket_url2 = util.get_object_without_sas(srcAccountURL, bucketName2)
        else:
            src_bucket_url1 = util.get_object_sas(srcAccountURL, bucketName1)
            src_bucket_url2 = util.get_object_sas(srcAccountURL, bucketName2)

        createBucketResult1 = util.Command("create").add_arguments(src_bucket_url1).add_flags("serviceType", srcType). \
            add_flags("resourceType", "Bucket").execute_azcopy_create()
        self.assertTrue(createBucketResult1)

        createBucketResult2 = util.Command("create").add_arguments(src_bucket_url2).add_flags("serviceType", srcType). \
            add_flags("resourceType", "Bucket").execute_azcopy_create()
        self.assertTrue(createBucketResult2)

        src_dir_name1 = "copy_files_from_%s_account_to_%s_account_1" % (srcType, dstType)
        src_dir_path1 = util.create_test_n_files(1*1024, 100, src_dir_name1)
        src_dir_name2 = "copy_files_from_%s_account_to_%s_account_2" % (srcType, dstType)
        src_dir_path2 = util.create_test_n_files(2, 2, src_dir_name2)

        self.util_upload_to_src(src_dir_path1, srcType, src_bucket_url1, True)
        self.util_upload_to_src(src_dir_path2, srcType, src_bucket_url2, True)

        result = util.Command("copy").add_arguments(srcAccountURL).add_arguments(dstAccountURL). \
            add_flags("log-level", "info").add_flags("recursive", "true").execute_azcopy_copy_command()
        self.assertTrue(result)

        validate_dir_name1 = "validate_copy_files_from_%s_account_to_%s_account_1" % (srcType, dstType)
        local_validate_dest1 = util.create_test_dir(validate_dir_name1)    
        dst_container_url1 = util.get_object_sas(dstAccountURL, bucketName1)
        dst_directory_url1 = util.get_object_sas(dst_container_url1, src_dir_name1)
        result = util.Command("copy").add_arguments(dst_directory_url1).add_arguments(local_validate_dest1). \
            add_flags("log-level", "info").add_flags("recursive", "true").execute_azcopy_copy_command()
        self.assertTrue(result)

        result = self.util_are_dir_trees_equal(src_dir_path1, os.path.join(local_validate_dest1, src_dir_name1))
        self.assertTrue(result)

        validate_dir_name2 = "validate_copy_files_from_%s_account_to_%s_account_2" % (srcType, dstType)
        local_validate_dest2 = util.create_test_dir(validate_dir_name2)    
        dst_container_url2 = util.get_object_sas(dstAccountURL, bucketName2)
        dst_directory_url2 = util.get_object_sas(dst_container_url2, src_dir_name2)
        result = util.Command("copy").add_arguments(dst_directory_url2).add_arguments(local_validate_dest2). \
            add_flags("log-level", "info").add_flags("recursive", "true").execute_azcopy_copy_command()
        self.assertTrue(result)

        result = self.util_are_dir_trees_equal(src_dir_path2, os.path.join(local_validate_dest2, src_dir_name2))
        self.assertTrue(result)

    def util_test_copy_single_file_from_x_to_x_propertyandmetadata(
        self,
        srcBucketURL,
        srcType,
        dstBucketURL,
        dstType,
        preserveProperties=True):
        result = util.Command("create").add_arguments(srcBucketURL).add_flags("serviceType", srcType). \
            add_flags("resourceType", "Bucket").execute_azcopy_create()
        self.assertTrue(result)

        fileName = "single_file_propertyandmetadata_%s" % (preserveProperties)

        if srcType == "GCP":
            srcFileURL = util.get_object_without_sas(srcBucketURL, fileName)
        else:
            srcFileURL = util.get_object_sas(srcBucketURL, fileName)

        dstFileURL = util.get_object_sas(dstBucketURL, fileName)
        result = util.Command("create").add_arguments(srcFileURL).add_flags("serviceType", srcType). \
            add_flags("resourceType", "SingleFile"). \
            add_flags("metadata", "author=mcdhee;viewport=width;description=test file"). \
            add_flags("content-type", "testctype").add_flags("content-encoding", "testenc"). \
            add_flags("content-disposition", "testcdis").add_flags("content-language", "en").\
            add_flags("cache-control", "testcc").execute_azcopy_create()
        self.assertTrue(result)

        cpCmd = util.Command("copy").add_arguments(srcFileURL).add_arguments(dstFileURL). \
            add_flags("log-level", "info")
        if preserveProperties == False:
            cpCmd.add_flags("s2s-preserve-properties", "false")

        result = cpCmd.execute_azcopy_copy_command()
        self.assertTrue(result)

        validate_dir_name = "validate_copy_single_file_from_%s_to_%s_propertyandmetadata_%s" % (srcType, dstType, preserveProperties)
        local_validate_dest_dir = util.create_test_dir(validate_dir_name)
        local_validate_dest = local_validate_dest_dir + fileName
        if srcType == "GCP":
            result = util.Command("copy").add_arguments(dstFileURL).add_arguments(local_validate_dest). \
                add_flags("log-level", "info").execute_azcopy_copy_command()
        else:
            result = util.Command("copy").add_arguments(srcFileURL).add_arguments(local_validate_dest). \
                add_flags("log-level", "info").execute_azcopy_copy_command()
        self.assertTrue(result)

        testCmdName = "testBlob" if dstType.lower() == "blob" else "testFile"
        validateCmd = util.Command(testCmdName).add_arguments(local_validate_dest).add_arguments(dstFileURL).add_flags("no-guess-mime-type", "true")

        if preserveProperties == True:
            validateCmd.add_flags("metadata", "author=mcdhee;viewport=width;description=test file"). \
            add_flags("content-type", "testctype").add_flags("content-encoding", "testenc"). \
            add_flags("content-disposition", "testcdis").add_flags("content-language", "en"). \
            add_flags("cache-control", "testcc")
        else:
            validateCmd.add_flags("metadata", ""). \
            add_flags("content-type", "").add_flags("content-encoding", ""). \
            add_flags("content-disposition", "").add_flags("content-language", ""). \
            add_flags("cache-control", "")
        
        if srcType != "GCP":
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
        result = util.Command("create").add_arguments(srcBucketURL).add_flags("serviceType", srcType). \
            add_flags("resourceType", "Bucket").execute_azcopy_create()
        self.assertTrue(result)

        fileName = "bucket_file_propertyandmetadata_%s" % (preserveProperties)

        if srcType == "GCP":
            srcFileURL = util.get_object_without_sas(srcBucketURL, fileName)
        else:
            srcFileURL = util.get_object_sas(srcBucketURL, fileName)

        dstFileURL = util.get_object_sas(dstBucketURL, fileName)
        result = util.Command("create").add_arguments(srcFileURL).add_flags("serviceType", srcType). \
            add_flags("resourceType", "SingleFile"). \
            add_flags("metadata", "author=mcdhee;viewport=width;description=test file"). \
            add_flags("content-type", "testctype").add_flags("content-encoding", "en"). \
            add_flags("content-disposition", "testcdis").add_flags("content-language", "en").\
            add_flags("cache-control", "testcc").execute_azcopy_create()
        self.assertTrue(result)

        cpCmd = util.Command("copy").add_arguments(srcBucketURL).add_arguments(dstBucketURL). \
            add_flags("log-level", "info").add_flags("recursive", "true")

        if not preserveProperties:
            cpCmd.add_flags("s2s-preserve-properties", "false")

        result = cpCmd.execute_azcopy_copy_command()
        self.assertTrue(result)

        validate_dir_name = "validate_copy_file_from_%s_bucket_to_%s_bucket_propertyandmetadata_%s" % (srcType, dstType, preserveProperties)
        local_validate_dest_dir = util.create_test_dir(validate_dir_name)
        local_validate_dest = local_validate_dest_dir + fileName

        if srcType == "GCP":
            result = util.Command("copy").add_arguments(dstFileURL).add_arguments(local_validate_dest). \
                add_flags("log-level", "info")
            if not preserveProperties:
                result.flags["check-md5"] = "NoCheck"
            result = result.execute_azcopy_copy_command()
        else:
            result = util.Command("copy").add_arguments(srcFileURL).add_arguments(local_validate_dest). \
                add_flags("log-level", "info")
            if not preserveProperties:
                result.flags["check-md5"] = "NoCheck"
            result = result.execute_azcopy_copy_command()
        self.assertTrue(result)

        validateCmd = util.Command("testBlob").add_arguments(local_validate_dest).add_arguments(dstFileURL).add_flags("no-guess-mime-type", "true")

        if preserveProperties == True:
            validateCmd.add_flags("metadata", "author=mcdhee;viewport=width;description=test file"). \
            add_flags("content-type", "testctype").add_flags("content-encoding", "testenc"). \
            add_flags("content-disposition", "testcdis").add_flags("content-language", "en"). \
            add_flags("cache-control", "testcc")
        else:
            validateCmd.add_flags("metadata", ""). \
            add_flags("content-type", "").add_flags("content-encoding", ""). \
            add_flags("content-disposition", "").add_flags("content-language", ""). \
            add_flags("cache-control", "")
        
        if srcType != "GCP":
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
        result = util.Command("create").add_arguments(srcBucketURL).add_flags("serviceType", srcType). \
            add_flags("resourceType", "Bucket").execute_azcopy_create()
        self.assertTrue(result)
        result = util.Command("create").add_arguments(dstBucketURL).add_flags("serviceType", dstType). \
            add_flags("resourceType", "Bucket").execute_azcopy_create()
        self.assertTrue(result)

        fileSize1 = 1
        fileSize2 = 2

        destFileName = "test_copy.txt"
        localFileName1 = "test_" + str(fileSize1) + "kb_copy.txt"
        localFileName2 = "test_" + str(fileSize2) + "kb_copy.txt"
        filePath1 = util.create_test_file(localFileName1, fileSize1*1024)
        filePath2 = util.create_test_file(localFileName2, fileSize2*1024)
        if srcType == "GCP":
            srcFileURL = util.get_object_without_sas(srcBucketURL, localFileName1)
        else:
            srcFileURL = util.get_object_sas(srcBucketURL, localFileName1)

        if oAuth:
            dstFileURL = util.get_object_without_sas(dstBucketURL, destFileName)
        else:
            dstFileURL = util.get_object_sas(dstBucketURL, destFileName)
        
        self.util_upload_to_src(filePath1, srcType, srcFileURL)
        self.util_upload_to_src(filePath2, dstType, dstFileURL)

        cpCmd = util.Command("copy").add_arguments(srcFileURL).add_arguments(dstFileURL). \
            add_flags("log-level", "info")

        if overwrite == False:
            cpCmd.add_flags("overwrite", "false")

        result = cpCmd.execute_azcopy_copy_command()
        self.assertTrue(result)

        validate_dir_name = "validate_overwrite_%s_copy_single_file_from_%s_to_%s" % (overwrite, srcType, dstType)
        local_validate_dest_dir = util.create_test_dir(validate_dir_name)
        local_validate_dest = os.path.join(local_validate_dest_dir, destFileName)
        result = util.Command("copy").add_arguments(dstFileURL).add_arguments(local_validate_dest). \
            add_flags("log-level", "info").execute_azcopy_copy_command()
        self.assertTrue(result)

        if overwrite:
            result = filecmp.cmp(filePath1, local_validate_dest, shallow=False)
        else:
            result = filecmp.cmp(filePath2, local_validate_dest, shallow=False)

        self.assertTrue(result)

    def util_test_copy_single_file_from_gcp_to_blob_handleinvalidmetadata(
        self, 
        invalidMetadataHandleOption,
        srcGCPMetadata, 
        expectResolvedMetadata):
        srcBucketURL = util.get_object_without_sas(util.test_s2s_src_gcp_service_url, self.bucket_name)
        dstBucketURL = util.get_object_sas(util.test_s2s_dst_blob_account_url, self.bucket_name)
        srcType = "GCP"

        # create bucket and create file with metadata and properties
        result = util.Command("create").add_arguments(srcBucketURL).add_flags("serviceType", srcType). \
            add_flags("resourceType", "Bucket").execute_azcopy_create()
        self.assertTrue(result)

        fileName = "test_copy_single_file_from_gcp_to_blob_handleinvalidmetadata_%s" % invalidMetadataHandleOption

        srcFileURL = util.get_object_without_sas(srcBucketURL, fileName)

        dstFileURL = util.get_object_sas(dstBucketURL, fileName)
        result = util.Command("create").add_arguments(srcFileURL).add_flags("serviceType", srcType). \
            add_flags("resourceType", "SingleFile"). \
            add_flags("metadata", srcGCPMetadata). \
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
        validate_dir_name = "validate_copy_single_file_from_gcp_to_blob_handleinvalidmetadata_%s" % invalidMetadataHandleOption
        local_validate_dest_dir = util.create_test_dir(validate_dir_name)
        local_validate_dest = local_validate_dest_dir + fileName
        result = util.Command("copy").add_arguments(dstFileURL).add_arguments(local_validate_dest). \
            add_flags("log-level", "info").execute_azcopy_copy_command()
        self.assertTrue(result)

        validateCmd = util.Command("testBlob").add_arguments(local_validate_dest).add_arguments(dstFileURL).add_flags("no-guess-mime-type", "true"). \
            add_flags("metadata", expectResolvedMetadata)

        result = validateCmd.execute_azcopy_verify()
        self.assertTrue(result)
