#!/bin/bash

echo -n 'Azure storage account name (with hierarchical namespace disabled):'
read accountName

echo -n 'Azure storage account key (with hierarchical namespace disabled):'
read accountKey

echo -n 'Azure storage account hns name (with hierarchical namespace enabled):'
read hnsAccountName

echo -n 'Azure storage account hns key (with hierarchical namespace enabled):'
read hnsAccountKey

echo -n 'SMB mount path:'
read smbRemotePath

echo -n 'SMB user name:'
read smbUserName

echo -n 'SMB password:'
read smbPassword

echo -n 'Tenant Id:'
read tenantId

echo -n 'App registration Application Id:'
read applicationId

echo -n 'App registration secret value:'
read secretValue

encrypitonKey=$(uuidgen)
sha256Hash=$(echo -n $encrypitonKey | shasum -a 256)

echo 'AZCOPY_E2E_ACCOUNT_KEY="'$accountKey'"'  >> /etc/environment
echo 'AZCOPY_E2E_ACCOUNT_NAME="'$accountName'"'  >> /etc/environment
echo 'AZCOPY_E2E_ACCOUNT_KEY_HNS="'$hnsAccountKey'"'  >> /etc/environment
echo 'AZCOPY_E2E_ACCOUNT_NAME_HNS="'$hnsAccountName'"'  >> /etc/environment
echo 'AZCOPY_E2E_SMB_MOUNT_PATH="'$smbRemotePath'"' >> 
echo 'AZCOPY_E2E_TENANT_ID="'$tenantId'"'  >> /etc/environment
echo 'AZCOPY_E2E_APPLICATION_ID="'$applicationId'"'  >> /etc/environment
echo 'AZCOPY_E2E_CLIENT_SECRET="'$secretValue'"'  >> /etc/environment
echo 'AZCOPY_E2E_CLASSIC_ACCOUNT_KEY="'$accountKey'"'  >> /etc/environment
echo 'AZCOPY_E2E_CLASSIC_ACCOUNT_NAME="'$accountName'"'  >> /etc/environment
echo 'CPK_ENCRYPTION_KEY="'$encrypitonKey'"'  >> /etc/environment
echo 'CPK_ENCRYPTION_KEY_SHA256="'$sha256Hash'"'  >> /etc/environment

echo '-------------------Mount SMB---------------------------'

sudo mount -t cifs -o rw,vers=3.0,username=$smbUserName,password=$smbPassword $smbRemotePath /mnt/AzCopyE2ESMB

echo '------------------Build azCopy-------------------------'

GOARCH=amd64 GOOS=linux go build -o azcopy_linux_amd64
echo 'AZCOPY_E2E_EXECUTABLE_PATH='$(pwd)'/azcopy_linux_amd64'  >> /etc/environment

echo '------------------Running E2E Tests -------------------'
go test -v -timeout 60m -race -short -cover ./e2etest
echo '--------------Running E2E Tests Finished---------------'