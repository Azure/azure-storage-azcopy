#!/bin/bash

echo -n 'Azure storage account name (with hierarchical namespace disabled):'
read accountName

echo -n 'Azure storage account key (with hierarchical namespace disabled):'
read accountKey

echo -n 'Azure storage account hns name (with hierarchical namespace enabled):'
read hnsAccountName

echo -n 'Azure storage account hns key (with hierarchical namespace enabled):'
read hnsAccountKey

echo -n 'Tenant Id:'
read tenantId

echo -n 'App registration Application Id:'
read applicationId

echo -n 'App registration secret value:'
read secretValue

encrypitonKey=$(uuidgen)
sha256Hash=$(echo -n $encrypitonKey | shasum -a 256)


export AZCOPY_E2E_ACCOUNT_KEY=$accountKey
export AZCOPY_E2E_ACCOUNT_NAME=$accountName
export AZCOPY_E2E_ACCOUNT_KEY_HNS==$hnsAccountKey
export AZCOPY_E2E_ACCOUNT_NAME_HNS=$hnsAccountName
export AZCOPY_E2E_TENANT_ID=$tenantId
export AZCOPY_E2E_APPLICATION_ID=$applicationId
export AZCOPY_E2E_CLIENT_SECRET=$secretValue
export AZCOPY_E2E_CLASSIC_ACCOUNT_KEY=$accountKey
export AZCOPY_E2E_CLASSIC_ACCOUNT_NAME=$accountName
export CPK_ENCRYPTION_KEY=$encrypitonKey
export CPK_ENCRYPTION_KEY_SHA256=$sha256Hash

echo '------------------Build azCopy-------------------------'

GOARCH=amd64 GOOS=linux go build -o azcopy_linux_amd64
export AZCOPY_E2E_EXECUTABLE_PATH=$(pwd)/azcopy_linux_amd64

echo '------------------Running E2E Tests -------------------'
go test -v -timeout 60m -race -short -cover ./e2etest
echo '--------------Running E2E Tests Finished---------------'