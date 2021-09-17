# Azcopy - azbfs

azbfs is an internal only package for Azcopy that implements REST APIs for HNS enabled accounts.

# Generation
# Azure Data Lakes Gen 2 for Golang
> see [https://aka.ms/autorest](https://aka.ms/autorest)

## [Installation Instructions](https://github.com/Azure/autorest/blob/main/docs/install/readme.md)
1. Install [Node js](https://github.com/nodesource/distributions/blob/master/README.md#installation-instructions) version 12.18.2
2. Install autorest using npm
```bash
sudo apt install npm
npm install -g autorest
# run using command 'autorest' to check if installation worked
autorest --help
``` 

## Generation Instructions.
From the root of azure-storage-azcopy, run the following commands
```bash
cd azbfs
sudo autorest --use=@microsoft.azure/autorest.go@v3.0.63 --input-file=./azure_dfs_swagger_manually_edited.json --go=true --output-folder=./ --namespace=azbfs --go-export-clients=false --file-prefix=zz_generated_
cd ..
gofmt -w azbfs
```