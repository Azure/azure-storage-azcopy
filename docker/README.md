# Steps to Pull an Image from Azure Container Registry:

# 1. Login to Azure

az login

# 2. Login to the Azure Container Registry

az acr login --name azcopycontainers

# 3. List the Images in Your ACR 

az acr repository list --name azcopycontainers --output table

# 4. Pull the Image

docker pull azcopycontainers.azurecr.io/<imagename>:<tag>

# 5. Run the Image

docker run --rm -it -v /local/path/to/mount:/azcopy azcopycontainers.azurecr.io/<imagename>:<tag> azcopy copy <source> <destination>
