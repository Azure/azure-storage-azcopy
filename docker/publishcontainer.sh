ver=`../azcopy --version | cut -d " " -f 3`
image="azure-azcopy-$3.$ver"

sudo docker login azcopycontainers.azurecr.io --username $1 --password $2

# Publish Ubn-22 container image
sudo docker tag $image:latest azcopycontainers.azurecr.io/$image
sudo docker push azcopycontainers.azurecr.io/$image

sudo docker logout azcopycontainers.azurecr.io

