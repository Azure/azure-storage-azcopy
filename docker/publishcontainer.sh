

ver=`../azcopy --version | cut -d " " -f 3`
image="azure-azcopy-$3.$ver"

sudo docker login azcopy.azurecr.io --username $1 --password $2

# Publish Ubn-22 container image
sudo docker tag $image:latest azcopy.azurecr.io/$image
sudo docker push azcopy.azurecr.io/$image

sudo docker logout azcopy.azurecr.io

