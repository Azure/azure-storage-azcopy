
# Build azcopy binary
cd ..
echo "Building azcopy"
./docker/build.sh
ls -l azcopy

ver=`./azcopy --version | cut -d " " -f 3`
tag="azure-azcopy-$2.$ver"

# Cleanup older container image from docker
sudo docker image rm $tag -f

# Build new container image using current code
echo "Build container for azcopy"
cd -
cp ../azcopy ./azcopy
sudo docker build -t $tag -f $1 .

# List all images to verify if new image is created
sudo docker images

# Image build is executed so we can clean up temp executable from here
rm -rf ./azcopy

# If build was successful then launch a container instance
status=`sudo docker images | grep $tag`
echo $status
