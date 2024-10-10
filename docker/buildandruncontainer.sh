# Build azcopy binary
./dockerinstall.sh
./buildcontainer.sh Dockerfile ubuntu-x86_64

# Fetch the version of azcopy and extract the version number
azcopy_version=$(../azcopy --version | awk '{print $3}')

# Construct the Docker image tag using the fetched version
docker_image_tag="azure-azcopy-ubuntu-x86_64.$azcopy_version"

# If build was successful then launch a container instance
status=`docker images | grep $docker_image_tag`

curr_dir=`pwd`
mkdir -p $curr_dir/azcopy
echo "Hello World" > $curr_dir/azcopy/hello.txt

if [ $? = 0 ]; then
    echo " **** Build successful, running container now ******"
    
    # Debug: Check the tag being used
    echo "Using Docker image: $docker_image_tag"

    docker run -it --rm \
        -v $curr_dir/azcopy:/azcopy \
        $docker_image_tag azcopy --help
else
    echo "Failed to build docker image"
fi
