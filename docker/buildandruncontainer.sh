# Build azcopy binary
cd ..
echo "Building azcopy"
./docker/build.sh
ls -l azcopy

# Build azcopy binary
ver=`../azcopy --version | cut -d " " -f 3`
tag="azure-azcopy.$ver"

./docker/buildcontainer.sh Dockerfile x86_64

# If build was successful then launch a container instance
status=`docker images | grep $tag`

curr_dir=`pwd`
mkdir -p $curr_dir/azcopy
echo "Hello World" > $curr_dir/azcopy/hello.txt

if [ $? = 0 ]; then
	echo " **** Build successful, running container now ******"
	docker run -it --rm \
		-v $curr_dir/azcopy:/azcopy \ 
		$tag
else
	echo "Failed to build docker image"
fi
