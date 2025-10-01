# Cleanup old installation
sudo apt remove -y docker-desktop
sudo apt-get remove -y docker docker-engine docker.io containerd runc
rm -r $HOME/.docker/desktop
sudo rm /usr/local/bin/com.docker.cli
sudo apt purge -y docker-desktop
sudo apt-get update

# Install certificates and pre-requisites
sudo apt-get install -y ca-certificates curl gnupg lsb-release

# Create keyring for docker
sudo mkdir -m 0755 -p /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg

# Create file for installation
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

# Install docker
sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin

# Verify installation
docker --version
sudo docker run hello-world


# Delete old azcopy image
docker rmi `docker images | grep azcopy | cut -d " " -f1`

# Remove existing images
docker system prune -f -a

# Start docker service
sudo service docker start

# List docker container images
docker images ls

# List docker instances running
docker container ls

