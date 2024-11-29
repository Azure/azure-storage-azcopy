# Cleanup old installation
sudo apt-get remove -y docker docker-engine docker.io containerd runc

#Update package index
sudo apt-get update
sudo apt-get upgrade -y

# Install required dependencies
sudo apt-get install -y ca-certificates curl gnupg lsb-release

# Add Docker's Official GPG Key
sudo mkdir -m 0755 -p /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg

# Set Up Docker's APT Repository
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

# Install Docker Engine
sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin

# Verify docker installation
docker --version
sudo docker run hello-world
sudo systemctl enable docker

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

