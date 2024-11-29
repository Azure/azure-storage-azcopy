# # Cleanup old installation
# sudo apt remove docker-desktop
# rm -r $HOME/.docker/desktop
# sudo rm /usr/local/bin/com.docker.cli
# sudo apt purge docker-desktop
# sudo apt-get update

# # Install certificates and pre-requisites
# sudo apt-get install ca-certificates curl gnupg lsb-release -y
# sudo mkdir -p /etc/apt/keyrings

# # Create keyring for docker
# curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg -y

# # Create file for installation
# echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable"| sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

# # Install docker 
# sudo apt-get update
# sudo apt-get install docker-ce docker-ce-cli containerd.io docker-compose-plugin -y
# sudo apt-get update

# # Resolve permission issues to connect to docker socket
# sudo groupadd docker
# sudo usermod -aG docker $USER
# sudo chown root:docker /var/run/docker.sock

# # Create the .docker directory if it doesn't exist
# mkdir -p $HOME/.docker
# sudo chown "$USER":"$USER" /home/"$USER"/.docker -R
# sudo chmod g+rwx "$HOME/.docker" -R

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

