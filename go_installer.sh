#!/bin/bash
work_dir=$(echo $1 | sed 's:/*$::')
version=$2
arch=`hostnamectl | grep "Arch" | rev | cut -d " " -f 1 | rev`

# Check for both 64-bit and 32-bit ARM architectures
if [ "$arch" = "arm64" ]; then
  arch="arm64"
elif [ "$arch" = "arm" ] || [ "$arch" = "armv7l" ] || [ "$arch" = "armhf" ]; then
  arch="arm"
else
  arch="amd64"
fi

echo "Installing on : " $arch " Version : " $version
wget "https://golang.org/dl/go$version.linux-$arch.tar.gz" -P "$work_dir"
sudo rm -rf /usr/local/go
sudo tar -C /usr/local -xzf "$work_dir"/go"$version".linux-$arch.tar.gz
sudo ln -sf /usr/local/go/bin/go /usr/bin/go