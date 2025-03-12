# Create container based on Ubuntu-22.04 Jammy Jellyfish image
FROM mcr.microsoft.com/mirror/docker/library/ubuntu:22.04 

# Copy azcopy binary to executable path
COPY ./azcopy /usr/local/bin/

# Make azcopy executable
RUN chmod +x /usr/local/bin/azcopy

WORKDIR /azcopy
CMD [ "azcopy" ]