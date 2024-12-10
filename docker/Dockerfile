ARG GO_VERSION=1.23.1
ARG ALPINE_VERSION=3.21

# Create container based on golang alpine image
FROM golang:$GO_VERSION-alpine$ALPINE_VERSION AS build

# Copy azcopy binary to executable path
COPY ./azcopy /usr/local/bin/

# Make azcopy executable
RUN chmod +x /usr/local/bin/azcopy

# Install dependencies
RUN \
	apt update && \
	apt-get install -y ca-certificates

WORKDIR /azcopy
CMD [ "azcopy" ]