# Create container based on Alpine image
FROM alpine:latest

# Install necessary dependencies
RUN apk add --no-cache bash curl

# Copy azcopy binary to executable path
COPY ./azcopy /usr/local/bin/

# Make azcopy executable
RUN chmod +x /usr/local/bin/azcopy

# Set the working directory
WORKDIR /WORKDIR

# Command to run azcopy
CMD ["azcopy"]
