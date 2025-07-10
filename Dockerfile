# --- Stage 1: Build AzCopy from source using custom Ubuntu base ---
FROM mcr.microsoft.com/mirror/docker/library/ubuntu:22.04 AS builder
 
# Install Go and dependencies
RUN apt-get update && \
    apt-get install -y curl git ca-certificates build-essential && \
    curl -LO https://golang.org/dl/go1.22.4.linux-amd64.tar.gz && \
    tar -C /usr/local -xzf go1.22.4.linux-amd64.tar.gz && \
    rm go1.22.4.linux-amd64.tar.gz
 
ENV PATH="/usr/local/go/bin:${PATH}"
 
# Set working directory
WORKDIR /azcopy
 
# Copy your own AzCopy source code into the image
COPY . .
 
# Build the binary (adjust `./cmd/azcopy` if needed)
RUN go build -o azcopy .
 
# --- Stage 2: Minimal runtime image ---
FROM ubuntu:22.04
 
# Install runtime dependencies (e.g., CA certs)
RUN apt-get update && \
    apt-get install -y ca-certificates && \
    apt-get clean
 
# Copy the built binary from the builder
COPY --from=builder /azcopy/azcopy /usr/bin/azcopy
 
# ENV CMD=""
# ENV SRC=""
# ENV DST=""
# ENV OPTS=""
 
# ENTRYPOINT ["/bin/sh", "-c", "azcopy \"$CMD\" \"$SRC\" \"$DST\" $OPTS"]