FROM golang:1.10

ENV GOPATH /go
ENV PATH ${GOPATH}/bin:$PATH
RUN go get -u github.com/golang/dep/cmd/dep
RUN go get -u golang.org/x/lint/golint

# Prepare enviroment for OSX cross compilation. 
# These steps are referenced from https://github.com/karalabe/xgo/blob/master/docker/base/Dockerfile (licensed with MIT)
# Note: Cross-compile might be considered to be replaced, when MacOS's container is used for testing.
# In that case, OSX's binary can be compiled directly in MacOS.
# For other platform's cross compilation, please refer to https://github.com/karalabe/xgo.

RUN \
  apt-get update && \
  apt-get install -y clang patch xz-utils && \
  apt-get install -y libglib2.0-dev && \
  apt-get install -y libgnome-keyring-dev && \
  apt-get install -y libsecret-1-dev

ENV OSX_SDK     MacOSX10.11.sdk
ENV OSX_NDK_X86 /usr/local/osx-ndk-x86

# Following steps in https://github.com/tpoechtrager/osxcross to prepare the OS X cross toolchain for Linux.
RUN \
  OSX_SDK_PATH=https://s3.dockerproject.org/darwin/v2/$OSX_SDK.tar.xz && \
  wget -q $OSX_SDK_PATH && \
  \
  git clone https://github.com/tpoechtrager/osxcross.git && \
  mv `basename $OSX_SDK_PATH` ./osxcross/tarballs/       && \
  \
  sed -i -e 's|-march=native||g' ./osxcross/build_clang.sh ./osxcross/wrapper/build.sh && \
  UNATTENDED=yes OSX_VERSION_MIN=10.6 ./osxcross/build.sh                              && \
  mv ./osxcross/target $OSX_NDK_X86                                                    && \
  \
  rm -rf ./osxcross

ENV PATH $OSX_NDK_X86/bin:$PATH