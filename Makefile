PROJECT_NAME = azure-storage-azcopy
WORK_DIR = /go/src/github.com/Azure/${PROJECT_NAME}

define with_docker
	WORK_DIR=$(WORK_DIR) docker-compose run $(2) --rm $(PROJECT_NAME) $(1)
endef

login: setup ## get a shell into the container
	WORK_DIR=$(WORK_DIR) docker-compose run --rm --entrypoint /bin/bash $(PROJECT_NAME)

docker-compose:
	which docker-compose

docker-build: docker-compose
	WORK_DIR=$(WORK_DIR) docker-compose build --force-rm

docker-clean: docker-compose
	WORK_DIR=$(WORK_DIR) docker-compose down

setup: clean docker-build ## setup environment for development

build: setup ## build binaries for the project
    # the environment variables need to be passed into the container explicitly
	GOARCH=amd64 GOOS=linux $(call with_docker,go build -o "azcopy_linux_amd64",-e GOARCH -e GOOS)

smoke: setup ## set up smoke test
	$(call with_docker,go build -o test-validator ./testSuite/)

all: setup build smoke ## run all tests

clean: docker-clean ## clean environment and binaries
	rm -rf bin
