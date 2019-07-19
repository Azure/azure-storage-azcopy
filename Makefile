PROJECT_NAME = azure-storage-azcopy
WORK_DIR = /go/src/github.com/Azure/${PROJECT_NAME}

define with_docker
	WORK_DIR=$(WORK_DIR) docker-compose run $(2) --rm $(PROJECT_NAME) $(1)
endef

define with_docker_and_travis_wait
	WORK_DIR=$(WORK_DIR) travis_wait docker-compose run $(2) --rm $(PROJECT_NAME) $(1)
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

test: setup ## run go tests
	ACCOUNT_NAME=$(ACCOUNT_NAME) ACCOUNT_KEY=$(ACCOUNT_KEY) AWS_ACCESS_KEY_ID=$(AWS_ACCESS_KEY_ID) AWS_SECRET_ACCESS_KEY=$(AWS_SECRET_ACCESS_KEY) $(call with_docker_and_travis_wait,go test -race -short -cover ./cmd ./common ./ste ./azbfs, -e ACCOUNT_NAME -e ACCOUNT_KEY -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY)

build: setup ## build binaries for the project
    # the environment variables need to be passed into the container explicitly
	GOARCH=amd64 GOOS=linux $(call with_docker,go build -o "azcopy_linux_amd64",-e GOARCH -e GOOS)

smoke: setup ## set up smoke test
	$(call with_docker,go build -o test-validator ./testSuite/)

all: setup test build smoke ## run all tests

clean: docker-clean ## clean environment and binaries
	rm -rf bin
