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

dep: docker-build #
	$(call with_docker,dep ensure -v)

setup: clean docker-build dep ## setup environment for development

test: setup ## run go tests
	ACCOUNT_NAME=$(ACCOUNT_NAME) ACCOUNT_KEY=$(ACCOUNT_KEY) $(call with_docker,go test -race -short -cover ./cmd ./common ./ste ./azbfs, -e ACCOUNT_NAME -e ACCOUNT_KEY)

build: setup ## build binaries for the project
    # the environment variables need to be passed into the container explicitly
	GOARCH=amd64 GOOS=linux $(call with_docker,go build -o "azcopy_linux_amd64",-e GOARCH -e GOOS)
	GOARCH=amd64 GOOS=linux $(call with_docker,go build -tags "se_integration" -o "azcopy_linux_se_amd64",-e GOARCH -e GOOS)
	GOARCH=amd64 GOOS=windows $(call with_docker,go build -o "azcopy_windows_amd64.exe",-e GOARCH -e GOOS)
	GOARCH=386 GOOS=windows $(call with_docker,go build -o "azcopy_windows_386.exe",-e GOARCH -e GOOS)

build-osx: setup ## build osx binary specially, as it's using CGO
	CC=o64-clang CXX=o64-clang++ GOOS=darwin GOARCH=amd64 CGO_ENABLED=1 $(call with_docker,go build -o "azcopy_darwin_amd64",-e CC -e CXX -e GOOS -e GOARCH -e CGO_ENABLED)

smoke: setup ## set up smoke test
	$(call with_docker,go build -o test-validator ./testSuite/)

all: setup test build build-osx smoke ## run all tests and lints

## unused for now
clean: docker-clean ## clean environment and binaries
	rm -rf bin

vet: setup ## run go vet
	$(call with_docker,go vet ./...)

lint: setup ## run go lint
	$(call with_docker,golint -set_exit_status ./...)

help: ## display this help screen
	@grep -h -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
