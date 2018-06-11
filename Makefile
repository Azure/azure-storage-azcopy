PROJECT_NAME = azure-storage-azcopy
WORK_DIR = /go/src/github.com/Azure/${PROJECT_NAME}
GOX_ARCH = linux/amd64 windows/amd64 darwin/amd64

define with_docker
	WORK_DIR=$(WORK_DIR) docker-compose run --rm $(PROJECT_NAME) $(1)
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
	$(call with_docker,dep ensure)

setup: clean docker-build dep ## setup environment for development

clean: docker-clean ## clean environment and binaries
	rm -rf bin

vet: setup ## run go vet
	$(call with_docker,go vet ./...)

lint: setup ## run go lint
	$(call with_docker,golint -set_exit_status ./...)

test: setup ## run go tests
	$(call with_docker,go test -race -short -cover ./cmd)

build: setup ## build binaries for the project
	$(call with_docker,gox -osarch="$(GOX_ARCH)" ./pkg/...)
	$(call with_docker,gox -output="bin/{{.Dir}}_{{.OS}}_{{.Arch}}" -osarch="$(GOX_ARCH)" ./cmd/...)

all: setup test build ## run all tests and lints

help: ## display this help screen
	@grep -h -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
