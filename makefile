# Makefile

# Export environment variables to be available in shell commands
export MONGO_HOST ?= localhost
export MONGO_USER ?= <MONGO_USER>
export MONGO_PASSWORD ?= <MONGO_PASSWORD>

export GO111MODULE=on
export PATH := $(PATH):/opt/homebrew/bin:/usr/local/bin:$(HOME)/go/bin
# update app name. this is the name of binary
APP=data-loader
APP_EXECUTABLE="./out/$(APP)"
ALL_PACKAGES=$(shell go list ./... | grep -v /vendor)
SHELL := /bin/bash # Use bash syntax


# Optional if you need DB and migration commands
# MONGO_URI=""
# DB_HOST=$(shell cat config/application.yml | grep -m 1 -i HOST | cut -d ":" -f2)
# DB_NAME=$(shell cat config/application.yml | grep -w -i NAME  | cut -d ":" -f2)
# DB_USER=$(shell cat config/application.yml | grep -i USERNAME | cut -d ":" -f2)

# Optional colors to beautify output
GREEN  := $(shell tput -Txterm setaf 2)
YELLOW := $(shell tput -Txterm setaf 3)
WHITE  := $(shell tput -Txterm setaf 7)
CYAN   := $(shell tput -Txterm setaf 6)
RESET  := $(shell tput -Txterm sgr0)

# run goimports formatting from url.
GO_IMPORTS_FMT := $(shell go env GOPATH)/bin/goimports

# use the `gofumpt` package for strict formatting.
GO_FMT_STRICT := $(shell go env GOPATH)/bin/gofumpt

WAILS ?= $(shell which wails 2>/dev/null || echo "$$(go env GOPATH)/bin/wails")

GOLANGCI_LINT ?= golangci-lint


## Quality
check-quality: ## runs code quality checks
	make lint
	make fmt
	make vet

# Append || true below if blocking local developement
lint: ## go linting. Update and use specific lint tool and options
	$(GOLANGCI_LINT) run

vet: ## go vet
	go vet ./...

fmt: ## runs go formatters
	$(GO_IMPORTS_FMT) -w .
	# go fmt ./...

	$(GO_FMT_STRICT) -l -w .

tidy: ## runs tidy to fix go.mod dependencies
	go mod tidy

## Temporary fix for running tests with coverage report.
## Right now these are exactly the same as `unit-test`.
test-ci: ## runs tests and create generates coverage report
	make tidy
	# go test -v -timeout 10m ./... -coverprofile=coverage.out -json > report.json
	go test -v -timeout 10m ./... -coverprofile=coverage.out -json
	go test
unit-test: ## runs unit tests and creates a coverage report
	make tidy
	go test -v -timeout 10m ./... -coverprofile=coverage.out

coverage: ## displays test coverage report in html mode
	make unit-test
	go tool cover -html=coverage.out

## Build
build: ## build the go application
	mkdir -p out/
	go build -o $(APP_EXECUTABLE)
	@echo "Build passed"

## Lambda Targets
build-lambda: ## build static linux/arm64 binary for lambda
	mkdir -p out/
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -ldflags="-s -w" -o out/bootstrap ./cmd/lambda

docker-build-lambda: ## build arm64 lambda container image
	docker build --provenance=false -f Dockerfile.lambda -t babylon-data-loader-lambda:latest .

test-lambda: ## run unit tests for lambda handler, secrets, and storage
	go test -v -race ./cmd/lambda/... ./config/... ./storage/...

AWS_ACCOUNT_ID ?= 615471835001
AWS_REGION     ?= us-west-2
ECR_REPO       ?= ajp/babylon
IMAGE_TAG      ?= data-loader-latest
COMMIT_SHA     ?= $(shell git rev-parse --short=7 HEAD 2>/dev/null || echo "dev")

docker-login-ecr: ## authenticate local Docker CLI to Amazon ECR
	aws ecr get-login-password --region $(AWS_REGION) | docker login --username AWS --password-stdin $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_REGION).amazonaws.com

deploy-ecr: docker-build-lambda docker-login-ecr ## build, tag, and push Lambda container to ECR
	docker tag babylon-data-loader-lambda:latest $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_REGION).amazonaws.com/$(ECR_REPO):$(IMAGE_TAG)
	docker tag babylon-data-loader-lambda:latest $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_REGION).amazonaws.com/$(ECR_REPO):data-loader-sha-$(COMMIT_SHA)
	docker push $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_REGION).amazonaws.com/$(ECR_REPO):$(IMAGE_TAG)
	docker push $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_REGION).amazonaws.com/$(ECR_REPO):data-loader-sha-$(COMMIT_SHA)

# ==============================================================================
# Distribution & Multi-Platform Cross-Compilation
# ==============================================================================

VERSION ?= $(shell git describe --tags --abbrev=0 2>/dev/null || echo "v0.1.0")
COMMIT  ?= $(shell git rev-parse --short=7 HEAD 2>/dev/null || echo "dev")
DIST_DIR := out/dist
LDFLAGS  := -s -w -X main.version=$(VERSION) -X main.commit=$(COMMIT)

.PHONY: clean-dist build-cross package-cross verify-checksums

## Distribution
clean-dist: ## cleans distribution and cross-compilation artifacts
	rm -rf $(DIST_DIR)

build-cross: clean-dist ## cross-compiles static binaries for all supported platforms
	mkdir -p $(DIST_DIR)
	@echo "$(CYAN)==> Cross-compiling for linux/amd64...$(RESET)"
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="$(LDFLAGS)" -o $(DIST_DIR)/$(APP)-linux-amd64 .
	@echo "$(CYAN)==> Cross-compiling for linux/arm64...$(RESET)"
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -trimpath -ldflags="$(LDFLAGS)" -o $(DIST_DIR)/$(APP)-linux-arm64 .
	@echo "$(CYAN)==> Cross-compiling for darwin/amd64...$(RESET)"
	CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 go build -trimpath -ldflags="$(LDFLAGS)" -o $(DIST_DIR)/$(APP)-darwin-amd64 .
	@echo "$(CYAN)==> Cross-compiling for darwin/arm64...$(RESET)"
	CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 go build -trimpath -ldflags="$(LDFLAGS)" -o $(DIST_DIR)/$(APP)-darwin-arm64 .
	@echo "$(GREEN)Cross-compilation successful. Binaries written to $(DIST_DIR)/$(RESET)"

package-cross: build-cross ## packages cross-compiled binaries into tar.gz and generates checksums.txt
	@echo "$(CYAN)==> Packaging distribution tarballs...$(RESET)"
	@for target in linux-amd64 linux-arm64 darwin-amd64 darwin-arm64; do \
		OS=$$(echo $$target | cut -d'-' -f1); \
		ARCH=$$(echo $$target | cut -d'-' -f2); \
		ARCHIVE_NAME="babylon-$(APP)_$(VERSION)_$${OS}_$${ARCH}.tar.gz"; \
		cp $(DIST_DIR)/$(APP)-$$target $(DIST_DIR)/$(APP); \
		chmod +x $(DIST_DIR)/$(APP); \
		tar -czvf $(DIST_DIR)/$$ARCHIVE_NAME -C $(DIST_DIR) $(APP); \
		rm -f $(DIST_DIR)/$(APP); \
		echo "$(GREEN)Created $${ARCHIVE_NAME}$(RESET)"; \
	done
	@echo "$(CYAN)==> Generating checksums.txt (SHA256)...$(RESET)"
	cd $(DIST_DIR) && shasum -a 256 babylon-$(APP)_$(VERSION)_*.tar.gz > checksums.txt
	@echo "$(GREEN)=== Distribution Checksums ===$(RESET)"
	@cat $(DIST_DIR)/checksums.txt

verify-checksums: ## verifies integrity of packaged tarballs against checksums.txt
	@echo "$(CYAN)==> Verifying checksums...$(RESET)"
	cd $(DIST_DIR) && shasum -a 256 -c checksums.txt


## Run
run: run-ingest ## runs the go binary. use additional options if required.

run-ingest: ## runs the go binary to ingest data.
	make build && \
	chmod +x $(APP_EXECUTABLE) && \
	$(APP_EXECUTABLE) ingest

run-generate: ## runs the go binary to generate synthetic data.
	make build && \
	chmod +x $(APP_EXECUTABLE) && \
	$(APP_EXECUTABLE) generate-synthetic-data --rows 100 --dir tmp/synthetic

run-generate-mongo: ## runs the go binary to generate synthetic data and persist to mongo.
	make build && \
	chmod +x $(APP_EXECUTABLE) && \
	$(APP_EXECUTABLE) generate-synthetic-data --rows 100 --persist-to-mongo

## Desktop
run-desktop: ## runs the desktop application in development mode
	cd desktop && $(WAILS) dev

build-desktop: ## builds the desktop application
	cd desktop && $(WAILS) build

run-ui: ## runs the frontend UI development server only (Vite)
	cd desktop/frontend && [ -d node_modules ] || npm install
	cd desktop/frontend && npm run dev

clean: ## cleans binary and other generated files
	go clean
	rm -rf out/
	rm -f coverage*.out

vendor: ## all packages required to support builds and tests in the /vendor directory
	go mod vendor


wire: ## for wiring dependencies (update if using some other DI tool)
	wire ./...

# [Optional] mock generation via go generate
# generate_mocks:
# 	go generate -x `go list ./... | grep - v wire`

# [Optional] Database commands
## Database
migrate: build
	${APP_EXECUTABLE} migrate --config=config/application.test.yml

rollback: build
	${APP_EXECUTABLE} migrate --config=config/application.test.yml



.PHONY: all test-ci build vendor unit-test build-lambda docker-build-lambda test-lambda clean-dist build-cross package-cross verify-checksums
## All
all: ## runs setup, quality checks and builds
	make check-quality
	make unit-test
	make build

.PHONY: help
## Help
help: ## Show this help.
	@echo ''
	@echo 'Usage:'
	@echo '  ${YELLOW}make${RESET} ${GREEN}<target>${RESET}'
	@echo ''
	@echo 'Targets:'
	@awk 'BEGIN {FS = ":.*?## "} { \
		if (/^[a-zA-Z_-]+:.*?##.*$$/) {printf "    ${YELLOW}%-20s${GREEN}%s${RESET}\n", $$1, $$2} \
		else if (/^## .*$$/) {printf "  ${CYAN}%s${RESET}\n", substr($$1,4)} \
		}' $(MAKEFILE_LIST)
