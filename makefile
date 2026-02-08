# Makefile

# Export environment variables to be available in shell commands
export MONGO_HOST ?= localhost
export MONGO_USER ?= babylon
export MONGO_PASSWORD ?= babylonpass

export GO111MODULE=on
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

## Quality
check-quality: ## runs code quality checks
	make lint
	make fmt
	make vet

# Append || true below if blocking local developement
lint: ## go linting. Update and use specific lint tool and options
	golangci-lint run

vet: ## go vet
	go vet ./...

fmt: ## runs go formatters
	$(GO_IMPORTS_FMT) -w .
	# go fmt ./...

	$(GO_FMT_STRICT) -l -w .

tidy: ## runs tidy to fix go.mod dependencies
	go mod tidy

## Test
test-ci: ## runs tests and create generates coverage report
	make tidy
	make vendor
	# go test -v -timeout 10m ./... -coverprofile=coverage.out -json > report.json
	go test -v -timeout 10m ./... -coverprofile=coverage.out -json
	go test
unit-test: ## runs unit tests and creates a coverage report
	make tidy
	make vendor
	go test -v -timeout 10m ./... -coverprofile=coverage.out

coverage: ## displays test coverage report in html mode
	make unit-test
	go tool cover -html=coverage.out

## Build
build: ## build the go application
	mkdir -p out/
	go build -o $(APP_EXECUTABLE)
	@echo "Build passed"

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



.PHONY: all test-ci build vendor unit-test
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
