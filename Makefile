.PHONY: help test build lint fmt bench clean coverage install mod-tidy all

# Colors for output
GREEN  := $(shell tput -Txterm setaf 2)
YELLOW := $(shell tput -Txterm setaf 3)
RESET  := $(shell tput -Txterm sgr0)

# Build variables
BINARY_NAME := ledgerq
BUILD_DIR := bin
CMD_DIR := ./cmd/ledgerq

help: ## Show this help message
	@echo "$(GREEN)Available targets:$(RESET)"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(YELLOW)%-15s$(RESET) %s\n", $$1, $$2}'

all: fmt lint test build ## Run fmt, lint, test, and build

test: ## Run tests with race detector
	@echo "$(GREEN)Running tests...$(RESET)"
	go test -race -v -timeout 30s ./...

build: ## Build the CLI binary
	@echo "$(GREEN)Building $(BINARY_NAME)...$(RESET)"
	@mkdir -p $(BUILD_DIR)
	go build -o $(BUILD_DIR)/$(BINARY_NAME) $(CMD_DIR)

lint: ## Run golangci-lint
	@echo "$(GREEN)Running linters...$(RESET)"
	@which golangci-lint > /dev/null || (echo "$(YELLOW)golangci-lint not installed. Install from https://golangci-lint.run/usage/install/$(RESET)" && exit 1)
	golangci-lint run

fmt: ## Format code with gofmt and goimports
	@echo "$(GREEN)Formatting code...$(RESET)"
	gofmt -s -w .
	@which goimports > /dev/null && goimports -w . || echo "$(YELLOW)goimports not installed. Run: go install golang.org/x/tools/cmd/goimports@latest$(RESET)"

bench: ## Run benchmarks
	@echo "$(GREEN)Running benchmarks...$(RESET)"
	go test -bench=. -benchmem -run=^$$ ./...

coverage: ## Generate test coverage report
	@echo "$(GREEN)Generating coverage report...$(RESET)"
	go test -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html
	@echo "$(GREEN)Coverage report generated: coverage.html$(RESET)"

clean: ## Clean build artifacts and test outputs
	@echo "$(GREEN)Cleaning...$(RESET)"
	rm -rf $(BUILD_DIR)
	rm -f coverage.out coverage.html
	rm -f *.test
	find . -type d -name "*.queue" -exec rm -rf {} + 2>/dev/null || true

install: ## Install the CLI binary
	@echo "$(GREEN)Installing $(BINARY_NAME)...$(RESET)"
	go install $(CMD_DIR)

mod-tidy: ## Tidy Go modules
	@echo "$(GREEN)Tidying Go modules...$(RESET)"
	go mod tidy
