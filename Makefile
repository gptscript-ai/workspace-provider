.DEFAULT_GOAL := build

all: build

build-exe:
	GOOS=windows go build -o bin/workspace-provider.exe -tags "${GO_TAGS}" .

build:
	CGO_ENABLED=0 go build -o bin/workspace-provider -tags "${GO_TAGS}" -ldflags "-s -w" .

tidy:
	go mod tidy

test:
	go test -v ./pkg/...

GOLANGCI_LINT_VERSION ?= v1.60.1
lint:
	if ! command -v golangci-lint &> /dev/null; then \
  		echo "Could not find golangci-lint, installing version $(GOLANGCI_LINT_VERSION)."; \
		curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $$(go env GOPATH)/bin $(GOLANGCI_LINT_VERSION); \
	fi

	golangci-lint run


validate: tidy lint
	if [ -n "$$(git status --porcelain)" ]; then \
		git status --porcelain; \
		echo "Encountered dirty repo!"; \
		git diff; \
		exit 1 \
	;fi

