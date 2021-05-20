export GO111MODULE := off

.PHONY: install-tools
install-tools:
	go get -u honnef.co/go/tools/cmd/staticcheck
	go get -u github.com/client9/misspell/cmd/misspell

.PHONY: lint
lint: goPkgs := $(shell GO111MODULE=$(GO111MODULE) go list ./...)
lint:
	if [ -n "$$(gofmt -l .)" ]; then exit 1; fi
	find . -type f -name "*.go" | grep -v "/vendor/" | xargs misspell -error -locale US
	go vet $(goPkgs)
	staticcheck $(goPkgs)

.PHONY: test-no-race
test-no-race:
	if [ "$$(ulimit -n)" -lt "8192" ]; then exit 1; fi
	go test -v -run=TestNoRace -failfast -p=1 ./...
.PHONY: test
test:
	if [ "$$(ulimit -n)" -lt "8192" ]; then exit 1; fi
	go test -v -race -p=1 -failfast ./...
.PHONY: test-tag
test-tag:
	if [ "$$(ulimit -n)" -lt "8192" ]; then exit 1; fi
	go test -v -run=TestVersionMatchesTag ./server
