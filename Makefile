# Makefile

VERSION     := 0.2.0
LDFLAGS     ?= "-s -w -X github.com/chengshiwen/influx-tool/cmd.Version=$(VERSION) -X github.com/chengshiwen/influx-tool/cmd.GitCommit=$(shell git rev-parse --short HEAD) -X 'github.com/chengshiwen/influx-tool/cmd.BuildTime=$(shell date '+%Y-%m-%d %H:%M:%S')'"
GOBUILD_ENV = GO111MODULE=on CGO_ENABLED=0
GOBUILD     = go build -o bin/influx-tool -a -ldflags $(LDFLAGS)
GOX         = go run github.com/mitchellh/gox
TARGETS     := darwin/amd64 linux/amd64 windows/amd64
DIST_DIRS   := find * -type d -maxdepth 0 -exec

.PHONY: build linux cross-build release test lint down tidy clean

all: build

build:
	$(GOBUILD_ENV) $(GOBUILD)

linux:
	GOOS=linux GOARCH=amd64 $(GOBUILD_ENV) $(GOBUILD)

cross-build: clean
	$(GOBUILD_ENV) $(GOX) -ldflags $(LDFLAGS) -parallel=3 -output="bin/influx-tool-$(VERSION)-{{.OS}}-{{.Arch}}/influx-tool" -osarch='$(TARGETS)' .

release: cross-build
	( \
		cd bin && \
		$(DIST_DIRS) cp ../LICENSE {} \; && \
		$(DIST_DIRS) cp ../README.md {} \; && \
		$(DIST_DIRS) tar -zcf {}.tar.gz {} \; && \
		$(DIST_DIRS) zip -r {}.zip {} \; && \
		$(DIST_DIRS) rm -rf {} \; && \
		sha256sum * > sha256sums.txt \
	)

test:
	go test -v ./...

lint:
	golangci-lint run --enable=golint --disable=errcheck --disable=typecheck && goimports -l -w . && go fmt ./... && go vet ./...

down:
	go list ./... && go mod verify

tidy:
	rm -f go.sum && go mod tidy -v

clean:
	rm -rf bin
