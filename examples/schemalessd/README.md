# go-schemaless/examples/apiserver

go-schemaless/examples/apiserver is an API server for demonstrating the basic
architecture and functionality of go-schemaless.

# Installation

Use ```go get``` to install it.

```bash
go get github.com/rbastic/go-schemaless/examples/apiserver
```

The above will error if you have not satisfied the required dependencies.

# Installing dependencies

# Usage

# Contributing

Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

# Usage

## Building

## Linters

This repository is (manually or automatically) linted using golangci-lint. I
installed it locally with the following:

```bash
go get github.com/golangci/golangci-lint
cd ~/go/src/github.com/golangci/golangci-lint
cd cmd/
go build 
go install
```
And then invoke it as follows:

```bash
cd go/src/github.com/rbastic/go-schemaless/examples/apiserver
~/go/bin/golangci-lint run --exclude=vendor
```
The code is also linted using golint with the following invocation:

```bash
cd go/src/github.com/rbastic/go-schemaless/examples/apiserver
golint ./...
```

Only complaints about missing comments on exported methods/types
should appear for now. (This should be resolved soon as the code
and patterns stabilize.)

## Deployment

One can test and run locally after running 'make build'.

Running 'make build' produces a directory 'sandbox' suitable for creating a
deployment tarball using a routine 'tar -czvf go-schemaless/examples/apiserver.tgz sandbox/' style of
command.

Some shared libraries may need to be installed on target machines.

# JSON Logging

While the default 'console logger' employed by go-schemaless/examples/apiserver is nicer for developers,
it's not very useful for instrumentation or analytics.

To enable JSON logging, run with:

```bash
$ JSON=1 ./go-schemaless/examples/apiserver
```

Note that both stdout and stderr are employed for output regardless of whether
the json command-line flag is enabled.

# Race detection

To test manually with the race detector enabled:

```bash
CONFIG=cmd/go-schemaless/examples/apiserver/config.json go run -race cmd/go-schemaless/examples/apiserver/go-schemaless/examples/apiserver.go 
```

