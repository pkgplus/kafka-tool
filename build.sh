#!/bin/bash
version=0.0.2
mkdir -p bin/$version
export CGO_ENABLED=0

# linux_amd64
export GOOS=linux
export GOARCH=amd64
mkdir -p bin/$version/linux_amd64
echo "build bin/$version/${GOOS}_${GOARCH} ..."
go build -o bin/$version/${GOOS}_${GOARCH}/kafka-tool ./main.go
echo "build bin/$version/${GOOS}_${GOARCH} over!"

# linux_arm
export GOOS=linux
export GOARCH=arm
mkdir -p bin/$version/linux_arm
echo "build bin/$version/${GOOS}_${GOARCH} ..."
go build -o bin/$version/${GOOS}_${GOARCH}/kafka-tool ./main.go
echo "build bin/$version/${GOOS}_${GOARCH} over!"

# darwin_amd64
export GOOS=darwin
export GOARCH=amd64
mkdir -p bin/$version/darwin_amd64
echo "build bin/$version/${GOOS}_${GOARCH} ..."
go build -o bin/$version/${GOOS}_${GOARCH}/kafka-tool ./main.go
echo "build bin/$version/${GOOS}_${GOARCH} over!"

# windows_386
export GOOS=windows
export GOARCH=386
mkdir -p bin/$version/windows_386
echo "build bin/$version/${GOOS}_${GOARCH} ..."
go build -o bin/$version/${GOOS}_${GOARCH}/kafka-tool.exe ./main.go
echo "build bin/$version/${GOOS}_${GOARCH} over!"

# windows_386
export GOOS=windows
export GOARCH=amd64
mkdir -p bin/$version/windows_amd64
echo "build bin/$version/${GOOS}_${GOARCH} ..."
go build -o bin/$version/${GOOS}_${GOARCH}/kafka-tool.exe ./main.go
echo "build bin/$version/${GOOS}_${GOARCH} over!"

echo "finish"
