build:
	CGO_ENABLED=0 go build -o bin/rdsrecorder cmd/rdsrecorder/main.go

build_production:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o ./rdsrecorder ./cmd/rdsrecorder/main.go

clean:
	rm bin/pg*

clean_build: clean build

test:
	go test ./...
