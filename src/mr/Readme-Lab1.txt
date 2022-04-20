go run -race mrcoordinator.go pg*.txt

go build -race -buildmode=plugin  ../mrapps/XXX.go
go run -race mrworker.go XXX.so

rm -f mr-*