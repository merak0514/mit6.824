# shellcheck disable=SC2046
kill -9 $(lsof -i :1234|sed -n '2p'|awk '{print $2}')
go build -race -buildmode=plugin ../mrapps/wc.go
go run -race mrcoordinator.go pg-*.txt&
sleep 5
go run -race mrworker.go wc.so&
sleep 1
go run -race mrworker.go wc.so
sleep 5
kill -9 $(lsof -i :1234|sed -n '2p'|awk '{print $2}')
