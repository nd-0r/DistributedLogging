set -e

go test andreworals.com/dlogger/chan_grep

go build -o ./dgrep/dgrep ./dgrep/main.go
go build -o ./logservent/logservent ./logservent/main.go

python3 ./tests/test_dgrep.py ./logservent/logservent ./dgrep/dgrep 

