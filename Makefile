demo:
	go get github.com/stratumn/kayak/kayakdemo

test:
	go test -v -race -count=1 github.com/stratumn/kayak/...

testfast:
	go test -v -count=1 -tags fast github.com/stratumn/kayak/...

errcheck:
	go get github.com/kisielk/errcheck

lint: errcheck
	go vet github.com/stratumn/kayak/...
	golint github.com/stratumn/kayak/...
	errcheck github.com/stratumn/kayak/...

.PHONY: test testfast errcheck lint
