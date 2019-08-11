PB = $(wildcard *.proto)
GO = $(PB:.proto=.pb.go)

ifeq ($(OS),Windows_NT)
  LIST_SEPARATOR = ;
else
  LIST_SEPARATOR = :
endif

all: $(GO)

%.pb.go: %.proto
		protoc --proto_path="$(GOPATH)/src$(LIST_SEPARATOR)." --gogofast_out=. $<

clean:
		rm -f *.pb.go
		rm -f *.go
