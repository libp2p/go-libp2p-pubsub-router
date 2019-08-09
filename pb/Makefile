PB = $(wildcard *.proto)
GO = $(PB:.proto=.pb.go)

ifeq ($(OS),Windows_NT)
  GOPATH_DELIMITER = \;
else
  GOPATH_DELIMITER = :
endif

all: $(GO)

%.pb.go: %.proto
		protoc --proto_path=$(GOPATH)/src$(GOPATH_DELIMITER). --gogofast_out=. $<

clean:
		rm -f *.pb.go
		rm -f *.go
