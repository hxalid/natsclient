# Project settings
BINARY_NAME=basic
EXAMPLE_PATH=examples/basic

.PHONY: all build run clean tidy

all: build

build:
	go build -o $(EXAMPLE_PATH)/$(BINARY_NAME) $(EXAMPLE_PATH)/main.go

run: build
	./$(EXAMPLE_PATH)/$(BINARY_NAME)

clean:
	rm -f $(EXAMPLE_PATH)/$(BINARY_NAME)

tidy:
	go mod tidy
