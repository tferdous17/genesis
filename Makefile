# Makefile

# Variables
PROTOC = protoc
PROTOC_FLAGS = --go_out=paths=source_relative:. --go-grpc_out=paths=source_relative:.
PROTO_FILES = proto/data_migration.proto
MAIN_FILE = cmd/main.go

# Default target
all: generate

# Target to generate .pb.go files
generate:
	$(PROTOC) $(PROTOC_FLAGS) $(PROTO_FILES)

# Clean generated files
clean:
	rm -f proto/*.pb.go

# Clean generated files
run: generate
	go run $(MAIN_FILE)

# Add a help target
help:
	@echo "Available targets:"
	@echo "  generate - Generate .pb.go files from .proto files"
	@echo "  clean    - Remove generated .pb.go files"
	@echo "  run      - Stats the server"
