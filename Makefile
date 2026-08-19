.PHONY: build test bench chaos clean

# Build the Forest Database Engine server
build:
	go build -o forest_server ./cmd/server

# Run the standard unit test suite with race detection
test:
	go vet ./...
	go test -v -race ./...

# Run the performance benchmarks to profile zero-allocation read/write paths
bench:
	go test -bench=. -benchmem ./internal/network/...

# Execute the Chaos Engineering test script
chaos: build
	bash ./scripts/chaos_test.sh

# Clean up build artifacts and database files
clean:
	rm -f forest_server
	rm -f *.wal *.sst
	rm -f cpu.out network.test.exe
