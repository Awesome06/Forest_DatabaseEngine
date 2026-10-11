.PHONY: build test bench chaos clean

# Build the Forest Database Engine server
build:
	go build -o bin/forest ./cmd/server

# Run the standard unit test suite with race detection
test:
	go vet ./...
	go test -v -race ./...

# Run the performance benchmarks to profile zero-allocation read/write paths
bench:
	go test -bench=. -benchmem ./...

# Execute the Chaos Engineering test script
chaos: build
	bash ./scripts/chaos_test.sh

# Clean up build artifacts and database files
clean:
	rm -rf bin/ data/
	rm -f forest_server forest_server.exe blaster* verifier*
	rm -f *.wal *.sst *.pem *.crt *.key
	rm -f cpu.out network.test.exe

