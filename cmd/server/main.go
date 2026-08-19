// Package main is the entry point for the Forest Database Engine server.
// It initializes the storage engine, sets up the TCP network listener,
// and manages graceful shutdown procedures.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/Forest_DatabaseEngine/internal/engine"
	"github.com/Forest_DatabaseEngine/internal/network"
)

func main() {
	// Parse command line flags for server configuration.
	portFlag := flag.Int("port", 9000, "TCP port to listen on for incoming client connections")
	walDirFlag := flag.String("wal-dir", "data/wal", "Directory path for Write-Ahead Log (WAL) files")
	// sstDirFlag is provided to match the CLI usage from the README.
	// Currently unused by engine.NewDB, but available for future use.
	_ = flag.String("sst-dir", "data/sst", "Directory path for Sorted String Table (SSTable) files")
	flag.Parse()

	// Ensure the WAL directory exists before initializing the engine.
	if err := os.MkdirAll(*walDirFlag, 0755); err != nil {
		log.Fatalf("failed to create WAL directory: %v", err)
	}

	walPath := fmt.Sprintf("%s/forest.wal", *walDirFlag)

	// Initialize the storage engine.
	dbEngine, err := engine.NewDB(walPath)
	if err != nil {
		log.Fatalf("failed to initialize storage engine at %s: %v", walPath, err)
	}

	// Initialize the TCP network server.
	addr := fmt.Sprintf("127.0.0.1:%d", *portFlag)
	tcpServer := network.NewServer(addr, dbEngine)

	// Handle graceful shutdown via OS signals.
	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
		<-sigChan

		log.Println("Received termination signal. Shutting down database...")

		// Stop accepting new TCP connections.
		if err := tcpServer.Stop(); err != nil {
			log.Printf("error stopping TCP server: %v", err)
		}

		// Flush remaining MemTables and close WAL.
		if err := dbEngine.Close(); err != nil {
			log.Printf("error closing storage engine: %v", err)
		}

		log.Println("Database shut down successfully.")
		os.Exit(0)
	}()

	// Start accepting connections. This call blocks indefinitely.
	if err := tcpServer.Start(); err != nil {
		log.Fatalf("TCP server encountered a fatal error: %v", err)
	}
}
