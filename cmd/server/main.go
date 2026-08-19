package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/Forest_DatabaseEngine/internal/engine"
	"github.com/Forest_DatabaseEngine/internal/network"
)

func main() {
	db, err := engine.NewDB("forest.wal")
	if err != nil {
		log.Fatalf("Failed to initialize engine: %v", err)
	}

	server := network.NewServer("127.0.0.1:9000", db)

	// Handle graceful shutdown
	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
		<-sigChan
		
		log.Println("Shutting down database...")
		db.Close()
		os.Exit(0)
	}()

	if err := server.Start(); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}
