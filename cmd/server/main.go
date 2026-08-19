package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/Forest_DatabaseEngine/internal/network"
)

func main() {
	// Listen on port 9000 as agreed
	srv := network.NewServer(":9000")

	// Start server in a background goroutine
	go func() {
		if err := srv.Start(); err != nil {
			log.Fatalf("Server stopped: %v", err)
		}
	}()

	// Wait for termination signal for graceful shutdown
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	log.Println("Shutting down server...")
	if err := srv.Stop(); err != nil {
		log.Fatalf("Error during shutdown: %v", err)
	}
	log.Println("Server successfully stopped.")
}
