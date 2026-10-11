// Package main is the entry point for the Forest Database Engine server.
// It initializes the storage engine, sets up the dual-protocol network listener (TCP/mTLS and UDP),
// exposes the Prometheus /metrics endpoint, and manages graceful shutdown procedures.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Forest_DatabaseEngine/internal/engine"
	"github.com/Forest_DatabaseEngine/internal/metrics"
	"github.com/Forest_DatabaseEngine/internal/network"
)

func main() {
	// Parse command line flags for server configuration.
	portFlag := flag.Int("port", 9000, "TCP port to listen on for incoming client connections")
	udpPortFlag := flag.Int("udp-port", 9001, "UDP port to listen on for loss-tolerant telemetry datagrams (0 to disable)")
	metricsPortFlag := flag.Int("metrics-port", 2112, "HTTP port to expose Prometheus /metrics endpoint (0 to disable)")
	walDirFlag := flag.String("wal-dir", "data/wal", "Directory path for Write-Ahead Log (WAL) files")
	sstDirFlag := flag.String("sst-dir", "data/sst", "Directory path for Sorted String Table (SSTable) files")

	// TLS / mTLS configuration flags
	tlsCertFlag := flag.String("tls-cert", "", "Path to PEM certificate file for mTLS")
	tlsKeyFlag := flag.String("tls-key", "", "Path to PEM private key file for mTLS")
	tlsCAFlag := flag.String("tls-ca", "", "Path to PEM root CA certificate for client certificate validation")
	genCertsFlag := flag.Bool("gen-certs", false, "Generate self-signed development mTLS certificates (ca.crt, server.crt/key, client.crt/key) and exit")

	flag.Parse()

	// If --gen-certs flag is set, generate development certificates and exit.
	if *genCertsFlag {
		if err := network.GenerateAndSaveDevCerts("."); err != nil {
			log.Fatalf("failed to generate development mTLS certificates: %v", err)
		}
		log.Println("Generated development mTLS certificates successfully in current directory:")
		log.Println("  - ca.crt (CA certificate)")
		log.Println("  - server.crt, server.key (Server TLS certificate pair)")
		log.Println("  - client.crt, client.key (Client TLS certificate pair)")
		return
	}

	// Ensure the WAL directory exists before initializing the engine.
	if err := os.MkdirAll(*walDirFlag, 0755); err != nil {
		log.Fatalf("failed to create WAL directory: %v", err)
	}

	walPath := fmt.Sprintf("%s/forest.wal", *walDirFlag)

	// Initialize the storage engine with configured SSTable directory.
	dbEngine, err := engine.NewDB(walPath, engine.WithSSTDir(*sstDirFlag))
	if err != nil {
		log.Fatalf("failed to initialize storage engine at %s: %v", walPath, err)
	}

	// Prepare network options
	var serverOpts []network.ServerOption

	if *udpPortFlag > 0 {
		udpAddr := fmt.Sprintf("127.0.0.1:%d", *udpPortFlag)
		serverOpts = append(serverOpts, network.WithUDP(udpAddr))

		// Process incoming UDP telemetry and heartbeats
		serverOpts = append(serverOpts, network.WithTelemetryHandler(func(op network.OpCode, payload []byte, from net.Addr) {
			switch op {
			case network.OpHeartbeat:
				log.Printf("Telemetry: Node heartbeat received from %s", from.String())
			case network.OpTelemetry:
				log.Printf("Telemetry: Metric report (%d bytes) from %s: %s", len(payload), from.String(), string(payload))
			}
		}))
	}

	if *tlsCertFlag != "" && *tlsKeyFlag != "" {
		tlsConfig, err := network.LoadServerTLSConfig(*tlsCertFlag, *tlsKeyFlag, *tlsCAFlag)
		if err != nil {
			log.Fatalf("failed to load TLS configuration: %v", err)
		}
		serverOpts = append(serverOpts, network.WithTLS(tlsConfig))
		log.Println("Mutual TLS (mTLS) enabled with two-way certificate authentication.")
	}

	// Initialize the dual-protocol network server.
	tcpAddr := fmt.Sprintf("127.0.0.1:%d", *portFlag)
	netServer := network.NewServer(tcpAddr, dbEngine, serverOpts...)

	// Start Prometheus /metrics HTTP server if enabled
	var metricsServer *http.Server
	if *metricsPortFlag > 0 {
		metricsAddr := fmt.Sprintf("127.0.0.1:%d", *metricsPortFlag)
		mux := http.NewServeMux()
		mux.Handle("/metrics", metrics.DefaultEngineMetrics.Registry.Handler())
		metricsServer = &http.Server{
			Addr:    metricsAddr,
			Handler: mux,
		}

		go func() {
			log.Printf("Forest Observability: Prometheus endpoint listening on http://%s/metrics\n", metricsAddr)
			if err := metricsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				log.Printf("metrics HTTP server exited: %v", err)
			}
		}()
	}

	// Handle graceful shutdown via OS signals.
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("Received termination signal. Shutting down database...")

		if metricsServer != nil {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_ = metricsServer.Shutdown(shutdownCtx)
		}

		// Stop network listeners (TCP/TLS and UDP)
		if err := netServer.Stop(); err != nil {
			log.Printf("error stopping network server: %v", err)
		}

		// Flush remaining MemTables and close WAL.
		if err := dbEngine.Close(); err != nil {
			log.Printf("error closing storage engine: %v", err)
		}

		log.Println("Database shut down successfully.")
		os.Exit(0)
	}()

	// Start accepting connections. This call blocks indefinitely.
	if err := netServer.Start(); err != nil {
		log.Fatalf("Network server encountered a fatal error: %v", err)
	}
}
