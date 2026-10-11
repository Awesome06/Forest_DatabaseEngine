// Package network provides TLS configuration utilities and mutual TLS (mTLS) authentication
// for secure node-to-node communication in the Forest storage engine.
package network

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"time"
)

var (
	// ErrMissingCertFiles is returned when certificate or private key paths are empty.
	ErrMissingCertFiles = errors.New("certificate and private key paths must be specified")
	// ErrFailedToAppendCA is returned when CA certificate fails to load into the pool.
	ErrFailedToAppendCA = errors.New("failed to parse root CA certificate into cert pool")
)

// LoadServerTLSConfig creates a hardened mTLS server configuration requiring and verifying
// client certificates signed by the provided CA.
func LoadServerTLSConfig(certFile, keyFile, caFile string) (*tls.Config, error) {
	if certFile == "" || keyFile == "" {
		return nil, ErrMissingCertFiles
	}

	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load server certificate pair: %w", err)
	}

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS13,
	}

	if caFile != "" {
		caBytes, err := os.ReadFile(caFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read CA certificate: %w", err)
		}
		caPool := x509.NewCertPool()
		if !caPool.AppendCertsFromPEM(caBytes) {
			return nil, ErrFailedToAppendCA
		}
		tlsConfig.ClientCAs = caPool
		tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
	}

	return tlsConfig, nil
}

// LoadClientTLSConfig creates an mTLS client configuration with mutual authentication.
func LoadClientTLSConfig(certFile, keyFile, caFile string, serverName string) (*tls.Config, error) {
	tlsConfig := &tls.Config{
		MinVersion: tls.VersionTLS13,
		ServerName: serverName,
	}

	if certFile != "" && keyFile != "" {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load client certificate pair: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	if caFile != "" {
		caBytes, err := os.ReadFile(caFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read root CA file: %w", err)
		}
		caPool := x509.NewCertPool()
		if !caPool.AppendCertsFromPEM(caBytes) {
			return nil, ErrFailedToAppendCA
		}
		tlsConfig.RootCAs = caPool
	}

	return tlsConfig, nil
}

// GenerateTestMTLSCredentials generates in-memory self-signed CA, server, and client certificates
// for testing and benchmark suites without disk or external tool dependencies.
func GenerateTestMTLSCredentials() (serverCfg *tls.Config, clientCfg *tls.Config, err error) {
	// 1. Generate Root CA
	caPriv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, fmt.Errorf("failed generating CA private key: %w", err)
	}

	caTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			CommonName:   "Forest Test Root CA",
			Organization: []string{"Forest Storage Engine"},
		},
		NotBefore:             time.Now().Add(-1 * time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
	}

	caBytes, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caPriv.PublicKey, caPriv)
	if err != nil {
		return nil, nil, fmt.Errorf("failed creating CA certificate: %w", err)
	}

	caPool := x509.NewCertPool()
	caCertObj, err := x509.ParseCertificate(caBytes)
	if err != nil {
		return nil, nil, fmt.Errorf("failed parsing CA cert: %w", err)
	}
	caPool.AddCert(caCertObj)

	// 2. Generate Server Certificate
	srvPriv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, fmt.Errorf("failed generating server private key: %w", err)
	}

	srvTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject: pkix.Name{
			CommonName:   "localhost",
			Organization: []string{"Forest Node"},
		},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1"), net.IPv6loopback},
		DNSNames:     []string{"localhost"},
		NotBefore:    time.Now().Add(-1 * time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}

	srvBytes, err := x509.CreateCertificate(rand.Reader, srvTemplate, caTemplate, &srvPriv.PublicKey, caPriv)
	if err != nil {
		return nil, nil, fmt.Errorf("failed creating server cert: %w", err)
	}

	serverCert := tls.Certificate{
		Certificate: [][]byte{srvBytes},
		PrivateKey:  srvPriv,
	}

	// 3. Generate Client Certificate
	clientPriv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, fmt.Errorf("failed generating client key: %w", err)
	}

	clientTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(3),
		Subject: pkix.Name{
			CommonName:   "forest-client-node",
			Organization: []string{"Forest Cluster"},
		},
		NotBefore:    time.Now().Add(-1 * time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}

	clientBytes, err := x509.CreateCertificate(rand.Reader, clientTemplate, caTemplate, &clientPriv.PublicKey, caPriv)
	if err != nil {
		return nil, nil, fmt.Errorf("failed creating client cert: %w", err)
	}

	clientCert := tls.Certificate{
		Certificate: [][]byte{clientBytes},
		PrivateKey:  clientPriv,
	}

	// Server Config with mTLS required
	serverCfg = &tls.Config{
		Certificates: []tls.Certificate{serverCert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    caPool,
		MinVersion:   tls.VersionTLS13,
	}

	// Client Config
	clientCfg = &tls.Config{
		Certificates: []tls.Certificate{clientCert},
		RootCAs:      caPool,
		ServerName:   "localhost",
		MinVersion:   tls.VersionTLS13,
	}

	return serverCfg, clientCfg, nil
}

// WritePEMFiles writes PEM-encoded certificate and private key files to disk (useful for tooling/CLI).
func WritePEMFiles(certFile, keyFile string, certDER []byte, privKey *ecdsa.PrivateKey) error {
	certOut, err := os.Create(certFile)
	if err != nil {
		return fmt.Errorf("failed to create cert file: %w", err)
	}
	defer certOut.Close()
	if err := pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: certDER}); err != nil {
		return fmt.Errorf("failed to write cert pem: %w", err)
	}

	keyBytes, err := x509.MarshalECPrivateKey(privKey)
	if err != nil {
		return fmt.Errorf("failed to marshal ec private key: %w", err)
	}

	keyOut, err := os.OpenFile(keyFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return fmt.Errorf("failed to create key file: %w", err)
	}
	defer keyOut.Close()

	if err := pem.Encode(keyOut, &pem.Block{Type: "EC PRIVATE KEY", Bytes: keyBytes}); err != nil {
		return fmt.Errorf("failed to write key pem: %w", err)
	}

	return nil
}

// GenerateAndSaveDevCerts generates self-signed CA, server, and client certificate/key PEM files
// in the specified directory for development, testing, and benchmarking.
func GenerateAndSaveDevCerts(outDir string) error {
	if err := os.MkdirAll(outDir, 0755); err != nil {
		return fmt.Errorf("failed to create cert output directory: %w", err)
	}

	caCertPath := filepath.Join(outDir, "ca.crt")
	caKeyPath := filepath.Join(outDir, "ca.key")
	serverCertPath := filepath.Join(outDir, "server.crt")
	serverKeyPath := filepath.Join(outDir, "server.key")
	clientCertPath := filepath.Join(outDir, "client.crt")
	clientKeyPath := filepath.Join(outDir, "client.key")

	// 1. Generate Root CA
	caPriv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return fmt.Errorf("failed generating CA private key: %w", err)
	}

	caTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			CommonName:   "Forest Development CA",
			Organization: []string{"Forest Storage Engine"},
		},
		NotBefore:             time.Now().Add(-1 * time.Hour),
		NotAfter:              time.Now().Add(365 * 24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
	}

	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caPriv.PublicKey, caPriv)
	if err != nil {
		return fmt.Errorf("failed creating CA certificate: %w", err)
	}
	if err := WritePEMFiles(caCertPath, caKeyPath, caDER, caPriv); err != nil {
		return err
	}

	// 2. Generate Server Certificate
	srvPriv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return fmt.Errorf("failed generating server private key: %w", err)
	}

	srvTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject: pkix.Name{
			CommonName:   "localhost",
			Organization: []string{"Forest Node"},
		},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1"), net.IPv6loopback},
		DNSNames:     []string{"localhost"},
		NotBefore:    time.Now().Add(-1 * time.Hour),
		NotAfter:     time.Now().Add(365 * 24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}

	srvDER, err := x509.CreateCertificate(rand.Reader, srvTemplate, caTemplate, &srvPriv.PublicKey, caPriv)
	if err != nil {
		return fmt.Errorf("failed creating server certificate: %w", err)
	}
	if err := WritePEMFiles(serverCertPath, serverKeyPath, srvDER, srvPriv); err != nil {
		return err
	}

	// 3. Generate Client Certificate
	clientPriv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return fmt.Errorf("failed generating client private key: %w", err)
	}

	clientTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(3),
		Subject: pkix.Name{
			CommonName:   "forest-client-node",
			Organization: []string{"Forest Cluster"},
		},
		NotBefore:    time.Now().Add(-1 * time.Hour),
		NotAfter:     time.Now().Add(365 * 24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}

	clientDER, err := x509.CreateCertificate(rand.Reader, clientTemplate, caTemplate, &clientPriv.PublicKey, caPriv)
	if err != nil {
		return fmt.Errorf("failed creating client certificate: %w", err)
	}
	return WritePEMFiles(clientCertPath, clientKeyPath, clientDER, clientPriv)
}

