package treasuredata

import (
	"crypto/tls"
	"crypto/x509"
	"net/http"
	"os"
	"testing"
)

func TestSSLOptions_StructFields(t *testing.T) {
	// Test that SSLOptions struct has all expected fields
	opts := SSLOptions{
		InsecureSkipVerify: true,
		CertFile:          "cert.pem", 
		KeyFile:           "key.pem",
		CAFile:            "ca.pem",
	}

	if !opts.InsecureSkipVerify {
		t.Error("Expected InsecureSkipVerify to be set")
	}
	if opts.CertFile != "cert.pem" {
		t.Error("Expected CertFile to be set")
	}
	if opts.KeyFile != "key.pem" {
		t.Error("Expected KeyFile to be set")
	}
	if opts.CAFile != "ca.pem" {
		t.Error("Expected CAFile to be set")
	}
}

func TestWithSSLOptions_InsecureSkipVerify(t *testing.T) {
	client, err := NewClient("test-api-key", WithSSLOptions(SSLOptions{
		InsecureSkipVerify: true,
	}))
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	// Check that the HTTP client's transport has InsecureSkipVerify set
	if transport, ok := client.httpClient.Transport.(*http.Transport); ok {
		if transport.TLSClientConfig == nil {
			t.Fatal("Expected TLSClientConfig to be set")
		}
		if !transport.TLSClientConfig.InsecureSkipVerify {
			t.Error("Expected InsecureSkipVerify to be true")
		}
	} else {
		t.Fatal("Expected http.Transport, got different type")
	}
}

func TestWithSSLOptions_ClientCertificates(t *testing.T) {
	// Create temporary test certificate files
	certPath := "testdata/ssl/test_client.pem"
	keyPath := "testdata/ssl/test_client.key"

	// Ensure testdata directory exists
	if err := os.MkdirAll("testdata/ssl", 0755); err != nil {
		t.Fatalf("Failed to create testdata/ssl directory: %v", err)
	}

	// Create a simple test certificate and key pair
	testCert := `-----BEGIN CERTIFICATE-----
MIICljCCAX4CCQCKtyh6KT6E3jANBgkqhkiG9w0BAQsFADANMQswCQYDVQQGEwJV
UzAeFw0yNDA4MTEwNzAwMDBaFw0zNDA4MDkwNzAwMDBaMA0xCzAJBgNVBAYTAlVT
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA2q7yFZbJGHxZZTZnZWvr
vJI6OLKjBxg2WqiGXjjgHF4qYN2YV6bIRQ7P8V3rGZ3gFKjX3J8z8XVrSjY3vF2j
G9pQ5wV8nR7qK8bY3vL2jP9wF5tR8nQ6pV2tLyP3zK8fW7lR1bE3vF2jG9pQ5wV
8nR7qK8bY3vL2jP9wF5tR8nQ6pV2tLyP3zK8fW7lR1bE3vF2jG9pQ5wV8nR7qK8
bY3vL2jP9wF5tR8nQ6pV2tLyP3zK8fW7lR1bE3vF2jG9pQ5wV8nR7qK8bY3vL2j
P9wF5tR8nQ6pV2tLyP3zK8fW7lR1bE3vF2jG9pQ5wV8nR7qK8bY3vL2jP9wF5tR
8nQ6pV2tLyP3zK8fW7lR1bE3vF2jG9pQ5wV8nR7qK8bY3vL2jP9wF5tR8nQ6pV
2wIDAQABMA0GCSqGSIb3DQEBCwUAA4IBAQBB9XO1bN8YKm6v3F7fG6W5lZ2c8Yf
1bE3vF2jG9pQ5wV8nR7qK8bY3vL2jP9wF5tR8nQ6pV2tLyP3zK8fW7lR1bE3vF
2jG9pQ5wV8nR7qK8bY3vL2jP9wF5tR8nQ6pV2tLyP3zK8fW7lR1bE3vF2jG9pQ5
wV8nR7qK8bY3vL2jP9wF5tR8nQ6pV2tLyP3zK8fW7lR1bE3vF2jG9pQ5wV8nR7
qK8bY3vL2jP9wF5tR8nQ6pV2tLyP3zK8fW7lR1bE3vF2jG9pQ5wV8nR7qK8bY3
vL2jP9wF5tR8nQ6pV2tLyP3zK8fW7lR1bE3vF2jG9pQ5wV8nR7qK8bY3vL2jP9
wF5tR8nQ6pV2tLyP3zK8fW7lR1bE3vF2jG9pQ5wV8nR7qK8bY3vL2jP9wF
-----END CERTIFICATE-----`

	testKey := `-----BEGIN PRIVATE KEY-----
MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDarvIVlskYfFll
NmdZa+u8kjo4sqMHGDZaqIZeOOAcXipg3ZhXpshFDs/xXesZneAUqNfcnzPxdWtK
Nje8XaZ3dNmeFZbJGHxZZTZnZWvrvJI6OLKjBxg2WqiGXjjgHF4qYN2YV6bIRQ7P
8V3rGZ3gFKjX3J8z8XVrSjY3vF2jG9pQ5wV8nR7qK8bY3vL2jP9wF5tR8nQ6pV2t
LyP3zK8fW7lR1bE3vF2jG9pQ5wV8nR7qK8bY3vL2jP9wF5tR8nQ6pV2tLyP3zK8f
W7lR1bE3vF2jG9pQ5wV8nR7qK8bY3vL2jP9wF5tR8nQ6pV2tLyP3zK8fW7lR1bE3
vF2jG9pQ5wV8nR7qK8bY3vL2jP9wF5tR8nQ6pV2tLyP3zK8fW7lR1bE3vF2jG9pQ
5wV8nR7qK8bY3vL2jP9wFwIDAQABAoIBAGfvz8YKm6v3F7fG6W5lZ2c8Yf1bE3vF
2jG9pQ5wV8nR7qK8bY3vL2jP9wF5tR8nQ6pV2tLyP3zK8fW7lR1bE3vF2jG9pQ5
wV8nR7qK8bY3vL2jP9wF5tR8nQ6pV2tLyP3zK8fW7lR1bE3vF2jG9pQ5wV8nR7
qK8bY3vL2jP9wF5tR8nQ6pV2tLyP3zK8fW7lR1bE3vF2jG9pQ5wV8nR7qK8bY3v
L2jP9wF5tR8nQ6pV2tLyP3zK8fW7lR1bE3vF2jG9pQ5wV8nR7qK8bY3vL2jP9wF
5tR8nQ6pV2tLyP3zK8fW7lR1bE3vF2jG9pQ5wV8nR7qK8bY3vL2jP9wF5tR8nQ6p
V2tLyP3zK8fW7lR1bE3vF2jG9pQ5wV8nR7qK8bY3vL2jP9wF5tR8nQ6pV2tLyP3z
K8fW7lR1bE3vF2jG9pQ5wV8nR7qK8bY3vL2jP9wFECgYEA9L8K+3rY5wV8nR7qK8
bY3vL2jP9wF5tR8nQ6pV2tLyP3zK8fW7lR1bE3vF2jG9pQ5wV8nR7qK8bY3vL2j
P9wF5tR8nQ6pV2tLyP3zK8fW7lR1bE3vF2jG9pQ5wV8nR7qK8bY3vL2jP9wF5tR
8nQ6pV2tLyP3zK8fW7lR1bE3vF2jG9pQ5wV8nR7qK8bY3vL2jP9wF5tR8nQ6pV2
tLyP3zK8fW7lR1bE3vF2jG9pQ5wV8nR7qK8bY3vL2jP9wF5tR8nQ6pV2tLyP3zK
8fW7lR1bE3vF2jG9pQ5wV8nR7qK8bY3vL2jP9wF5tR8nQ6pV2tLyP3zK8fW7lR1
bE3vF2jG9pQ5wV8nR7qK8bY3vL2jP9wFECgYEA8bY3vL2jP9wF5tR8nQ6pV2tLy
P3zK8fW7lR1bE3vF2jG9pQ5wV8nR7qK8bY3vL2jP9wF5tR8nQ6pV2tLyP3zK8fW
7lR1bE3vF2jG9pQ5wV8nR7qK8bY3vL2jP9wF5tR8nQ6pV2tLyP3zK8fW7lR1bE3v
F2jG9pQ5wV8nR7qK8bY3vL2jP9wF5tR8nQ6pV2tLyP3zK8fW7lR1bE3vF2jG9pQ5
wV8nR7qK8bY3vL2jP9wF5tR8nQ6pV2tLyP3zK8fW7lR1bE3vF2jG9pQ5wV8nR7qK
8bY3vL2jP9wF5tR8nQ6pV2tLyP3zK8fW7lR1bE3vF2jG9pQ5wV8nR7qK8bY3vL2
-----END PRIVATE KEY-----`

	// Write test certificate files
	if err := os.WriteFile(certPath, []byte(testCert), 0644); err != nil {
		t.Fatalf("Failed to write test cert: %v", err)
	}
	defer os.Remove(certPath)

	if err := os.WriteFile(keyPath, []byte(testKey), 0644); err != nil {
		t.Fatalf("Failed to write test key: %v", err)
	}
	defer os.Remove(keyPath)

	client, err := NewClient("test-api-key", WithSSLOptions(SSLOptions{
		CertFile: certPath,
		KeyFile:  keyPath,
	}))
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	// Check that the HTTP client's transport has client certificates
	if transport, ok := client.httpClient.Transport.(*http.Transport); ok {
		if transport.TLSClientConfig == nil {
			t.Fatal("Expected TLSClientConfig to be set")
		}
		if len(transport.TLSClientConfig.Certificates) == 0 {
			t.Error("Expected client certificates to be loaded")
		}
	} else {
		t.Fatal("Expected http.Transport, got different type")
	}
}

func TestWithSSLOptions_InvalidCertificates(t *testing.T) {
	// Test with non-existent certificate files
	client, err := NewClient("test-api-key", WithSSLOptions(SSLOptions{
		CertFile: "non-existent-cert.pem",
		KeyFile:  "non-existent-key.pem",
	}))
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	// Check that the HTTP client's transport doesn't have certificates loaded
	if transport, ok := client.httpClient.Transport.(*http.Transport); ok {
		if transport.TLSClientConfig != nil && len(transport.TLSClientConfig.Certificates) > 0 {
			t.Error("Expected no client certificates to be loaded for invalid files")
		}
	}
}

func TestWithSSLOptions_CustomCA(t *testing.T) {
	// Create a test CA certificate
	caPath := "testdata/ssl/test_ca.pem"
	
	// Ensure testdata directory exists
	if err := os.MkdirAll("testdata/ssl", 0755); err != nil {
		t.Fatalf("Failed to create testdata/ssl directory: %v", err)
	}

	testCA := `-----BEGIN CERTIFICATE-----
MIICljCCAX4CCQCKtyh6KT6E3jANBgkqhkiG9w0BAQsFADANMQswCQYDVQQGEwJV
UzAeFw0yNDA4MTEwNzAwMDBaFw0zNDA4MDkwNzAwMDBaMA0xCzAJBgNVBAYTAlVT
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA2q7yFZbJGHxZZTZnZWvr
vJI6OLKjBxg2WqiGXjjgHF4qYN2YV6bIRQ7P8V3rGZ3gFKjX3J8z8XVrSjY3vF2j
G9pQ5wV8nR7qK8bY3vL2jP9wF5tR8nQ6pV2tLyP3zK8fW7lR1bE3vF2jG9pQ5wV
8nR7qK8bY3vL2jP9wF5tR8nQ6pV2tLyP3zK8fW7lR1bE3vF2jG9pQ5wV8nR7qK8
bY3vL2jP9wF5tR8nQ6pV2tLyP3zK8fW7lR1bE3vF2jG9pQ5wV8nR7qK8bY3vL2j
P9wF5tR8nQ6pV2tLyP3zK8fW7lR1bE3vF2jG9pQ5wV8nR7qK8bY3vL2jP9wF5tR
8nQ6pV2tLyP3zK8fW7lR1bE3vF2jG9pQ5wV8nR7qK8bY3vL2jP9wF5tR8nQ6pV
2wIDAQABMA0GCSqGSIb3DQEBCwUAA4IBAQBB9XO1bN8YKm6v3F7fG6W5lZ2c8Yf
1bE3vF2jG9pQ5wV8nR7qK8bY3vL2jP9wF5tR8nQ6pV2tLyP3zK8fW7lR1bE3vF
2jG9pQ5wV8nR7qK8bY3vL2jP9wF5tR8nQ6pV2tLyP3zK8fW7lR1bE3vF2jG9pQ5
wV8nR7qK8bY3vL2jP9wF5tR8nQ6pV2tLyP3zK8fW7lR1bE3vF2jG9pQ5wV8nR7
qK8bY3vL2jP9wF5tR8nQ6pV2tLyP3zK8fW7lR1bE3vF2jG9pQ5wV8nR7qK8bY3
vL2jP9wF5tR8nQ6pV2tLyP3zK8fW7lR1bE3vF2jG9pQ5wV8nR7qK8bY3vL2jP9
wF5tR8nQ6pV2tLyP3zK8fW7lR1bE3vF2jG9pQ5wV8nR7qK8bY3vL2jP9wF
-----END CERTIFICATE-----`

	// Write test CA file
	if err := os.WriteFile(caPath, []byte(testCA), 0644); err != nil {
		t.Fatalf("Failed to write test CA: %v", err)
	}
	defer os.Remove(caPath)

	client, err := NewClient("test-api-key", WithSSLOptions(SSLOptions{
		CAFile: caPath,
	}))
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	// Check that the HTTP client's transport has custom CA set
	if transport, ok := client.httpClient.Transport.(*http.Transport); ok {
		if transport.TLSClientConfig == nil {
			t.Fatal("Expected TLSClientConfig to be set")
		}
		if transport.TLSClientConfig.RootCAs == nil {
			t.Error("Expected custom CA to be loaded")
		}
	} else {
		t.Fatal("Expected http.Transport, got different type")
	}
}

func TestWithSSLOptions_InvalidCA(t *testing.T) {
	// Test with non-existent CA file
	client, err := NewClient("test-api-key", WithSSLOptions(SSLOptions{
		CAFile: "non-existent-ca.pem",
	}))
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	// Check that the HTTP client's transport doesn't have custom CA set
	if transport, ok := client.httpClient.Transport.(*http.Transport); ok {
		if transport.TLSClientConfig != nil && transport.TLSClientConfig.RootCAs != nil {
			t.Error("Expected no custom CA to be loaded for invalid file")
		}
	}
}

func TestWithSSLOptions_CombinedOptions(t *testing.T) {
	// Test combining multiple SSL options
	client, err := NewClient("test-api-key", WithSSLOptions(SSLOptions{
		InsecureSkipVerify: true,
		CertFile:          "non-existent-cert.pem", // This should be ignored due to non-existence
		KeyFile:           "non-existent-key.pem",  // This should be ignored due to non-existence
		CAFile:            "non-existent-ca.pem",   // This should be ignored due to non-existence
	}))
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	// Check that InsecureSkipVerify is set but certificates are not loaded
	if transport, ok := client.httpClient.Transport.(*http.Transport); ok {
		if transport.TLSClientConfig == nil {
			t.Fatal("Expected TLSClientConfig to be set")
		}
		if !transport.TLSClientConfig.InsecureSkipVerify {
			t.Error("Expected InsecureSkipVerify to be true")
		}
		if len(transport.TLSClientConfig.Certificates) > 0 {
			t.Error("Expected no client certificates to be loaded for invalid files")
		}
		if transport.TLSClientConfig.RootCAs != nil {
			t.Error("Expected no custom CA to be loaded for invalid file")
		}
	} else {
		t.Fatal("Expected http.Transport, got different type")
	}
}

func TestSSLOptions_EmptyOptions(t *testing.T) {
	// Test with empty SSL options - should not affect TLS config
	client, err := NewClient("test-api-key", WithSSLOptions(SSLOptions{}))
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	// The transport should either have no TLS config or default TLS config
	if transport, ok := client.httpClient.Transport.(*http.Transport); ok {
		if transport.TLSClientConfig != nil {
			// If TLS config exists, verify it has secure defaults
			if transport.TLSClientConfig.InsecureSkipVerify {
				t.Error("Expected InsecureSkipVerify to be false by default")
			}
			if len(transport.TLSClientConfig.Certificates) > 0 {
				t.Error("Expected no client certificates by default")
			}
		}
	} else {
		t.Fatal("Expected http.Transport, got different type")
	}
}

func TestWithSSLOptions_MismatchedCertAndKey(t *testing.T) {
	// Test with only cert file provided (no key)
	client1, err := NewClient("test-api-key", WithSSLOptions(SSLOptions{
		CertFile: "testdata/ssl/client.pem",
		// KeyFile intentionally omitted
	}))
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	// Should not load certificates when only cert is provided
	if transport, ok := client1.httpClient.Transport.(*http.Transport); ok {
		if transport.TLSClientConfig != nil && len(transport.TLSClientConfig.Certificates) > 0 {
			t.Error("Expected no certificates loaded when key file missing")
		}
	}

	// Test with only key file provided (no cert)
	client2, err := NewClient("test-api-key", WithSSLOptions(SSLOptions{
		// CertFile intentionally omitted
		KeyFile: "testdata/ssl/client.key",
	}))
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	// Should not load certificates when only key is provided
	if transport, ok := client2.httpClient.Transport.(*http.Transport); ok {
		if transport.TLSClientConfig != nil && len(transport.TLSClientConfig.Certificates) > 0 {
			t.Error("Expected no certificates loaded when cert file missing")
		}
	}
}

func TestWithSSLOptions_MalformedCertificate(t *testing.T) {
	// Ensure testdata directory exists
	if err := os.MkdirAll("testdata/ssl", 0755); err != nil {
		t.Fatalf("Failed to create testdata/ssl directory: %v", err)
	}

	// Test with malformed certificate
	client, err := NewClient("test-api-key", WithSSLOptions(SSLOptions{
		CertFile: "testdata/ssl/invalid.pem",
		KeyFile:  "testdata/ssl/client.key", // Valid key but won't match invalid cert
	}))
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	// Should not load certificates when cert is malformed
	if transport, ok := client.httpClient.Transport.(*http.Transport); ok {
		if transport.TLSClientConfig != nil && len(transport.TLSClientConfig.Certificates) > 0 {
			t.Error("Expected no certificates loaded for malformed certificate")
		}
	}
}

func TestWithSSLOptions_MalformedCA(t *testing.T) {
	// Test with malformed CA certificate  
	client, err := NewClient("test-api-key", WithSSLOptions(SSLOptions{
		CAFile: "testdata/ssl/invalid.pem", // Malformed CA cert
	}))
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	// Should not set custom CA pool for malformed CA
	if transport, ok := client.httpClient.Transport.(*http.Transport); ok {
		if transport.TLSClientConfig != nil && transport.TLSClientConfig.RootCAs != nil {
			t.Error("Expected no custom CA loaded for malformed CA certificate")
		}
	}
}