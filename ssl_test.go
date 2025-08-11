package treasuredata

import (
	"net/http"
	"os"
	"strings"
	"testing"
)

func TestSSLOptions_StructFields(t *testing.T) {
	// Test that SSLOptions struct has all expected fields
	opts := SSLOptions{
		InsecureSkipVerify: true,
		CertFile:           "cert.pem",
		KeyFile:            "key.pem",
		CAFile:             "ca.pem",
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
	// Use the existing valid test certificate files
	certPath := "testdata/ssl/client.pem"
	keyPath := "testdata/ssl/client.key"

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
	// Test with non-existent certificate files - should return error
	_, err := NewClient("test-api-key", WithSSLOptions(SSLOptions{
		CertFile: "non-existent-cert.pem",
		KeyFile:  "non-existent-key.pem",
	}))
	if err == nil {
		t.Fatal("Expected error when creating client with non-existent certificate files")
	}

	// Verify the error message contains expected information
	if !strings.Contains(err.Error(), "failed to load client certificate") {
		t.Errorf("Expected error about client certificate loading, got: %v", err)
	}
}

func TestWithSSLOptions_CustomCA(t *testing.T) {
	// Use the existing valid test CA certificate
	caPath := "testdata/ssl/ca.pem"

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
	// Test with non-existent CA file - should return error
	_, err := NewClient("test-api-key", WithSSLOptions(SSLOptions{
		CAFile: "non-existent-ca.pem",
	}))
	if err == nil {
		t.Fatal("Expected error when creating client with non-existent CA file")
	}

	// Verify the error message contains expected information
	if !strings.Contains(err.Error(), "failed to read CA certificate file") {
		t.Errorf("Expected error about CA certificate file reading, got: %v", err)
	}
}

func TestWithSSLOptions_CombinedOptions(t *testing.T) {
	// Test combining multiple SSL options - should fail due to invalid certificate files
	_, err := NewClient("test-api-key", WithSSLOptions(SSLOptions{
		InsecureSkipVerify: true,
		CertFile:           "non-existent-cert.pem", // This should cause an error
		KeyFile:            "non-existent-key.pem",  // This should cause an error
		CAFile:             "non-existent-ca.pem",   // This should cause an error
	}))
	if err == nil {
		t.Fatal("Expected error when creating client with non-existent certificate files")
	}

	// Verify the error message contains expected information about certificate loading
	if !strings.Contains(err.Error(), "failed to load client certificate") {
		t.Errorf("Expected error about client certificate loading, got: %v", err)
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

	// Test with malformed certificate - should return error
	_, err := NewClient("test-api-key", WithSSLOptions(SSLOptions{
		CertFile: "testdata/ssl/invalid.pem",
		KeyFile:  "testdata/ssl/client.key", // Valid key but won't match invalid cert
	}))
	if err == nil {
		t.Fatal("Expected error when creating client with malformed certificate")
	}

	// Verify the error message contains expected information about certificate loading
	if !strings.Contains(err.Error(), "failed to load client certificate") {
		t.Errorf("Expected error about client certificate loading, got: %v", err)
	}
}

func TestWithSSLOptions_MalformedCA(t *testing.T) {
	// Test with malformed CA certificate - should return error
	_, err := NewClient("test-api-key", WithSSLOptions(SSLOptions{
		CAFile: "testdata/ssl/invalid.pem", // Malformed CA cert
	}))
	if err == nil {
		t.Fatal("Expected error when creating client with malformed CA certificate")
	}

	// Verify the error message contains expected information about CA parsing
	if !strings.Contains(err.Error(), "failed to parse CA certificate") {
		t.Errorf("Expected error about CA certificate parsing, got: %v", err)
	}
}
