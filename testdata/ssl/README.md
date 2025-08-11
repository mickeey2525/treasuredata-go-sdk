# SSL/TLS Test Fixtures

This directory contains test certificates and keys for testing SSL/TLS functionality in the Treasure Data Go SDK.

## Files

- `ca.pem` - Test root Certificate Authority certificate for custom CA testing
- `client.pem` - Test client certificate for mutual TLS authentication testing
- `client.key` - Test client private key corresponding to `client.pem`
- `invalid.pem` - Invalid/malformed certificate content for error handling tests

## Usage

These test fixtures are used by the SSL/TLS test suite in `ssl_test.go` to validate:

- Client certificate loading and configuration
- Custom root CA certificate support
- TLS configuration options
- Error handling for invalid certificates
- Mutual TLS authentication setup

## Security Note

These are **test certificates only** and should never be used in production environments. They are specifically designed for unit testing SSL/TLS functionality.

The certificates are:
- Self-signed test certificates
- Have long validity periods to avoid test failures due to expiration
- Contain test subjects like "Test Client" and "Test CA"
- Are publicly visible in the repository

## Certificate Details

All certificates use RSA 2048-bit keys and are valid from 2024-01-01 to 2034-01-01 (10-year validity for stable testing).