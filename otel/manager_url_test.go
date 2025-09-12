package otel

import "testing"

// TestExtractHostPort tests the extractHostPort function
func TestExtractHostPort(t *testing.T) {
	testCases := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "Full HTTP URL",
			input:    "http://localhost:4318/v1/traces",
			expected: "localhost:4318",
		},
		{
			name:     "Full HTTPS URL",
			input:    "https://api.example.com:8080/v1/metrics",
			expected: "api.example.com:8080",
		},
		{
			name:     "URL without path",
			input:    "http://localhost:4318",
			expected: "localhost:4318",
		},
		{
			name:     "URL with default port",
			input:    "https://api.example.com/v1/traces",
			expected: "api.example.com",
		},
		{
			name:     "Already host:port format",
			input:    "localhost:4318",
			expected: "localhost:4318",
		},
		{
			name:     "Empty string",
			input:    "",
			expected: "",
		},
		{
			name:     "Invalid URL",
			input:    "not-a-url",
			expected: "not-a-url",
		},
		{
			name:     "URL with query parameters",
			input:    "http://localhost:4318/v1/traces?param=value",
			expected: "localhost:4318",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := extractHostPort(tc.input)
			if result != tc.expected {
				t.Errorf("extractHostPort(%q) = %q, expected %q", tc.input, result, tc.expected)
			}
		})
	}
}
