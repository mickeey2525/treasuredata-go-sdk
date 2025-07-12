// Test runner for Treasure Data Go SDK
// Usage: go run *.go <API_KEY> [test_type]
// This file must be run with all other .go files to access their functions
package main

import (
	"fmt"
	"log"
	"os"
	"strings"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Treasure Data Go SDK Test Suite")
		fmt.Println()
		fmt.Println("Usage:")
		fmt.Printf("  %s <API_KEY> [test_type]\n", os.Args[0])
		fmt.Printf("  %s constants\n", os.Args[0])
		fmt.Println()
		fmt.Println("Test types:")
		fmt.Println("  basic        - Basic connectivity and database listing")
		fmt.Println("  comprehensive - Full SDK functionality tests")
		fmt.Println("  query        - Query execution and job monitoring")
		fmt.Println("  validation   - API response structure validation")
		fmt.Println("  errors       - Error handling and edge cases")
		fmt.Println("  permissions  - Access control and permissions testing")
		fmt.Println("  constants    - Test query type constants (no API key needed)")
		fmt.Println("  all          - Run all tests (default)")
		fmt.Println()
		fmt.Println("Examples:")
		fmt.Println("  go run *.go YOUR_API_KEY")
		fmt.Println("  go run *.go YOUR_API_KEY basic")
		fmt.Println("  go run *.go YOUR_API_KEY permissions")
		fmt.Println("  go run *.go constants")
		os.Exit(1)
	}

	// Handle constants test case (no API key needed)
	if len(os.Args) == 2 && os.Args[1] == "constants" {
		testConstants()
		return
	}

	apiKey := os.Args[1]
	testType := "all"

	if len(os.Args) >= 3 {
		testType = os.Args[2]
	}

	// Validate API key format (except for constants test)
	if testType != "constants" && !isValidAPIKey(apiKey) {
		log.Fatal("Invalid API key format. Expected format: account_id/api_key")
	}

	fmt.Printf("=== Treasure Data Go SDK Test Suite ===\n")
	if testType != "constants" {
		fmt.Printf("API Key: %s***%s\n", apiKey[:10], apiKey[len(apiKey)-10:])
	}
	fmt.Printf("Test Type: %s\n\n", testType)

	switch testType {
	case "basic":
		runSimpleTest(apiKey)
	case "comprehensive":
		runComprehensiveTests(apiKey)
	case "query":
		runQueryTests(apiKey)
	case "validation":
		runResponseValidationTests(apiKey)
	case "errors":
		runErrorHandlingTests(apiKey)
	case "permissions":
		runPermissionsTests(apiKey)
	case "constants":
		testConstants()
	case "all":
		fmt.Println("Running all test suites...")

		fmt.Println("=" + strings.Repeat("=", 50))
		runSimpleTest(apiKey)

		fmt.Println("\n" + "=" + strings.Repeat("=", 50))
		runComprehensiveTests(apiKey)

		fmt.Println("\n" + "=" + strings.Repeat("=", 50))
		runQueryTests(apiKey)

		fmt.Println("\n" + "=" + strings.Repeat("=", 50))
		runResponseValidationTests(apiKey)

		fmt.Println("\n" + "=" + strings.Repeat("=", 50))
		runErrorHandlingTests(apiKey)

		fmt.Println("\n" + "=" + strings.Repeat("=", 50))
		runPermissionsTests(apiKey)

		fmt.Println("\n" + "=" + strings.Repeat("=", 50))
		fmt.Println("✅ All test suites completed!")
	default:
		log.Fatalf("Unknown test type: %s", testType)
	}
}

func isValidAPIKey(apiKey string) bool {
	// TD API keys should be in format: account_id/api_key
	// Basic validation: contains exactly one slash and has content on both sides
	parts := strings.Split(apiKey, "/")
	return len(parts) == 2 && len(parts[0]) > 0 && len(parts[1]) > 0
}
