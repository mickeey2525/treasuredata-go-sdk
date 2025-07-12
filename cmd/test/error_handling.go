package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	td "github.com/mickeey2525/treasuredata-go-sdk"
)

func runErrorHandlingTests(apiKey string) {
	fmt.Println("=== Error Handling Tests ===")

	client, err := td.NewClient(apiKey)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}

	ctx := context.Background()

	// Test 1: Non-existent database
	fmt.Println("\n1. Testing non-existent database access...")
	_, err = client.Databases.Get(ctx, "non_existent_database_12345")
	if err != nil {
		if tdErr, ok := err.(*td.ErrorResponse); ok {
			fmt.Printf("✅ Proper error handling for non-existent database\n")
			fmt.Printf("   Status Code: %d\n", tdErr.Response.StatusCode)
			fmt.Printf("   Error Message: %s\n", tdErr.Message)
			if tdErr.ErrorMsg != "" {
				fmt.Printf("   Error Detail: %s\n", tdErr.ErrorMsg)
			}
		} else {
			fmt.Printf("⚠️  Got error but not ErrorResponse type: %v\n", err)
		}
	} else {
		fmt.Printf("❌ Expected error for non-existent database but got none\n")
	}

	// Test 2: Non-existent table
	fmt.Println("\n2. Testing non-existent table access...")

	// Find a real database first
	databases, err := client.Databases.List(ctx)
	if err != nil || len(databases) == 0 {
		fmt.Printf("⚠️  Cannot test table errors - no databases available\n")
	} else {
		dbName := databases[0].Name
		_, err = client.Tables.Get(ctx, dbName, "non_existent_table_12345")
		if err != nil {
			if tdErr, ok := err.(*td.ErrorResponse); ok {
				fmt.Printf("✅ Proper error handling for non-existent table\n")
				fmt.Printf("   Status Code: %d\n", tdErr.Response.StatusCode)
				fmt.Printf("   Error Message: %s\n", tdErr.Message)
			} else {
				fmt.Printf("⚠️  Got error but not ErrorResponse type: %v\n", err)
			}
		} else {
			fmt.Printf("❌ Expected error for non-existent table but got none\n")
		}
	}

	// Test 3: Non-existent job
	fmt.Println("\n3. Testing non-existent job access...")
	_, err = client.Jobs.Get(ctx, "999999999")
	if err != nil {
		if tdErr, ok := err.(*td.ErrorResponse); ok {
			fmt.Printf("✅ Proper error handling for non-existent job\n")
			fmt.Printf("   Status Code: %d\n", tdErr.Response.StatusCode)
			fmt.Printf("   Error Message: %s\n", tdErr.Message)
		} else {
			fmt.Printf("⚠️  Got error but not ErrorResponse type: %v\n", err)
		}
	} else {
		fmt.Printf("❌ Expected error for non-existent job but got none\n")
	}

	// Test 4: Invalid query syntax
	fmt.Println("\n4. Testing invalid query syntax...")
	if len(databases) > 0 {
		dbName := databases[0].Name
		invalidQuery := "INVALID SQL SYNTAX HERE"

		queryOpts := &td.IssueQueryOptions{
			Query:      invalidQuery,
			Priority:   0,
			RetryLimit: 0, // Don't retry failed queries
		}

		queryResp, err := client.Queries.Issue(ctx, td.QueryTypeTrino, dbName, queryOpts)
		if err != nil {
			if tdErr, ok := err.(*td.ErrorResponse); ok {
				fmt.Printf("✅ Proper error handling for invalid query\n")
				fmt.Printf("   Status Code: %d\n", tdErr.Response.StatusCode)
				fmt.Printf("   Error Message: %s\n", tdErr.Message)
			} else {
				fmt.Printf("⚠️  Got error but not ErrorResponse type: %v\n", err)
			}
		} else {
			// Query might be accepted but will fail during execution
			fmt.Printf("⚠️  Invalid query was accepted (Job ID: %s)\n", queryResp.JobID)
			fmt.Printf("   This query will likely fail during execution\n")
		}
	}

	// Test 5: Test client configuration errors
	fmt.Println("\n5. Testing client configuration errors...")

	// Test with empty API key
	_, err = td.NewClient("")
	if err != nil {
		fmt.Printf("✅ Proper error handling for empty API key: %v\n", err)
	} else {
		fmt.Printf("❌ Expected error for empty API key but got none\n")
	}

	// Test with malformed API key
	_, err = td.NewClient("invalid-api-key")
	if err != nil {
		fmt.Printf("✅ Client created with malformed API key (will fail on first request)\n")
	}

	// Test API call with malformed key
	invalidClient, _ := td.NewClient("invalid-api-key")
	_, err = invalidClient.Databases.List(ctx)
	if err != nil {
		if tdErr, ok := err.(*td.ErrorResponse); ok {
			fmt.Printf("✅ Proper error handling for invalid API key\n")
			fmt.Printf("   Status Code: %d\n", tdErr.Response.StatusCode)
			fmt.Printf("   Error Message: %s\n", tdErr.Message)
		} else {
			fmt.Printf("⚠️  Got error but not ErrorResponse type: %v\n", err)
		}
	} else {
		fmt.Printf("❌ Expected error for invalid API key but got none\n")
	}

	// Test 6: Test timeout handling
	fmt.Println("\n6. Testing timeout configuration...")
	timeoutClient, err := td.NewClient(apiKey, td.WithHTTPClient(&http.Client{
		Timeout: 1 * time.Nanosecond, // Extremely short timeout
	}))
	if err != nil {
		fmt.Printf("❌ Failed to create timeout client: %v\n", err)
	} else {
		_, err = timeoutClient.Databases.List(ctx)
		if err != nil {
			fmt.Printf("✅ Timeout handling working: %v\n", err)
		} else {
			fmt.Printf("⚠️  Expected timeout error but request succeeded\n")
		}
	}

	fmt.Println("\n✅ Error handling tests completed!")
}

// Removed main function - use run_all_tests.go instead
