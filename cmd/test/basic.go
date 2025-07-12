package main

import (
	"context"
	"fmt"
	"log"

	td "github.com/mickeey2525/treasuredata-go-sdk"
)

// runSimpleTest is the original simple test function
// This is now called from run_all_tests.go for the "basic" test type
func runSimpleTest(apiKey string) {
	// Create client
	client, err := td.NewClient(apiKey)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}

	ctx := context.Background()

	// Test 1: List databases
	fmt.Println("Testing: List databases...")
	databases, err := client.Databases.List(ctx)
	if err != nil {
		log.Fatalf("Failed to list databases: %v", err)
	}

	fmt.Printf("Found %d databases:\n", len(databases))
	for _, db := range databases {
		fmt.Printf("  - %s (created: %s)\n", db.Name, db.CreatedAt)
	}

	// Test 2: If we have databases, list tables in the first one
	if len(databases) > 0 {
		dbName := databases[0].Name
		fmt.Printf("\nTesting: List tables in database '%s'...\n", dbName)

		tables, err := client.Tables.List(ctx, dbName)
		if err != nil {
			log.Printf("Failed to list tables: %v", err)
		} else {
			fmt.Printf("Found %d tables in '%s':\n", len(tables), dbName)
			for i, table := range tables {
				if i < 5 { // Show first 5 tables
					fmt.Printf("  - %s (type: %s, count: %d)\n", table.Name, table.Type, table.Count)
				}
			}
			if len(tables) > 5 {
				fmt.Printf("  ... and %d more tables\n", len(tables)-5)
			}
		}
	}

	fmt.Println("\nAPI test completed successfully!")
}
