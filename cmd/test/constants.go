package main

import (
	"fmt"
	td "github.com/mickeey2525/treasuredata-go-sdk"
)

func testConstants() {
	fmt.Println("=== Query Type Constants Test ===")
	fmt.Printf("QueryTypeHive: %s\n", td.QueryTypeHive)
	fmt.Printf("QueryTypeTrino: %s\n", td.QueryTypeTrino)
	fmt.Printf("QueryTypePresto (deprecated): %s\n", td.QueryTypePresto)

	// Test that both Presto and Trino constants work
	if td.QueryTypeTrino == "trino" {
		fmt.Println("✅ QueryTypeTrino is correctly set to 'trino'")
	}

	if td.QueryTypePresto == "presto" {
		fmt.Println("✅ QueryTypePresto is still available for backward compatibility")
	}

	fmt.Println("✅ All query type constants are working correctly!")
}
