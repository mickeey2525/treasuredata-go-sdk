package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	td "github.com/mickeey2525/treasuredata-go-sdk"
)

func runQueryTests(apiKey string) {
	fmt.Println("=== Query and Job Tests ===")

	client, err := td.NewClient(apiKey)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}

	ctx := context.Background()

	// Find a database with tables for testing
	databases, err := client.Databases.List(ctx)
	if err != nil {
		log.Fatalf("Failed to list databases: %v", err)
	}

	var testDB string
	var testTable string

	for _, db := range databases {
		tables, err := client.Tables.List(ctx, db.Name)
		if err != nil {
			continue
		}
		if len(tables) > 0 {
			testDB = db.Name
			testTable = tables[0].Name
			break
		}
	}

	if testDB == "" {
		fmt.Println("No databases with tables found for query testing")
		return
	}

	fmt.Printf("Using database: %s, table: %s\n\n", testDB, testTable)

	// Test 1: Submit a simple Trino query
	fmt.Println("1. Testing Trino Query Submission...")
	trinoQuery := fmt.Sprintf("SELECT COUNT(*) as row_count FROM %s LIMIT 1", testTable)

	queryOpts := &td.IssueQueryOptions{
		Query:      trinoQuery,
		Priority:   0,
		RetryLimit: 1,
	}

	queryResp, err := client.Queries.Issue(ctx, td.QueryTypeTrino, testDB, queryOpts)
	if err != nil {
		fmt.Printf("❌ Failed to submit Trino query: %v\n", err)
	} else {
		fmt.Printf("✅ Trino query submitted successfully (Job ID: %s)\n", queryResp.JobID)

		// Monitor the job
		fmt.Println("   Monitoring job progress...")
		monitorJob(ctx, client, queryResp.JobID)
	}

	// Test 1.5: Test various data types with Trino
	fmt.Println("\n1.5. Testing Trino Queries with Various Data Types...")
	testDataTypeQueries(ctx, client, testDB, td.QueryTypeTrino)

	// Test 2: Submit a Hive query
	fmt.Println("\n2. Testing Hive Query Submission...")
	hiveQuery := fmt.Sprintf("SELECT COUNT(*) as row_count FROM %s LIMIT 1", testTable)

	queryOpts.Query = hiveQuery
	queryResp, err = client.Queries.Issue(ctx, td.QueryTypeHive, testDB, queryOpts)
	if err != nil {
		fmt.Printf("❌ Failed to submit Hive query: %v\n", err)
	} else {
		fmt.Printf("✅ Hive query submitted successfully (Job ID: %s)\n", queryResp.JobID)

		// Monitor the job
		fmt.Println("   Monitoring job progress...")
		monitorJob(ctx, client, queryResp.JobID)
	}

	// Test 2.5: Test various data types with Hive
	fmt.Println("\n2.5. Testing Hive Queries with Various Data Types...")
	testDataTypeQueries(ctx, client, testDB, td.QueryTypeHive)

	// Test 3: Test with idempotency key
	fmt.Println("\n3. Testing Query with Idempotency Key...")
	idempotentQuery := "SELECT 1 as test_value"
	domainKey := fmt.Sprintf("test-query-%d", time.Now().Unix())

	queryOpts = &td.IssueQueryOptions{
		Query:      idempotentQuery,
		Priority:   0,
		RetryLimit: 1,
		DomainKey:  domainKey,
	}

	queryResp1, err := client.Queries.Issue(ctx, td.QueryTypeTrino, testDB, queryOpts)
	if err != nil {
		fmt.Printf("❌ Failed to submit first idempotent query: %v\n", err)
	} else {
		fmt.Printf("✅ First idempotent query submitted (Job ID: %s)\n", queryResp1.JobID)

		// Submit the same query again with the same domain key
		queryResp2, err := client.Queries.Issue(ctx, td.QueryTypeTrino, testDB, queryOpts)
		if err != nil {
			fmt.Printf("❌ Failed to submit second idempotent query: %v\n", err)
		} else {
			fmt.Printf("✅ Second idempotent query submitted (Job ID: %s)\n", queryResp2.JobID)

			if queryResp1.JobID == queryResp2.JobID {
				fmt.Println("✅ Idempotency working correctly - same job ID returned")
			} else {
				fmt.Println("⚠️  Different job IDs returned - idempotency may not be working")
			}
		}

		// Test status by domain key
		status, err := client.Jobs.StatusByDomainKey(ctx, domainKey)
		if err != nil {
			fmt.Printf("❌ Failed to get status by domain key: %v\n", err)
		} else {
			fmt.Printf("✅ Status by domain key: %s\n", status.Status)
		}
	}
}

func monitorJob(ctx context.Context, client *td.Client, jobID string) {
	for range 30 { // Wait up to 30 seconds
		status, err := client.Jobs.Status(ctx, jobID)
		if err != nil {
			fmt.Printf("   ❌ Error getting job status: %v\n", err)
			return
		}

		fmt.Printf("   Status: %s", status.Status)
		if status.Status == "success" {
			fmt.Printf(" (Duration: %d seconds)\n", status.Duration)

			// Try to get results
			if status.ResultSize > 0 {
				fmt.Println("   Retrieving results...")

				// Get raw results first to see the actual format
				body, err := client.Results.GetResult(ctx, jobID, &td.GetResultOptions{Format: td.ResultFormatJSON})
				if err != nil {
					fmt.Printf("   ❌ Error getting results: %v\n", err)
				} else {
					defer body.Close()

					// Read the raw JSON to understand the format
					var rawResults any
					err := json.NewDecoder(body).Decode(&rawResults)
					if err != nil {
						fmt.Printf("   ❌ Error decoding results: %v\n", err)
					} else {
						fmt.Printf("   ✅ Retrieved results: %v\n", rawResults)

						// Try to handle different result formats
						switch v := rawResults.(type) {
						case []any:
							fmt.Printf("   Result format: Array with %d elements\n", len(v))
							if len(v) > 0 {
								fmt.Printf("   First element: %v\n", v[0])
							}
						case map[string]any:
							fmt.Printf("   Result format: Object with keys: %v\n", getKeys(v))
						case []map[string]any:
							fmt.Printf("   Result format: Array of objects with %d elements\n", len(v))
							if len(v) > 0 {
								fmt.Printf("   First row: %v\n", v[0])
							}
						default:
							fmt.Printf("   Result format: Single value of type %T\n", v)
						}
					}
				}
			}
			return
		} else if status.Status == "error" {
			fmt.Printf(" (Failed after %d seconds)\n", status.Duration)

			// Get job details for error information
			job, err := client.Jobs.Get(ctx, jobID)
			if err == nil && job.Debug != nil {
				fmt.Printf("   Error details: %s\n", job.Debug.Stderr)
			}
			return
		} else if status.Status == "killed" {
			fmt.Println(" (Job was killed)")
			return
		}

		fmt.Printf("...\n")
		time.Sleep(1 * time.Second)
	}

	fmt.Println("   ⚠️  Job monitoring timed out")
}

// getKeys returns the keys of a map
func getKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// testDataTypeQueries tests various data types and SQL functions
func testDataTypeQueries(ctx context.Context, client *td.Client, testDB string, queryType td.QueryType) {
	dataTypeTests := []struct {
		name  string
		query string
		desc  string
	}{
		{
			name:  "Integers",
			query: "SELECT 42 as integer_val, -123 as negative_int, 0 as zero_val",
			desc:  "Test integer values (positive, negative, zero)",
		},
		{
			name:  "Floating Point",
			query: "SELECT 3.14159 as pi, -2.5 as negative_float, 0.0 as zero_float, 1.23e-4 as scientific",
			desc:  "Test floating point numbers including scientific notation",
		},
		{
			name:  "Strings",
			query: "SELECT 'Hello World' as simple_string, 'String with spaces' as spaced_string, '' as empty_string, 'Special chars: !@#$%^&*()' as special_chars",
			desc:  "Test various string formats",
		},
		{
			name:  "Booleans",
			query: "SELECT true as bool_true, false as bool_false, (1 > 0) as computed_bool",
			desc:  "Test boolean values and boolean expressions",
		},
		{
			name:  "Nulls",
			query: "SELECT null as null_val, CAST(null AS VARCHAR) as null_string, CAST(null AS INTEGER) as null_int",
			desc:  "Test null values with different types",
		},
		{
			name:  "Dates and Times",
			query: "SELECT current_date as today, current_timestamp as now, date('2023-01-01') as fixed_date",
			desc:  "Test date and timestamp functions",
		},
		{
			name:  "Mathematical Operations",
			query: "SELECT 10 + 5 as addition, 10 - 3 as subtraction, 4 * 6 as multiplication, 15 / 3 as division, 17 % 5 as modulo",
			desc:  "Test basic mathematical operations",
		},
		{
			name:  "String Functions",
			query: "SELECT upper('hello') as uppercase, lower('WORLD') as lowercase, length('test') as str_length, concat('Hello', ' ', 'World') as concatenated",
			desc:  "Test string manipulation functions",
		},
		{
			name:  "Conditional Logic",
			query: "SELECT CASE WHEN 1 > 0 THEN 'positive' ELSE 'not positive' END as case_result, coalesce(null, 'default') as coalesce_result",
			desc:  "Test conditional expressions and null handling",
		},
		{
			name:  "Arrays",
			query: "SELECT array[1, 2, 3] as int_array, array['a', 'b', 'c'] as string_array, array[true, false] as bool_array",
			desc:  "Test array data types",
		},
		{
			name:  "JSON-like String",
			query: "SELECT '{\"key\": \"value\", \"number\": 42}' as json_string",
			desc:  "Test JSON-like string handling",
		},
		{
			name:  "Aggregations",
			query: "SELECT COUNT(*) as count_all, SUM(value) as sum_val, AVG(value) as avg_val, MIN(value) as min_val, MAX(value) as max_val FROM (SELECT 1 as value UNION ALL SELECT 2 UNION ALL SELECT 3)",
			desc:  "Test aggregate functions",
		},
	}

	for i, test := range dataTypeTests {
		fmt.Printf("   %d. %s: %s\n", i+1, test.name, test.desc)

		// Skip some tests for Hive that might not be supported
		if queryType == td.QueryTypeHive && test.name == "Arrays" {
			fmt.Printf("      ⚠️  Skipping %s test for Hive (may not be supported)\n", test.name)
			continue
		}

		// Adjust queries based on query engine capabilities
		queryToRun := test.query
		if queryType == td.QueryTypeHive {
			// Hive-specific adjustments
			switch test.name {
			case "Dates and Times":
				queryToRun = "SELECT current_date() as today, current_timestamp() as now, date('2023-01-01') as fixed_date"
			case "String Functions":
				queryToRun = "SELECT upper('hello') as uppercase, lower('WORLD') as lowercase, length('test') as str_length, concat('Hello', ' ', 'World') as concatenated"
			}
		}

		queryOpts := &td.IssueQueryOptions{
			Query:      queryToRun,
			Priority:   0,
			RetryLimit: 1,
		}

		queryResp, err := client.Queries.Issue(ctx, queryType, testDB, queryOpts)
		if err != nil {
			fmt.Printf("      ❌ Failed to submit query: %v\n", err)
			continue
		}

		fmt.Printf("      ✅ Query submitted (Job ID: %s)\n", queryResp.JobID)

		// Monitor the job with a shorter timeout for data type tests
		monitorJobQuiet(ctx, client, queryResp.JobID, test.name)
	}
}

// monitorJobQuiet monitors a job with minimal output for data type tests
func monitorJobQuiet(ctx context.Context, client *td.Client, jobID string, testName string) {
	for range 20 { // Wait up to 20 seconds for data type tests
		status, err := client.Jobs.Status(ctx, jobID)
		if err != nil {
			fmt.Printf("      ❌ Error getting job status: %v\n", err)
			return
		}

		if status.Status == "success" {
			fmt.Printf("      ✅ %s test completed successfully (Duration: %d seconds)\n", testName, status.Duration)

			// Get and display results
			if status.ResultSize > 0 {
				body, err := client.Results.GetResult(ctx, jobID, &td.GetResultOptions{Format: td.ResultFormatJSON})
				if err != nil {
					fmt.Printf("      ❌ Error getting results: %v\n", err)
				} else {
					defer body.Close()

					var rawResults any
					err := json.NewDecoder(body).Decode(&rawResults)
					if err != nil {
						fmt.Printf("      ❌ Error decoding results: %v\n", err)
					} else {
						// Display results in a compact format
						fmt.Printf("      📊 Results: %v\n", formatResultsCompact(rawResults))
					}
				}
			}
			return
		} else if status.Status == "error" {
			fmt.Printf("      ❌ %s test failed (Duration: %d seconds)\n", testName, status.Duration)

			// Get error details
			job, err := client.Jobs.Get(ctx, jobID)
			if err == nil && job.Debug != nil {
				fmt.Printf("      Error: %s\n", job.Debug.Stderr)
			}
			return
		} else if status.Status == "killed" {
			fmt.Printf("      ❌ %s test was killed\n", testName)
			return
		}

		time.Sleep(1 * time.Second)
	}

	fmt.Printf("      ⚠️  %s test timed out\n", testName)
}

// formatResultsCompact formats results in a compact way for display
func formatResultsCompact(results any) string {
	switch v := results.(type) {
	case []any:
		if len(v) == 0 {
			return "[]"
		}
		if len(v) == 1 {
			return fmt.Sprintf("[%v]", v[0])
		}
		return fmt.Sprintf("[%v, ... (%d items)]", v[0], len(v))
	case map[string]any:
		keys := make([]string, 0, len(v))
		for k := range v {
			keys = append(keys, k)
		}
		return fmt.Sprintf("{%v}", keys)
	default:
		return fmt.Sprintf("%v", v)
	}
}
