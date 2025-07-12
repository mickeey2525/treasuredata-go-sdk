package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	td "github.com/mickeey2525/treasuredata-go-sdk"
)

// TestResult represents the result of a test
type TestResult struct {
	Name    string
	Success bool
	Error   error
	Message string
}

func runComprehensiveTests(apiKey string) {
	fmt.Println("=== Treasure Data Go SDK Comprehensive Tests ===")

	// Create client
	client, err := td.NewClient(apiKey)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}

	ctx := context.Background()
	results := []TestResult{}

	// Test 1: Database Operations
	fmt.Println("1. Testing Database Operations...")
	results = append(results, testDatabaseOperations(ctx, client)...)

	// Test 2: User Operations (if permissions allow)
	fmt.Println("\n2. Testing User Operations...")
	results = append(results, testUserOperations(ctx, client)...)

	// Test 3: Job Operations
	fmt.Println("\n3. Testing Job Operations...")
	results = append(results, testJobOperations(ctx, client)...)

	// Test 4: Client Configuration
	fmt.Println("\n4. Testing Client Configuration...")
	results = append(results, testClientConfiguration(apiKey)...)

	// Print Summary
	fmt.Println("\n=== Test Summary ===")
	passed := 0
	failed := 0

	for _, result := range results {
		status := "✅ PASS"
		if !result.Success {
			status = "❌ FAIL"
			failed++
		} else {
			passed++
		}

		fmt.Printf("%s %s", status, result.Name)
		if result.Message != "" {
			fmt.Printf(" - %s", result.Message)
		}
		if result.Error != nil {
			fmt.Printf(" (Error: %v)", result.Error)
		}
		fmt.Println()
	}

	fmt.Printf("\nTotal: %d tests, %d passed, %d failed\n", len(results), passed, failed)

	if failed > 0 {
		os.Exit(1)
	}
}

func testDatabaseOperations(ctx context.Context, client *td.Client) []TestResult {
	var results []TestResult

	// Test listing databases
	databases, err := client.Databases.List(ctx)
	if err != nil {
		results = append(results, TestResult{
			Name:    "List Databases",
			Success: false,
			Error:   err,
		})
		return results
	}

	results = append(results, TestResult{
		Name:    "List Databases",
		Success: true,
		Message: fmt.Sprintf("Found %d databases", len(databases)),
	})

	// Test getting a specific database (if any exist)
	if len(databases) > 0 {
		dbName := databases[0].Name
		db, err := client.Databases.Get(ctx, dbName)
		if err != nil {
			results = append(results, TestResult{
				Name:    "Get Database",
				Success: false,
				Error:   err,
			})
		} else {
			results = append(results, TestResult{
				Name:    "Get Database",
				Success: true,
				Message: fmt.Sprintf("Retrieved database '%s'", db.Name),
			})
		}

		// Test listing tables in the database
		tables, err := client.Tables.List(ctx, dbName)
		if err != nil {
			results = append(results, TestResult{
				Name:    "List Tables",
				Success: false,
				Error:   err,
			})
		} else {
			results = append(results, TestResult{
				Name:    "List Tables",
				Success: true,
				Message: fmt.Sprintf("Found %d tables in '%s'", len(tables), dbName),
			})
		}

		// Test getting a specific table (if any exist)
		if len(tables) > 0 {
			tableName := tables[0].Name
			table, err := client.Tables.Get(ctx, dbName, tableName)
			if err != nil {
				results = append(results, TestResult{
					Name:    "Get Table",
					Success: false,
					Error:   err,
				})
			} else {
				results = append(results, TestResult{
					Name:    "Get Table",
					Success: true,
					Message: fmt.Sprintf("Retrieved table '%s' (count: %d)", table.Name, table.Count),
				})
			}
		}
	}

	return results
}

func testUserOperations(ctx context.Context, client *td.Client) []TestResult {
	var results []TestResult

	// Test listing users (may fail due to permissions)
	users, err := client.Users.List(ctx)
	if err != nil {
		results = append(results, TestResult{
			Name:    "List Users",
			Success: false,
			Error:   err,
			Message: "This may fail due to insufficient permissions",
		})
	} else {
		results = append(results, TestResult{
			Name:    "List Users",
			Success: true,
			Message: fmt.Sprintf("Found %d users", len(users)),
		})

		// Test getting current user's details (if any users found)
		if len(users) > 0 {
			// Find the current user (marked with Me: true)
			var currentUser *td.User
			for _, user := range users {
				if user.Me {
					currentUser = &user
					break
				}
			}

			if currentUser != nil {
				user, err := client.Users.Get(ctx, currentUser.Email)
				if err != nil {
					results = append(results, TestResult{
						Name:    "Get Current User",
						Success: false,
						Error:   err,
					})
				} else {
					results = append(results, TestResult{
						Name:    "Get Current User",
						Success: true,
						Message: fmt.Sprintf("Retrieved user '%s'", user.Email),
					})
				}

				// Test listing API keys for current user
				apiKeys, err := client.Users.ListAPIKeys(ctx, currentUser.Email)
				if err != nil {
					results = append(results, TestResult{
						Name:    "List API Keys",
						Success: false,
						Error:   err,
					})
				} else {
					results = append(results, TestResult{
						Name:    "List API Keys",
						Success: true,
						Message: fmt.Sprintf("Found %d API keys", len(apiKeys)),
					})
				}
			}
		}
	}

	return results
}

func testJobOperations(ctx context.Context, client *td.Client) []TestResult {
	var results []TestResult

	// Test listing recent jobs
	opts := &td.JobListOptions{
		From: 0,
		To:   10, // Get last 10 jobs
	}

	jobList, err := client.Jobs.List(ctx, opts)
	if err != nil {
		results = append(results, TestResult{
			Name:    "List Jobs",
			Success: false,
			Error:   err,
		})
		return results
	}

	results = append(results, TestResult{
		Name:    "List Jobs",
		Success: true,
		Message: fmt.Sprintf("Found %d jobs (showing last 10)", jobList.Count),
	})

	// Test getting details of the first job (if any exist)
	if len(jobList.Jobs) > 0 {
		job := jobList.Jobs[0]
		jobDetails, err := client.Jobs.Get(ctx, job.JobID)
		if err != nil {
			results = append(results, TestResult{
				Name:    "Get Job Details",
				Success: false,
				Error:   err,
			})
		} else {
			results = append(results, TestResult{
				Name:    "Get Job Details",
				Success: true,
				Message: fmt.Sprintf("Retrieved job '%s' (status: %s)", jobDetails.JobID, jobDetails.Status),
			})
		}

		// Test getting job status
		status, err := client.Jobs.Status(ctx, job.JobID)
		if err != nil {
			results = append(results, TestResult{
				Name:    "Get Job Status",
				Success: false,
				Error:   err,
			})
		} else {
			results = append(results, TestResult{
				Name:    "Get Job Status",
				Success: true,
				Message: fmt.Sprintf("Job status: %s", status.Status),
			})
		}

		// Test getting results if job is successful
		if jobDetails.Status == "success" && jobDetails.ResultSize > 0 {
			opts := &td.GetResultOptions{
				Format: td.ResultFormatJSON,
			}
			reader, err := client.Results.GetResult(ctx, job.JobID, opts)
			if err != nil {
				results = append(results, TestResult{
					Name:    "Get Job Results",
					Success: false,
					Error:   err,
				})
			} else {
				reader.Close() // Just test that we can get the reader
				results = append(results, TestResult{
					Name:    "Get Job Results",
					Success: true,
					Message: "Successfully retrieved result stream",
				})
			}
		}
	}

	return results
}

func testClientConfiguration(apiKey string) []TestResult {
	var results []TestResult

	// Test different client configurations
	configurations := []struct {
		name   string
		client *td.Client
		err    error
	}{
		{
			name: "Default Configuration",
			client: func() *td.Client {
				c, _ := td.NewClient(apiKey)
				return c
			}(),
			err: func() error {
				_, err := td.NewClient(apiKey)
				return err
			}(),
		},
		{
			name: "US Region Configuration",
			client: func() *td.Client {
				c, _ := td.NewClient(apiKey, td.WithRegion("us"))
				return c
			}(),
			err: func() error {
				_, err := td.NewClient(apiKey, td.WithRegion("us"))
				return err
			}(),
		},
		{
			name: "Custom User Agent",
			client: func() *td.Client {
				c, _ := td.NewClient(apiKey, td.WithUserAgent("test-sdk/1.0"))
				return c
			}(),
			err: func() error {
				_, err := td.NewClient(apiKey, td.WithUserAgent("test-sdk/1.0"))
				return err
			}(),
		},
		{
			name: "Custom Timeout",
			client: func() *td.Client {
				c, _ := td.NewClient(apiKey, td.WithHTTPClient(&http.Client{
					Timeout: 60 * time.Second,
				}))
				return c
			}(),
			err: func() error {
				_, err := td.NewClient(apiKey, td.WithHTTPClient(&http.Client{
					Timeout: 60 * time.Second,
				}))
				return err
			}(),
		},
	}

	for _, config := range configurations {
		if config.err != nil {
			results = append(results, TestResult{
				Name:    config.name,
				Success: false,
				Error:   config.err,
			})
		} else {
			results = append(results, TestResult{
				Name:    config.name,
				Success: true,
				Message: fmt.Sprintf("Client created successfully (BaseURL: %s)", config.client.BaseURL.String()),
			})
		}
	}

	return results
}

// Removed main function - use run_all_tests.go instead
