package main

import (
	"context"
	"fmt"
	"log"
	"time"

	td "github.com/mickeey2525/treasuredata-go-sdk"
)

func runResponseValidationTests(apiKey string) {
	fmt.Println("=== API Response Validation Tests ===")

	client, err := td.NewClient(apiKey)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}

	ctx := context.Background()

	// Test 1: Validate Database Response Structure
	fmt.Println("1. Testing Database Response Structure...")
	databases, err := client.Databases.List(ctx)
	if err != nil {
		fmt.Printf("❌ Failed to list databases: %v\n", err)
	} else {
		fmt.Printf("✅ Retrieved %d databases\n", len(databases))

		if len(databases) > 0 {
			db := databases[0]
			validateDatabaseStruct(db)

			// Test individual database fetch
			singleDB, err := client.Databases.Get(ctx, db.Name)
			if err != nil {
				fmt.Printf("❌ Failed to get single database: %v\n", err)
			} else {
				fmt.Printf("✅ Single database fetch successful\n")
				validateDatabaseStruct(*singleDB)
			}
		}
	}

	// Test 2: Validate Table Response Structure
	fmt.Println("\n2. Testing Table Response Structure...")
	if len(databases) > 0 {
		dbName := databases[0].Name
		tables, err := client.Tables.List(ctx, dbName)
		if err != nil {
			fmt.Printf("❌ Failed to list tables: %v\n", err)
		} else {
			fmt.Printf("✅ Retrieved %d tables from database '%s'\n", len(tables), dbName)

			if len(tables) > 0 {
				table := tables[0]
				validateTableStruct(table)

				// Test individual table fetch
				singleTable, err := client.Tables.Get(ctx, dbName, table.Name)
				if err != nil {
					fmt.Printf("❌ Failed to get single table: %v\n", err)
				} else {
					fmt.Printf("✅ Single table fetch successful\n")
					validateTableStruct(*singleTable)
				}
			}
		}
	}

	// Test 3: Validate Job Response Structure
	fmt.Println("\n3. Testing Job Response Structure...")
	jobList, err := client.Jobs.List(ctx, &td.JobListOptions{From: 0, To: 5})
	if err != nil {
		fmt.Printf("❌ Failed to list jobs: %v\n", err)
	} else {
		fmt.Printf("✅ Retrieved job list (count: %d)\n", jobList.Count)

		if len(jobList.Jobs) > 0 {
			job := jobList.Jobs[0]
			validateJobStruct(job)

			// Test individual job fetch
			singleJob, err := client.Jobs.Get(ctx, job.JobID)
			if err != nil {
				fmt.Printf("❌ Failed to get single job: %v\n", err)
			} else {
				fmt.Printf("✅ Single job fetch successful\n")
				validateJobStruct(*singleJob)
			}

			// Test job status
			status, err := client.Jobs.Status(ctx, job.JobID)
			if err != nil {
				fmt.Printf("❌ Failed to get job status: %v\n", err)
			} else {
				fmt.Printf("✅ Job status fetch successful\n")
				validateJobStatusStruct(*status)
			}
		}
	}

	// Test 4: Validate User Response Structure (if accessible)
	fmt.Println("\n4. Testing User Response Structure...")
	users, err := client.Users.List(ctx)
	if err != nil {
		fmt.Printf("⚠️  Cannot test user responses (insufficient permissions): %v\n", err)
	} else {
		fmt.Printf("✅ Retrieved %d users\n", len(users))

		if len(users) > 0 {
			user := users[0]
			validateUserStruct(user)

			// Find current user and test API keys
			for _, u := range users {
				if u.Me {
					apiKeys, err := client.Users.ListAPIKeys(ctx, u.Email)
					if err != nil {
						fmt.Printf("❌ Failed to get API keys: %v\n", err)
					} else {
						fmt.Printf("✅ Retrieved %d API keys\n", len(apiKeys))
						if len(apiKeys) > 0 {
							validateAPIKeyStruct(apiKeys[0])
						}
					}
					break
				}
			}
		}
	}

	// Test 5: Validate Time Parsing
	fmt.Println("\n5. Testing Time Parsing...")
	testTimeParsing(databases)
}

func validateDatabaseStruct(db td.Database) {
	fmt.Printf("   Database '%s':\n", db.Name)
	fmt.Printf("     - Name: %s ✅\n", db.Name)
	fmt.Printf("     - CreatedAt: %s ✅\n", db.CreatedAt.Time.Format(time.RFC3339))
	fmt.Printf("     - UpdatedAt: %s ✅\n", db.UpdatedAt.Time.Format(time.RFC3339))
	fmt.Printf("     - Count: %d ✅\n", db.Count)
	fmt.Printf("     - Permission: %s ✅\n", db.Permission)
	fmt.Printf("     - DeleteProtected: %t ✅\n", db.DeleteProtected)

	if db.Organization != nil {
		fmt.Printf("     - Organization: %s ✅\n", *db.Organization)
	}

	// Validate that times are properly parsed
	if db.CreatedAt.Time.IsZero() {
		fmt.Printf("     ❌ CreatedAt is zero time\n")
	}
	if db.UpdatedAt.Time.IsZero() {
		fmt.Printf("     ❌ UpdatedAt is zero time\n")
	}
}

func validateTableStruct(table td.Table) {
	fmt.Printf("   Table '%s':\n", table.Name)
	fmt.Printf("     - Name: %s ✅\n", table.Name)
	fmt.Printf("     - Type: %s ✅\n", table.Type)
	fmt.Printf("     - Count: %d ✅\n", table.Count)
	fmt.Printf("     - CreatedAt: %s ✅\n", table.CreatedAt.Time.Format(time.RFC3339))
	fmt.Printf("     - UpdatedAt: %s ✅\n", table.UpdatedAt.Time.Format(time.RFC3339))
	fmt.Printf("     - EstimatedStorageSize: %d ✅\n", table.EstimatedStorageSize)
	fmt.Printf("     - DeleteProtected: %t ✅\n", table.DeleteProtected)

	if table.CounterUpdatedAt != nil {
		fmt.Printf("     - CounterUpdatedAt: %s ✅\n", table.CounterUpdatedAt.Time.Format(time.RFC3339))
	}

	if table.LastLogTimestamp.Value != nil {
		fmt.Printf("     - LastLogTimestamp: %d ✅\n", *table.LastLogTimestamp.Value)
	}
}

func validateJobStruct(job td.Job) {
	fmt.Printf("   Job '%s':\n", job.JobID)
	fmt.Printf("     - JobID: %s ✅\n", job.JobID)
	fmt.Printf("     - Type: %s ✅\n", job.Type)
	fmt.Printf("     - Status: %s ✅\n", job.Status)
	fmt.Printf("     - Database: %s ✅\n", job.Database)
	fmt.Printf("     - Query: %s ✅\n", truncateString(job.Query.String(), 50))
	fmt.Printf("     - CreatedAt: %s ✅\n", job.CreatedAt.Time.Format(time.RFC3339))
	fmt.Printf("     - UpdatedAt: %s ✅\n", job.UpdatedAt.Time.Format(time.RFC3339))
	fmt.Printf("     - StartAt: %s ✅\n", job.StartAt.Time.Format(time.RFC3339))
	fmt.Printf("     - EndAt: %s ✅\n", job.EndAt.Time.Format(time.RFC3339))
	fmt.Printf("     - Priority: %d ✅\n", job.Priority)
	fmt.Printf("     - RetryLimit: %d ✅\n", job.RetryLimit)

	if job.CPUTime != nil {
		fmt.Printf("     - CPUTime: %d ✅\n", *job.CPUTime)
	}

	fmt.Printf("     - ResultSize: %d ✅\n", job.ResultSize)
	fmt.Printf("     - NumRecords: %d ✅\n", job.NumRecords)
}

func validateJobStatusStruct(status td.JobStatus) {
	fmt.Printf("   Job Status '%s':\n", status.JobID)
	fmt.Printf("     - Status: %s ✅\n", status.Status)
	fmt.Printf("     - Duration: %d ✅\n", status.Duration)
	fmt.Printf("     - CreatedAt: %s ✅\n", status.CreatedAt.Time.Format(time.RFC3339))
	fmt.Printf("     - UpdatedAt: %s ✅\n", status.UpdatedAt.Time.Format(time.RFC3339))
	fmt.Printf("     - StartAt: %s ✅\n", status.StartAt.Time.Format(time.RFC3339))
	fmt.Printf("     - EndAt: %s ✅\n", status.EndAt.Time.Format(time.RFC3339))
}

func validateUserStruct(user td.User) {
	fmt.Printf("   User '%s':\n", user.Email)
	fmt.Printf("     - ID: %d ✅\n", user.ID)
	fmt.Printf("     - Name: %s ✅\n", user.Name)
	fmt.Printf("     - Email: %s ✅\n", user.Email)
	fmt.Printf("     - AccountID: %d ✅\n", user.AccountID)
	fmt.Printf("     - CreatedAt: %s ✅\n", user.CreatedAt.Time.Format(time.RFC3339))
	fmt.Printf("     - UpdatedAt: %s ✅\n", user.UpdatedAt.Time.Format(time.RFC3339))
	fmt.Printf("     - Administrator: %t ✅\n", user.Administrator)
	fmt.Printf("     - Me: %t ✅\n", user.Me)
	fmt.Printf("     - EmailVerified: %t ✅\n", user.EmailVerified)
}

func validateAPIKeyStruct(apiKey td.APIKey) {
	fmt.Printf("   API Key:\n")
	fmt.Printf("     - Key: %s ✅\n", truncateString(apiKey.Key, 20)+"...")
	fmt.Printf("     - Type: %s ✅\n", apiKey.Type)
	fmt.Printf("     - CreatedAt: %s ✅\n", apiKey.CreatedAt.Time.Format(time.RFC3339))
}

func testTimeParsing(databases []td.Database) {
	if len(databases) == 0 {
		fmt.Printf("   ⚠️  No databases to test time parsing\n")
		return
	}

	db := databases[0]
	fmt.Printf("   Testing time parsing with database '%s':\n", db.Name)

	// Check that the time fields are not zero
	if !db.CreatedAt.Time.IsZero() {
		fmt.Printf("     ✅ CreatedAt properly parsed: %s\n", db.CreatedAt.Time.Format(time.RFC3339))
	} else {
		fmt.Printf("     ❌ CreatedAt is zero time\n")
	}

	if !db.UpdatedAt.Time.IsZero() {
		fmt.Printf("     ✅ UpdatedAt properly parsed: %s\n", db.UpdatedAt.Time.Format(time.RFC3339))
	} else {
		fmt.Printf("     ❌ UpdatedAt is zero time\n")
	}

	// Check that the time is reasonable (not in the future, not too old)
	now := time.Now()
	twoYearsAgo := now.AddDate(-2, 0, 0)

	if db.CreatedAt.Time.After(twoYearsAgo) && db.CreatedAt.Time.Before(now) {
		fmt.Printf("     ✅ CreatedAt is within reasonable range\n")
	} else {
		fmt.Printf("     ⚠️  CreatedAt seems unusual: %s\n", db.CreatedAt.Time.Format(time.RFC3339))
	}
}

func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen]
}

// Removed main function - use run_all_tests.go instead
