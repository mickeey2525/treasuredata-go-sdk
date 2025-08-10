package treasuredata

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"testing"
)

func TestQueriesService_Issue_Hive(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/v3/job/issue/hive/test_db", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "POST")
		testURL(t, r, "/v3/job/issue/hive/test_db")

		var body IssueQueryOptions
		json.NewDecoder(r.Body).Decode(&body)

		expectedQuery := "SELECT COUNT(*) FROM events"
		if body.Query != expectedQuery {
			t.Errorf("Request body query = %v, want %v", body.Query, expectedQuery)
		}
		if body.Priority != 1 {
			t.Errorf("Request body priority = %v, want %v", body.Priority, 1)
		}
		if body.RetryLimit != 3 {
			t.Errorf("Request body retry_limit = %v, want %v", body.RetryLimit, 3)
		}

		fmt.Fprint(w, `{
			"job": "job123456",
			"job_id": "123456",
			"database": "test_db"
		}`)
	})

	ctx := context.Background()
	opts := &IssueQueryOptions{
		Query:      "SELECT COUNT(*) FROM events",
		Priority:   1,
		RetryLimit: 3,
		Result:     "result_table",
		Type:       "hive",
	}

	resp, err := client.Queries.Issue(ctx, QueryTypeHive, "test_db", opts)
	if err != nil {
		t.Errorf("Queries.Issue returned error: %v", err)
	}

	want := &IssueQueryResponse{
		Job:      "job123456",
		JobID:    "123456",
		Database: "test_db",
	}

	if !reflect.DeepEqual(resp, want) {
		t.Errorf("Queries.Issue returned %+v, want %+v", resp, want)
	}
}

func TestQueriesService_Issue_Trino(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/v3/job/issue/trino/test_db", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "POST")
		testURL(t, r, "/v3/job/issue/trino/test_db")

		var body IssueQueryOptions
		json.NewDecoder(r.Body).Decode(&body)

		expectedQuery := "SELECT user_id, COUNT(*) FROM events GROUP BY user_id"
		if body.Query != expectedQuery {
			t.Errorf("Request body query = %v, want %v", body.Query, expectedQuery)
		}

		fmt.Fprint(w, `{
			"job": "job789012",
			"job_id": "789012",
			"database": "test_db"
		}`)
	})

	ctx := context.Background()
	opts := &IssueQueryOptions{
		Query:         "SELECT user_id, COUNT(*) FROM events GROUP BY user_id",
		Priority:      0,
		PoolName:      "default",
		Type:          "trino",
		EngineVersion: "0.172",
	}

	resp, err := client.Queries.Issue(ctx, QueryTypeTrino, "test_db", opts)
	if err != nil {
		t.Errorf("Queries.Issue returned error: %v", err)
	}

	want := &IssueQueryResponse{
		Job:      "job789012",
		JobID:    "789012",
		Database: "test_db",
	}

	if !reflect.DeepEqual(resp, want) {
		t.Errorf("Queries.Issue returned %+v, want %+v", resp, want)
	}
}

func TestQueriesService_Issue_Presto_Deprecated(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/v3/job/issue/presto/test_db", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "POST")
		testURL(t, r, "/v3/job/issue/presto/test_db")

		fmt.Fprint(w, `{
			"job": "job345678",
			"job_id": "345678",
			"database": "test_db"
		}`)
	})

	ctx := context.Background()
	opts := &IssueQueryOptions{
		Query: "SELECT * FROM users LIMIT 10",
	}

	resp, err := client.Queries.Issue(ctx, QueryTypePresto, "test_db", opts)
	if err != nil {
		t.Errorf("Queries.Issue returned error: %v", err)
	}

	want := &IssueQueryResponse{
		Job:      "job345678",
		JobID:    "345678",
		Database: "test_db",
	}

	if !reflect.DeepEqual(resp, want) {
		t.Errorf("Queries.Issue returned %+v, want %+v", resp, want)
	}
}

func TestQueriesService_Issue_InvalidQuery(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/v3/job/issue/hive/test_db", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, `{
			"error": "Invalid query syntax",
			"message": "Syntax error at line 1: SELECT COUNTT(*) FROM events"
		}`)
	})

	ctx := context.Background()
	opts := &IssueQueryOptions{
		Query: "SELECT COUNTT(*) FROM events", // Intentional typo
	}

	_, err := client.Queries.Issue(ctx, QueryTypeHive, "test_db", opts)
	if err == nil {
		t.Error("Expected error to be returned")
	}

	if tdErr, ok := err.(*ErrorResponse); ok {
		if tdErr.Response.StatusCode != http.StatusBadRequest {
			t.Errorf("Expected status code 400, got %d", tdErr.Response.StatusCode)
		}
	} else {
		t.Errorf("Expected ErrorResponse, got %T", err)
	}
}

func TestQueriesService_Issue_DatabaseNotFound(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/v3/job/issue/hive/nonexistent", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprint(w, `{
			"error": "Database not found",
			"message": "Database 'nonexistent' does not exist"
		}`)
	})

	ctx := context.Background()
	opts := &IssueQueryOptions{
		Query: "SELECT COUNT(*) FROM events",
	}

	_, err := client.Queries.Issue(ctx, QueryTypeHive, "nonexistent", opts)
	if err == nil {
		t.Error("Expected error to be returned")
	}

	if tdErr, ok := err.(*ErrorResponse); ok {
		if tdErr.Response.StatusCode != http.StatusNotFound {
			t.Errorf("Expected status code 404, got %d", tdErr.Response.StatusCode)
		}
	}
}

func TestQueriesService_Issue_UnauthorizedAccess(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/v3/job/issue/hive/restricted_db", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprint(w, `{
			"error": "Unauthorized",
			"message": "Access denied to database 'restricted_db'"
		}`)
	})

	ctx := context.Background()
	opts := &IssueQueryOptions{
		Query: "SELECT COUNT(*) FROM events",
	}

	_, err := client.Queries.Issue(ctx, QueryTypeHive, "restricted_db", opts)
	if err == nil {
		t.Error("Expected error to be returned")
	}

	if tdErr, ok := err.(*ErrorResponse); ok {
		if tdErr.Response.StatusCode != http.StatusUnauthorized {
			t.Errorf("Expected status code 401, got %d", tdErr.Response.StatusCode)
		}
	}
}

func TestQueriesService_Issue_WithAllOptions(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/v3/job/issue/trino/analytics_db", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "POST")
		testURL(t, r, "/v3/job/issue/trino/analytics_db")

		var body IssueQueryOptions
		json.NewDecoder(r.Body).Decode(&body)

		// Verify all options are sent correctly
		if body.Query != "SELECT * FROM large_table" {
			t.Errorf("Request body query = %v, want 'SELECT * FROM large_table'", body.Query)
		}
		if body.Priority != 2 {
			t.Errorf("Request body priority = %v, want 2", body.Priority)
		}
		if body.RetryLimit != 5 {
			t.Errorf("Request body retry_limit = %v, want 5", body.RetryLimit)
		}
		if body.Result != "analysis_results" {
			t.Errorf("Request body result = %v, want 'analysis_results'", body.Result)
		}
		if body.DomainKey != "analytics-domain" {
			t.Errorf("Request body domain_key = %v, want 'analytics-domain'", body.DomainKey)
		}
		if body.PoolName != "high-memory" {
			t.Errorf("Request body pool_name = %v, want 'high-memory'", body.PoolName)
		}
		if body.Type != "trino" {
			t.Errorf("Request body type = %v, want 'trino'", body.Type)
		}
		if body.EngineVersion != "0.215" {
			t.Errorf("Request body engine_version = %v, want '0.215'", body.EngineVersion)
		}

		fmt.Fprint(w, `{
			"job": "job999888",
			"job_id": "999888",
			"database": "analytics_db"
		}`)
	})

	ctx := context.Background()
	opts := &IssueQueryOptions{
		Query:         "SELECT * FROM large_table",
		Priority:      2,
		RetryLimit:    5,
		Result:        "analysis_results",
		DomainKey:     "analytics-domain",
		PoolName:      "high-memory",
		Type:          "trino",
		EngineVersion: "0.215",
	}

	resp, err := client.Queries.Issue(ctx, QueryTypeTrino, "analytics_db", opts)
	if err != nil {
		t.Errorf("Queries.Issue returned error: %v", err)
	}

	want := &IssueQueryResponse{
		Job:      "job999888",
		JobID:    "999888",
		Database: "analytics_db",
	}

	if !reflect.DeepEqual(resp, want) {
		t.Errorf("Queries.Issue returned %+v, want %+v", resp, want)
	}
}

func TestQueriesService_Issue_EmptyOptions(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/v3/job/issue/hive/test_db", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "POST")
		testURL(t, r, "/v3/job/issue/hive/test_db")

		var body IssueQueryOptions
		json.NewDecoder(r.Body).Decode(&body)

		// Should handle empty/default values
		if body.Query != "SELECT 1" {
			t.Errorf("Request body query = %v, want 'SELECT 1'", body.Query)
		}
		if body.Priority != 0 { // Default value
			t.Errorf("Request body priority = %v, want 0", body.Priority)
		}

		fmt.Fprint(w, `{
			"job": "job111222",
			"job_id": "111222",
			"database": "test_db"
		}`)
	})

	ctx := context.Background()
	opts := &IssueQueryOptions{
		Query: "SELECT 1",
		// All other options use default/zero values
	}

	_, err := client.Queries.Issue(ctx, QueryTypeHive, "test_db", opts)
	if err != nil {
		t.Errorf("Queries.Issue returned error: %v", err)
	}
}

// Example tests demonstrating common query operations

func ExampleQueriesService_Issue_hive() {
	client, _ := NewClient("YOUR_API_KEY")
	ctx := context.Background()

	// Issue a Hive query
	opts := &IssueQueryOptions{
		Query:    "SELECT COUNT(*) FROM events WHERE date >= '2023-01-01'",
		Priority: 1,
		Result:   "count_result",
	}

	resp, err := client.Queries.Issue(ctx, QueryTypeHive, "analytics", opts)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	fmt.Printf("Query submitted: Job ID %s\n", resp.JobID)
}

func ExampleQueriesService_Issue_trino() {
	client, _ := NewClient("YOUR_API_KEY")
	ctx := context.Background()

	// Issue a Trino query with custom engine version
	opts := &IssueQueryOptions{
		Query:         "SELECT user_id, COUNT(*) as event_count FROM events GROUP BY user_id ORDER BY event_count DESC LIMIT 100",
		Priority:      2,
		PoolName:      "high-memory",
		EngineVersion: "0.215",
	}

	resp, err := client.Queries.Issue(ctx, QueryTypeTrino, "user_analytics", opts)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	fmt.Printf("Trino query submitted: Job ID %s\n", resp.JobID)
}

func ExampleQueriesService_Issue_with_retry() {
	client, _ := NewClient("YOUR_API_KEY")
	ctx := context.Background()

	// Issue a query with retry configuration
	opts := &IssueQueryOptions{
		Query:      "SELECT * FROM large_dataset WHERE processing_date = CURRENT_DATE",
		Priority:   1,
		RetryLimit: 3,
		Result:     "daily_processing_result",
	}

	resp, err := client.Queries.Issue(ctx, QueryTypeHive, "production", opts)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	fmt.Printf("Query with retry submitted: Job ID %s\n", resp.JobID)
}
