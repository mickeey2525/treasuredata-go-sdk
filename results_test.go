package treasuredata

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"
)

func TestResultsService_GetResult_JSON(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/v3/job/result/123456", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "GET")
		testURL(t, r, "/v3/job/result/123456?format=json")

		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `[
			{"user_id": 1, "name": "Alice", "count": 100},
			{"user_id": 2, "name": "Bob", "count": 200}
		]`)
	})

	ctx := context.Background()
	opts := &GetResultOptions{Format: ResultFormatJSON}

	body, err := client.Results.GetResult(ctx, "123456", opts)
	if err != nil {
		t.Errorf("Results.GetResult returned error: %v", err)
	}
	defer body.Close()

	data, err := io.ReadAll(body)
	if err != nil {
		t.Errorf("Failed to read response body: %v", err)
	}

	expectedJSON := `[
			{"user_id": 1, "name": "Alice", "count": 100},
			{"user_id": 2, "name": "Bob", "count": 200}
		]`

	// Parse both JSON strings to compare structure
	var expected, actual interface{}
	if err := json.Unmarshal([]byte(expectedJSON), &expected); err != nil {
		t.Errorf("Failed to parse expected JSON: %v", err)
	}
	if err := json.Unmarshal(data, &actual); err != nil {
		t.Errorf("Failed to parse actual JSON: %v", err)
	}

	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("Results.GetResult returned %s, want %s", string(data), expectedJSON)
	}
}

func TestResultsService_GetResult_CSV(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/v3/job/result/123456", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "GET")
		testURL(t, r, "/v3/job/result/123456?format=csv")

		w.Header().Set("Content-Type", "text/csv")
		fmt.Fprint(w, `user_id,name,count
1,Alice,100
2,Bob,200`)
	})

	ctx := context.Background()
	opts := &GetResultOptions{Format: ResultFormatCSV}

	body, err := client.Results.GetResult(ctx, "123456", opts)
	if err != nil {
		t.Errorf("Results.GetResult returned error: %v", err)
	}
	defer body.Close()

	data, err := io.ReadAll(body)
	if err != nil {
		t.Errorf("Failed to read response body: %v", err)
	}

	expected := "user_id,name,count\n1,Alice,100\n2,Bob,200"
	if string(data) != expected {
		t.Errorf("Results.GetResult returned %q, want %q", string(data), expected)
	}
}

func TestResultsService_GetResult_WithLimit(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/v3/job/result/123456", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "GET")
		testURL(t, r, "/v3/job/result/123456?format=json&limit=10")

		fmt.Fprint(w, `[{"user_id": 1, "name": "Alice"}]`)
	})

	ctx := context.Background()
	opts := &GetResultOptions{
		Format: ResultFormatJSON,
		Limit:  10,
	}

	body, err := client.Results.GetResult(ctx, "123456", opts)
	if err != nil {
		t.Errorf("Results.GetResult returned error: %v", err)
	}
	defer body.Close()
}

func TestResultsService_GetResult_NoOptions(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/v3/job/result/123456", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "GET")
		testURL(t, r, "/v3/job/result/123456")

		fmt.Fprint(w, `[{"result": "success"}]`)
	})

	ctx := context.Background()
	body, err := client.Results.GetResult(ctx, "123456", nil)
	if err != nil {
		t.Errorf("Results.GetResult returned error: %v", err)
	}
	defer body.Close()
}

func TestResultsService_GetResult_JobNotFound(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/v3/job/result/nonexistent", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprint(w, `{"error": "Job not found"}`)
	})

	ctx := context.Background()
	_, err := client.Results.GetResult(ctx, "nonexistent", nil)
	if err == nil {
		t.Error("Expected error to be returned")
	}

	if tdErr, ok := err.(*ErrorResponse); ok {
		if tdErr.Response.StatusCode != http.StatusNotFound {
			t.Errorf("Expected status code 404, got %d", tdErr.Response.StatusCode)
		}
	}
}

func TestResultsService_GetResult_JobNotCompleted(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/v3/job/result/running123", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
		fmt.Fprint(w, `{"error": "Job is not completed yet"}`)
	})

	ctx := context.Background()
	_, err := client.Results.GetResult(ctx, "running123", nil)
	if err == nil {
		t.Error("Expected error to be returned")
	}

	if tdErr, ok := err.(*ErrorResponse); ok {
		if tdErr.Response.StatusCode != http.StatusConflict {
			t.Errorf("Expected status code 409, got %d", tdErr.Response.StatusCode)
		}
	}
}

func TestResultsService_GetResultJSON(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/v3/job/result/123456", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "GET")
		testURL(t, r, "/v3/job/result/123456?format=json")

		fmt.Fprint(w, `{
			"total_count": 1000,
			"users": [
				{"id": 1, "name": "Alice"},
				{"id": 2, "name": "Bob"}
			]
		}`)
	})

	ctx := context.Background()

	type ResultData struct {
		TotalCount int                      `json:"total_count"`
		Users      []map[string]interface{} `json:"users"`
	}

	var result ResultData
	err := client.Results.GetResultJSON(ctx, "123456", &result)
	if err != nil {
		t.Errorf("Results.GetResultJSON returned error: %v", err)
	}

	if result.TotalCount != 1000 {
		t.Errorf("Expected total_count 1000, got %d", result.TotalCount)
	}
	if len(result.Users) != 2 {
		t.Errorf("Expected 2 users, got %d", len(result.Users))
	}
}

func TestResultsService_GetResultJSON_Error(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/v3/job/result/invalid", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, `{"error": "Invalid job ID"}`)
	})

	ctx := context.Background()
	var result map[string]interface{}
	err := client.Results.GetResultJSON(ctx, "invalid", &result)
	if err == nil {
		t.Error("Expected error to be returned")
	}
}

func TestResultsService_GetResultJSONL(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/v3/job/result/123456", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "GET")
		testURL(t, r, "/v3/job/result/123456?format=jsonl")

		fmt.Fprint(w, `{"user_id": 1, "name": "Alice", "score": 95}
{"user_id": 2, "name": "Bob", "score": 87}
{"user_id": 3, "name": "Carol", "score": 92}`)
	})

	ctx := context.Background()
	scanner, err := client.Results.GetResultJSONL(ctx, "123456")
	if err != nil {
		t.Errorf("Results.GetResultJSONL returned error: %v", err)
	}
	defer scanner.Close()

	var records []map[string]interface{}
	for scanner.Scan() {
		var record map[string]interface{}
		if err := scanner.Decode(&record); err != nil {
			t.Errorf("Failed to decode JSONL record: %v", err)
			continue
		}
		records = append(records, record)
	}

	if err := scanner.Err(); err != nil {
		t.Errorf("JSONL scanner error: %v", err)
	}

	if len(records) != 3 {
		t.Errorf("Expected 3 records, got %d", len(records))
	}

	// Verify first record
	if records[0]["user_id"].(float64) != 1 {
		t.Errorf("Expected user_id 1, got %v", records[0]["user_id"])
	}
	if records[0]["name"] != "Alice" {
		t.Errorf("Expected name Alice, got %v", records[0]["name"])
	}
}

func TestResultsService_GetResultJSONL_Error(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/v3/job/result/invalid", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprint(w, `{"error": "Job not found"}`)
	})

	ctx := context.Background()
	_, err := client.Results.GetResultJSONL(ctx, "invalid")
	if err == nil {
		t.Error("Expected error to be returned")
	}
}

func TestJSONLScanner_Methods(t *testing.T) {
	// Test JSONLScanner methods with mock data
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/v3/job/result/123456", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"id": 1, "text": "first line"}
{"id": 2, "text": "second line"}`)
	})

	ctx := context.Background()
	scanner, err := client.Results.GetResultJSONL(ctx, "123456")
	if err != nil {
		t.Errorf("Failed to get JSONL scanner: %v", err)
	}
	defer scanner.Close()

	// Test first line
	if !scanner.Scan() {
		t.Error("Expected first scan to return true")
	}

	text := scanner.Text()
	expectedText := `{"id": 1, "text": "first line"}`
	if text != expectedText {
		t.Errorf("Expected text %q, got %q", expectedText, text)
	}

	bytes := scanner.Bytes()
	if string(bytes) != expectedText {
		t.Errorf("Expected bytes %q, got %q", expectedText, string(bytes))
	}

	var record map[string]interface{}
	if err := scanner.Decode(&record); err != nil {
		t.Errorf("Failed to decode record: %v", err)
	}
	if record["id"].(float64) != 1 {
		t.Errorf("Expected id 1, got %v", record["id"])
	}

	// Test second line
	if !scanner.Scan() {
		t.Error("Expected second scan to return true")
	}

	// Test end of data
	if scanner.Scan() {
		t.Error("Expected third scan to return false")
	}

	if err := scanner.Err(); err != nil {
		t.Errorf("Scanner error: %v", err)
	}
}

func TestResultsService_ListResults(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/v3/result/list", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "GET")
		testURL(t, r, "/v3/result/list")

		fmt.Fprint(w, `{
			"results": [
				{
					"name": "s3_output",
					"url": "s3://my-bucket/results/",
					"id": "result_1",
					"type": "s3",
					"settings": {
						"region": "us-east-1",
						"access_key_id": "AKIAIOSFODNN7EXAMPLE"
					}
				},
				{
					"name": "ftp_export",
					"url": "ftp://ftp.example.com/exports/",
					"id": "result_2",
					"type": "ftp",
					"settings": {
						"username": "ftpuser",
						"password": "encrypted_password"
					}
				}
			]
		}`)
	})

	ctx := context.Background()
	results, err := client.Results.ListResults(ctx)
	if err != nil {
		t.Errorf("Results.ListResults returned error: %v", err)
	}

	want := []Result{
		{
			Name: "s3_output",
			URL:  "s3://my-bucket/results/",
			ID:   "result_1",
			Type: "s3",
			Settings: map[string]interface{}{
				"region":        "us-east-1",
				"access_key_id": "AKIAIOSFODNN7EXAMPLE",
			},
		},
		{
			Name: "ftp_export",
			URL:  "ftp://ftp.example.com/exports/",
			ID:   "result_2",
			Type: "ftp",
			Settings: map[string]interface{}{
				"username": "ftpuser",
				"password": "encrypted_password",
			},
		},
	}

	if !reflect.DeepEqual(results, want) {
		t.Errorf("Results.ListResults returned %+v, want %+v", results, want)
	}
}

func TestResultsService_ListResults_Empty(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/v3/result/list", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"results": []}`)
	})

	ctx := context.Background()
	results, err := client.Results.ListResults(ctx)
	if err != nil {
		t.Errorf("Results.ListResults returned error: %v", err)
	}

	if len(results) != 0 {
		t.Errorf("Expected empty results, got %d items", len(results))
	}
}

func TestResultsService_ListResults_Error(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/v3/result/list", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprint(w, `{"error": "Invalid API key"}`)
	})

	ctx := context.Background()
	_, err := client.Results.ListResults(ctx)
	if err == nil {
		t.Error("Expected error to be returned")
	}

	if tdErr, ok := err.(*ErrorResponse); ok {
		if tdErr.Response.StatusCode != http.StatusUnauthorized {
			t.Errorf("Expected status code 401, got %d", tdErr.Response.StatusCode)
		}
	}
}

// Test different result formats

func TestResultsService_GetResult_AllFormats(t *testing.T) {
	formats := []struct {
		format   ResultFormat
		expected string
		mimeType string
	}{
		{ResultFormatJSON, `[{"id":1}]`, "application/json"},
		{ResultFormatCSV, "id\n1", "text/csv"},
		{ResultFormatTSV, "id\t1", "text/tab-separated-values"},
		{ResultFormatJSONL, `{"id":1}`, "application/jsonl"},
		{ResultFormatMessagePack, "msgpack_data", "application/msgpack"},
	}

	for _, tt := range formats {
		t.Run(string(tt.format), func(t *testing.T) {
			client, mux, teardown := setup()
			defer teardown()

			mux.HandleFunc("/v3/job/result/123456", func(w http.ResponseWriter, r *http.Request) {
				expectedURL := fmt.Sprintf("/v3/job/result/123456?format=%s", tt.format)
				testURL(t, r, expectedURL)

				w.Header().Set("Content-Type", tt.mimeType)
				fmt.Fprint(w, tt.expected)
			})

			ctx := context.Background()
			opts := &GetResultOptions{Format: tt.format}

			body, err := client.Results.GetResult(ctx, "123456", opts)
			if err != nil {
				t.Errorf("Results.GetResult returned error: %v", err)
			}
			defer body.Close()

			data, err := io.ReadAll(body)
			if err != nil {
				t.Errorf("Failed to read response body: %v", err)
			}

			if string(data) != tt.expected {
				t.Errorf("Expected %q, got %q", tt.expected, string(data))
			}
		})
	}
}

// Example tests demonstrating common result operations

func ExampleResultsService_GetResult() {
	client, _ := NewClient("YOUR_API_KEY")
	ctx := context.Background()

	// Get job results in CSV format
	opts := &GetResultOptions{
		Format: ResultFormatCSV,
		Limit:  1000,
	}

	body, err := client.Results.GetResult(ctx, "123456", opts)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	defer body.Close()

	// Process CSV data
	data, _ := io.ReadAll(body)
	lines := strings.Split(string(data), "\n")
	fmt.Printf("Got %d lines of CSV data\n", len(lines))
}

func ExampleResultsService_GetResultJSON() {
	client, _ := NewClient("YOUR_API_KEY")
	ctx := context.Background()

	// Get results as structured JSON
	type UserStats struct {
		UserID int    `json:"user_id"`
		Name   string `json:"name"`
		Count  int    `json:"count"`
	}

	var users []UserStats
	err := client.Results.GetResultJSON(ctx, "123456", &users)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	for _, user := range users {
		fmt.Printf("User %s: %d events\n", user.Name, user.Count)
	}
}

func ExampleResultsService_GetResultJSONL() {
	client, _ := NewClient("YOUR_API_KEY")
	ctx := context.Background()

	// Get results in JSONL format for streaming processing
	scanner, err := client.Results.GetResultJSONL(ctx, "123456")
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	defer scanner.Close()

	count := 0
	for scanner.Scan() {
		var record map[string]interface{}
		if err := scanner.Decode(&record); err != nil {
			fmt.Printf("Decode error: %v\n", err)
			continue
		}
		count++
		// Process each record...
	}

	if err := scanner.Err(); err != nil {
		fmt.Printf("Scanner error: %v\n", err)
		return
	}

	fmt.Printf("Processed %d records\n", count)
}
