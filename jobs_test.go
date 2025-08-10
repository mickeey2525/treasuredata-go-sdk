package treasuredata

import (
	"context"
	"fmt"
	"net/http"
	"reflect"
	"testing"
	"time"
)

func TestJobsService_List(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/v3/job/list", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "GET")
		testURL(t, r, "/v3/job/list")

		fmt.Fprint(w, `{
			"count": 2,
			"from": 0,
			"to": 20,
			"jobs": [
				{
					"job_id": "123456",
					"type": "hive",
					"database": "test_db",
					"query": "SELECT COUNT(*) FROM events",
					"status": "success",
					"url": "https://console.treasuredata.com/jobs/123456",
					"user_name": "test_user",
					"created_at": 1609459200,
					"updated_at": 1609459800,
					"start_at": 1609459300,
					"end_at": 1609459700,
					"duration": 400,
					"cpu_time": 350,
					"result_size": 1024,
					"num_records": 1,
					"priority": 1,
					"retry_limit": 3,
					"organization": "test_org",
					"hive_result_schema": "[[\"count\",\"bigint\"]]",
					"result": "result_table_123",
					"linked_result_export_job_id": null,
					"result_export_target_job_id": "789012"
				},
				{
					"job_id": "456789",
					"type": "trino",
					"database": "analytics",
					"query": {"sql": "SELECT user_id FROM events LIMIT 10"},
					"status": "running",
					"url": "https://console.treasuredata.com/jobs/456789",
					"user_name": "analyst",
					"created_at": 1609545600,
					"updated_at": 1609545900,
					"start_at": 1609545700,
					"end_at": 1609546000,
					"duration": 0,
					"result_size": 0,
					"num_records": 0,
					"priority": 0,
					"retry_limit": 0,
					"hive_result_schema": "",
					"result": "",
					"linked_result_export_job_id": "111222",
					"result_export_target_job_id": null
				}
			]
		}`)
	})

	ctx := context.Background()
	resp, err := client.Jobs.List(ctx, nil)
	if err != nil {
		t.Errorf("Jobs.List returned error: %v", err)
	}

	cpuTime1 := 350
	from := 0
	to := 20
	orgPtr := "test_org"
	linkedExportJobID := "111222"
	resultExportTargetJobID := "789012"

	want := &JobListResponse{
		Count: 2,
		From:  &from,
		To:    &to,
		Jobs: []Job{
			{
				JobID:                   "123456",
				Type:                    "hive",
				Database:                "test_db",
				Query:                   QueryField{Value: "SELECT COUNT(*) FROM events"},
				Status:                  "success",
				URL:                     "https://console.treasuredata.com/jobs/123456",
				UserName:                "test_user",
				CreatedAt:               TDTime{time.Unix(1609459200, 0)},
				UpdatedAt:               TDTime{time.Unix(1609459800, 0)},
				StartAt:                 TDTime{time.Unix(1609459300, 0)},
				EndAt:                   TDTime{time.Unix(1609459700, 0)},
				Duration:                400,
				CPUTime:                 &cpuTime1,
				ResultSize:              1024,
				NumRecords:              1,
				Priority:                1,
				RetryLimit:              3,
				Organization:            &orgPtr,
				HiveResultSchema:        "[[\"count\",\"bigint\"]]",
				Result:                  "result_table_123",
				LinkedResultExportJobID: FlexibleString{Value: nil},
				ResultExportTargetJobID: FlexibleString{Value: &resultExportTargetJobID},
			},
			{
				JobID:                   "456789",
				Type:                    "trino",
				Database:                "analytics",
				Query:                   QueryField{Value: "{\"sql\":\"SELECT user_id FROM events LIMIT 10\"}"},
				Status:                  "running",
				URL:                     "https://console.treasuredata.com/jobs/456789",
				UserName:                "analyst",
				CreatedAt:               TDTime{time.Unix(1609545600, 0)},
				UpdatedAt:               TDTime{time.Unix(1609545900, 0)},
				StartAt:                 TDTime{time.Unix(1609545700, 0)},
				EndAt:                   TDTime{time.Unix(1609546000, 0)},
				Duration:                0,
				CPUTime:                 nil,
				ResultSize:              0,
				NumRecords:              0,
				Priority:                0,
				RetryLimit:              0,
				Organization:            nil,
				HiveResultSchema:        "",
				Result:                  "",
				LinkedResultExportJobID: FlexibleString{Value: &linkedExportJobID},
				ResultExportTargetJobID: FlexibleString{Value: nil},
			},
		},
	}

	if !reflect.DeepEqual(resp, want) {
		t.Errorf("Jobs.List returned %+v, want %+v", resp, want)
	}
}

func TestJobsService_List_WithOptions(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/v3/job/list", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "GET")
		
		// Check URL path
		if r.URL.Path != "/v3/job/list" {
			t.Errorf("Request path: %v, want %v", r.URL.Path, "/v3/job/list")
		}
		
		// Check URL parameters
		params := r.URL.Query()
		if params.Get("from") != "10" {
			t.Errorf("Parameter 'from': %v, want %v", params.Get("from"), "10")
		}
		if params.Get("to") != "20" {
			t.Errorf("Parameter 'to': %v, want %v", params.Get("to"), "20")
		}
		if params.Get("status") != "success" {
			t.Errorf("Parameter 'status': %v, want %v", params.Get("status"), "success")
		}
		if params.Get("slow") != "true" {
			t.Errorf("Parameter 'slow': %v, want %v", params.Get("slow"), "true")
		}

		fmt.Fprint(w, `{
			"count": 0,
			"jobs": []
		}`)
	})

	ctx := context.Background()
	opts := &JobListOptions{
		From:   10,
		To:     20,
		Status: "success",
		Slow:   true,
	}

	_, err := client.Jobs.List(ctx, opts)
	if err != nil {
		t.Errorf("Jobs.List returned error: %v", err)
	}
}

func TestJobsService_List_Error(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/v3/job/list", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprint(w, `{"error": "Invalid API key"}`)
	})

	ctx := context.Background()
	_, err := client.Jobs.List(ctx, nil)
	if err == nil {
		t.Error("Expected error to be returned")
	}

	if tdErr, ok := err.(*ErrorResponse); ok {
		if tdErr.Response.StatusCode != http.StatusUnauthorized {
			t.Errorf("Expected status code 401, got %d", tdErr.Response.StatusCode)
		}
	}
}

func TestJobsService_Get(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/v3/job/show/123456", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "GET")
		testURL(t, r, "/v3/job/show/123456")

		fmt.Fprint(w, `{
			"job_id": "123456",
			"type": "hive",
			"database": "test_db",
			"query": "SELECT * FROM users WHERE id = 1",
			"status": "success",
			"url": "https://console.treasuredata.com/jobs/123456",
			"user_name": "test_user",
			"created_at": 1609459200,
			"updated_at": 1609459800,
			"start_at": 1609459300,
			"end_at": 1609459700,
			"duration": 400,
			"cpu_time": 350,
			"result_size": 2048,
			"num_records": 1,
			"priority": 2,
			"retry_limit": 3,
			"organization": "test_org",
			"hive_result_schema": "[[\"id\",\"bigint\"],[\"name\",\"string\"]]",
			"result": "result_table_123",
			"linked_result_export_job_id": null,
			"result_export_target_job_id": null,
			"debug": {
				"cmdout": "Query executed successfully",
				"stderr": ""
			}
		}`)
	})

	ctx := context.Background()
	job, err := client.Jobs.Get(ctx, "123456")
	if err != nil {
		t.Errorf("Jobs.Get returned error: %v", err)
	}

	cpuTime := 350
	orgPtr := "test_org"
	want := &Job{
		JobID:                   "123456",
		Type:                    "hive",
		Database:                "test_db",
		Query:                   QueryField{Value: "SELECT * FROM users WHERE id = 1"},
		Status:                  "success",
		URL:                     "https://console.treasuredata.com/jobs/123456",
		UserName:                "test_user",
		CreatedAt:               TDTime{time.Unix(1609459200, 0)},
		UpdatedAt:               TDTime{time.Unix(1609459800, 0)},
		StartAt:                 TDTime{time.Unix(1609459300, 0)},
		EndAt:                   TDTime{time.Unix(1609459700, 0)},
		Duration:                400,
		CPUTime:                 &cpuTime,
		ResultSize:              2048,
		NumRecords:              1,
		Priority:                2,
		RetryLimit:              3,
		Organization:            &orgPtr,
		HiveResultSchema:        "[[\"id\",\"bigint\"],[\"name\",\"string\"]]",
		Result:                  "result_table_123",
		LinkedResultExportJobID: FlexibleString{Value: nil},
		ResultExportTargetJobID: FlexibleString{Value: nil},
		Debug: &JobDebug{
			Cmdout: "Query executed successfully",
			Stderr: "",
		},
	}

	if !reflect.DeepEqual(job, want) {
		t.Errorf("Jobs.Get returned %+v, want %+v", job, want)
	}
}

func TestJobsService_Get_NotFound(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/v3/job/show/nonexistent", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprint(w, `{"error": "Job not found"}`)
	})

	ctx := context.Background()
	_, err := client.Jobs.Get(ctx, "nonexistent")
	if err == nil {
		t.Error("Expected error to be returned")
	}

	if tdErr, ok := err.(*ErrorResponse); ok {
		if tdErr.Response.StatusCode != http.StatusNotFound {
			t.Errorf("Expected status code 404, got %d", tdErr.Response.StatusCode)
		}
	}
}

func TestJobsService_Status(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/v3/job/status/123456", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "GET")
		testURL(t, r, "/v3/job/status/123456")

		fmt.Fprint(w, `{
			"status": "success",
			"cpu_time": 250,
			"result_size": 1024,
			"duration": 300,
			"job_id": "123456",
			"created_at": 1609459200,
			"updated_at": 1609459800,
			"start_at": 1609459300,
			"end_at": 1609459600,
			"num_records": 10
		}`)
	})

	ctx := context.Background()
	status, err := client.Jobs.Status(ctx, "123456")
	if err != nil {
		t.Errorf("Jobs.Status returned error: %v", err)
	}

	cpuTime := 250
	want := &JobStatus{
		Status:     "success",
		CPUTime:    &cpuTime,
		ResultSize: 1024,
		Duration:   300,
		JobID:      "123456",
		CreatedAt:  TDTime{time.Unix(1609459200, 0)},
		UpdatedAt:  TDTime{time.Unix(1609459800, 0)},
		StartAt:    TDTime{time.Unix(1609459300, 0)},
		EndAt:      TDTime{time.Unix(1609459600, 0)},
		NumRecords: 10,
	}

	if !reflect.DeepEqual(status, want) {
		t.Errorf("Jobs.Status returned %+v, want %+v", status, want)
	}
}

func TestJobsService_Status_Running(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/v3/job/status/789012", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{
			"status": "running",
			"cpu_time": null,
			"result_size": 0,
			"duration": 0,
			"job_id": "789012",
			"created_at": 1609459200,
			"updated_at": 1609459300,
			"start_at": 1609459250,
			"end_at": 1609459250,
			"num_records": 0
		}`)
	})

	ctx := context.Background()
	status, err := client.Jobs.Status(ctx, "789012")
	if err != nil {
		t.Errorf("Jobs.Status returned error: %v", err)
	}

	want := &JobStatus{
		Status:     "running",
		CPUTime:    nil,
		ResultSize: 0,
		Duration:   0,
		JobID:      "789012",
		CreatedAt:  TDTime{time.Unix(1609459200, 0)},
		UpdatedAt:  TDTime{time.Unix(1609459300, 0)},
		StartAt:    TDTime{time.Unix(1609459250, 0)},
		EndAt:      TDTime{time.Unix(1609459250, 0)},
		NumRecords: 0,
	}

	if !reflect.DeepEqual(status, want) {
		t.Errorf("Jobs.Status returned %+v, want %+v", status, want)
	}
}

func TestJobsService_StatusByDomainKey(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/v3/job/status_by_domain_key/daily-report-key", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "GET")
		testURL(t, r, "/v3/job/status_by_domain_key/daily-report-key")

		fmt.Fprint(w, `{
			"status": "success",
			"cpu_time": 180,
			"result_size": 4096,
			"duration": 200,
			"job_id": "345678",
			"created_at": 1609459200,
			"updated_at": 1609459400,
			"start_at": 1609459250,
			"end_at": 1609459450,
			"num_records": 25
		}`)
	})

	ctx := context.Background()
	status, err := client.Jobs.StatusByDomainKey(ctx, "daily-report-key")
	if err != nil {
		t.Errorf("Jobs.StatusByDomainKey returned error: %v", err)
	}

	cpuTime := 180
	want := &JobStatus{
		Status:     "success",
		CPUTime:    &cpuTime,
		ResultSize: 4096,
		Duration:   200,
		JobID:      "345678",
		CreatedAt:  TDTime{time.Unix(1609459200, 0)},
		UpdatedAt:  TDTime{time.Unix(1609459400, 0)},
		StartAt:    TDTime{time.Unix(1609459250, 0)},
		EndAt:      TDTime{time.Unix(1609459450, 0)},
		NumRecords: 25,
	}

	if !reflect.DeepEqual(status, want) {
		t.Errorf("Jobs.StatusByDomainKey returned %+v, want %+v", status, want)
	}
}

func TestJobsService_Kill(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/v3/job/kill/123456", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "POST")
		testURL(t, r, "/v3/job/kill/123456")
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, `{"job_id": "123456", "status": "killed"}`)
	})

	ctx := context.Background()
	err := client.Jobs.Kill(ctx, "123456")
	if err != nil {
		t.Errorf("Jobs.Kill returned error: %v", err)
	}
}

func TestJobsService_Kill_NotFound(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/v3/job/kill/nonexistent", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprint(w, `{"error": "Job not found"}`)
	})

	ctx := context.Background()
	err := client.Jobs.Kill(ctx, "nonexistent")
	if err == nil {
		t.Error("Expected error to be returned")
	}

	if tdErr, ok := err.(*ErrorResponse); ok {
		if tdErr.Response.StatusCode != http.StatusNotFound {
			t.Errorf("Expected status code 404, got %d", tdErr.Response.StatusCode)
		}
	}
}

// Test QueryField JSON marshaling/unmarshaling

func TestQueryField_UnmarshalJSON_String(t *testing.T) {
	var q QueryField
	err := q.UnmarshalJSON([]byte(`"SELECT * FROM events"`))
	if err != nil {
		t.Errorf("UnmarshalJSON returned error: %v", err)
	}
	if q.Value != "SELECT * FROM events" {
		t.Errorf("Expected 'SELECT * FROM events', got %q", q.Value)
	}
}

func TestQueryField_UnmarshalJSON_Object(t *testing.T) {
	var q QueryField
	err := q.UnmarshalJSON([]byte(`{"sql": "SELECT COUNT(*) FROM users"}`))
	if err != nil {
		t.Errorf("UnmarshalJSON returned error: %v", err)
	}
	expected := `{"sql":"SELECT COUNT(*) FROM users"}`
	if q.Value != expected {
		t.Errorf("Expected %q, got %q", expected, q.Value)
	}
}

func TestQueryField_String(t *testing.T) {
	q := QueryField{Value: "SELECT * FROM events"}
	if q.String() != "SELECT * FROM events" {
		t.Errorf("Expected 'SELECT * FROM events', got %q", q.String())
	}
}

// Test FlexibleString JSON marshaling/unmarshaling

func TestFlexibleString_UnmarshalJSON_String(t *testing.T) {
	var f FlexibleString
	err := f.UnmarshalJSON([]byte(`"123456"`))
	if err != nil {
		t.Errorf("UnmarshalJSON returned error: %v", err)
	}
	if f.Value == nil || *f.Value != "123456" {
		t.Errorf("Expected '123456', got %v", f.Value)
	}
}

func TestFlexibleString_UnmarshalJSON_Number(t *testing.T) {
	var f FlexibleString
	err := f.UnmarshalJSON([]byte(`789012`))
	if err != nil {
		t.Errorf("UnmarshalJSON returned error: %v", err)
	}
	if f.Value == nil || *f.Value != "789012" {
		t.Errorf("Expected '789012', got %v", f.Value)
	}
}

func TestFlexibleString_UnmarshalJSON_Null(t *testing.T) {
	var f FlexibleString
	err := f.UnmarshalJSON([]byte(`null`))
	if err != nil {
		t.Errorf("UnmarshalJSON returned error: %v", err)
	}
	if f.Value != nil {
		t.Errorf("Expected nil, got %v", f.Value)
	}
}

func TestFlexibleString_MarshalJSON(t *testing.T) {
	val := "123456"
	f := FlexibleString{Value: &val}
	data, err := f.MarshalJSON()
	if err != nil {
		t.Errorf("MarshalJSON returned error: %v", err)
	}
	expected := `"123456"`
	if string(data) != expected {
		t.Errorf("Expected %s, got %s", expected, string(data))
	}
}

func TestFlexibleString_MarshalJSON_Null(t *testing.T) {
	f := FlexibleString{Value: nil}
	data, err := f.MarshalJSON()
	if err != nil {
		t.Errorf("MarshalJSON returned error: %v", err)
	}
	expected := "null"
	if string(data) != expected {
		t.Errorf("Expected %s, got %s", expected, string(data))
	}
}

// Example tests demonstrating common job operations

func ExampleJobsService_List() {
	client, _ := NewClient("YOUR_API_KEY")
	ctx := context.Background()

	// List recent jobs
	resp, err := client.Jobs.List(ctx, nil)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	fmt.Printf("Found %d jobs:\n", resp.Count)
	for _, job := range resp.Jobs {
		fmt.Printf("Job %s: %s (%s)\n", job.JobID, job.Status, job.Type)
	}
}

func ExampleJobsService_Get() {
	client, _ := NewClient("YOUR_API_KEY")
	ctx := context.Background()

	// Get specific job details
	job, err := client.Jobs.Get(ctx, "123456")
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	fmt.Printf("Job %s: %s\n", job.JobID, job.Status)
	fmt.Printf("Query: %s\n", job.Query.String())
	fmt.Printf("Duration: %d seconds\n", job.Duration)
}

func ExampleJobsService_Status() {
	client, _ := NewClient("YOUR_API_KEY")
	ctx := context.Background()

	// Check job status
	status, err := client.Jobs.Status(ctx, "123456")
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	fmt.Printf("Job %s status: %s\n", status.JobID, status.Status)
	if status.CPUTime != nil {
		fmt.Printf("CPU Time: %d seconds\n", *status.CPUTime)
	}
}