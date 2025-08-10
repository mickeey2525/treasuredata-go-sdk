package treasuredata

import (
	"context"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestBulkImportService_Create(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/v3/bulk_import/create/test_session/analytics/events", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "POST")
		testURL(t, r, "/v3/bulk_import/create/test_session/analytics/events")
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, `{"message": "Bulk import session created"}`)
	})

	ctx := context.Background()
	err := client.BulkImport.Create(ctx, "test_session", "analytics", "events")
	if err != nil {
		t.Errorf("BulkImport.Create returned error: %v", err)
	}
}

func TestBulkImportService_Create_Error(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/v3/bulk_import/create/existing/analytics/events", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
		fmt.Fprint(w, `{"error": "Bulk import session already exists"}`)
	})

	ctx := context.Background()
	err := client.BulkImport.Create(ctx, "existing", "analytics", "events")
	if err == nil {
		t.Error("Expected error to be returned")
	}

	if tdErr, ok := err.(*ErrorResponse); ok {
		if tdErr.Response.StatusCode != http.StatusConflict {
			t.Errorf("Expected status code 409, got %d", tdErr.Response.StatusCode)
		}
	}
}

func TestBulkImportService_Show(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/v3/bulk_import/show/test_session", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "GET")
		testURL(t, r, "/v3/bulk_import/show/test_session")

		fmt.Fprint(w, `{
			"name": "test_session",
			"database": "analytics",
			"table": "events",
			"status": "uploading",
			"job_id": "",
			"valid_records": 1000,
			"error_records": 5,
			"valid_parts": 3,
			"error_parts": 0,
			"upload_frozen": false,
			"created_at": 1609459200
		}`)
	})

	ctx := context.Background()
	bulkImport, err := client.BulkImport.Show(ctx, "test_session")
	if err != nil {
		t.Errorf("BulkImport.Show returned error: %v", err)
	}

	want := &BulkImport{
		Name:         "test_session",
		Database:     "analytics",
		Table:        "events",
		Status:       "uploading",
		JobID:        "",
		ValidRecords: 1000,
		ErrorRecords: 5,
		ValidParts:   3,
		ErrorParts:   0,
		UploadFrozen: false,
		CreatedAt:    TDTime{time.Unix(1609459200, 0)},
	}

	if !reflect.DeepEqual(bulkImport, want) {
		t.Errorf("BulkImport.Show returned %+v, want %+v", bulkImport, want)
	}
}

func TestBulkImportService_List(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/v3/bulk_import/list", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "GET")
		testURL(t, r, "/v3/bulk_import/list")

		fmt.Fprint(w, `{
			"bulk_imports": [
				{
					"name": "session1",
					"database": "analytics",
					"table": "events",
					"status": "committed",
					"job_id": "12345",
					"valid_records": 5000,
					"error_records": 10,
					"valid_parts": 5,
					"error_parts": 1,
					"upload_frozen": true,
					"created_at": 1609459200
				},
				{
					"name": "session2",
					"database": "logs",
					"table": "access_logs",
					"status": "uploading",
					"job_id": "",
					"valid_records": 2000,
					"error_records": 0,
					"valid_parts": 2,
					"error_parts": 0,
					"upload_frozen": false,
					"created_at": 1609545600
				}
			]
		}`)
	})

	ctx := context.Background()
	bulkImports, err := client.BulkImport.List(ctx)
	if err != nil {
		t.Errorf("BulkImport.List returned error: %v", err)
	}

	want := []BulkImport{
		{
			Name:         "session1",
			Database:     "analytics",
			Table:        "events",
			Status:       "committed",
			JobID:        "12345",
			ValidRecords: 5000,
			ErrorRecords: 10,
			ValidParts:   5,
			ErrorParts:   1,
			UploadFrozen: true,
			CreatedAt:    TDTime{time.Unix(1609459200, 0)},
		},
		{
			Name:         "session2",
			Database:     "logs",
			Table:        "access_logs",
			Status:       "uploading",
			JobID:        "",
			ValidRecords: 2000,
			ErrorRecords: 0,
			ValidParts:   2,
			ErrorParts:   0,
			UploadFrozen: false,
			CreatedAt:    TDTime{time.Unix(1609545600, 0)},
		},
	}

	if !reflect.DeepEqual(bulkImports, want) {
		t.Errorf("BulkImport.List returned %+v, want %+v", bulkImports, want)
	}
}

func TestBulkImportService_UploadPart(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/v3/bulk_import/upload_part/test_session/part001.csv", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "PUT")
		testURL(t, r, "/v3/bulk_import/upload_part/test_session/part001.csv")

		// Verify Content-Type is multipart/form-data
		contentType := r.Header.Get("Content-Type")
		if !strings.HasPrefix(contentType, "multipart/form-data") {
			t.Errorf("Expected multipart/form-data content type, got %s", contentType)
		}

		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, `{"message": "Part uploaded successfully"}`)
	})

	ctx := context.Background()
	data := strings.NewReader("user_id,name,age\n1,Alice,25\n2,Bob,30")
	err := client.BulkImport.UploadPart(ctx, "test_session", "part001.csv", data)
	if err != nil {
		t.Errorf("BulkImport.UploadPart returned error: %v", err)
	}
}

func TestBulkImportService_Commit(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/v3/bulk_import/commit/test_session", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "POST")
		testURL(t, r, "/v3/bulk_import/commit/test_session")
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, `{"message": "Bulk import session committed"}`)
	})

	ctx := context.Background()
	err := client.BulkImport.Commit(ctx, "test_session")
	if err != nil {
		t.Errorf("BulkImport.Commit returned error: %v", err)
	}
}

func TestBulkImportService_Freeze(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/v3/bulk_import/freeze/test_session", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "POST")
		testURL(t, r, "/v3/bulk_import/freeze/test_session")
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, `{"message": "Bulk import session frozen"}`)
	})

	ctx := context.Background()
	err := client.BulkImport.Freeze(ctx, "test_session")
	if err != nil {
		t.Errorf("BulkImport.Freeze returned error: %v", err)
	}
}

func TestBulkImportService_Unfreeze(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/v3/bulk_import/unfreeze/test_session", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "POST")
		testURL(t, r, "/v3/bulk_import/unfreeze/test_session")
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, `{"message": "Bulk import session unfrozen"}`)
	})

	ctx := context.Background()
	err := client.BulkImport.Unfreeze(ctx, "test_session")
	if err != nil {
		t.Errorf("BulkImport.Unfreeze returned error: %v", err)
	}
}

func TestBulkImportService_Perform(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/v3/bulk_import/perform/test_session", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "POST")
		testURL(t, r, "/v3/bulk_import/perform/test_session")

		fmt.Fprint(w, `{
			"job_id": "67890",
			"type": "bulk_import",
			"database": "analytics",
			"status": "queued",
			"url": "https://console.treasuredata.com/jobs/67890",
			"user_name": "bulk_import_user",
			"created_at": 1609632000,
			"updated_at": 1609632000,
			"start_at": 1609632000,
			"end_at": 1609632000,
			"duration": 0,
			"result_size": 0,
			"num_records": 0,
			"priority": 0,
			"retry_limit": 0,
			"hive_result_schema": "",
			"result": ""
		}`)
	})

	ctx := context.Background()
	job, err := client.BulkImport.Perform(ctx, "test_session")
	if err != nil {
		t.Errorf("BulkImport.Perform returned error: %v", err)
	}

	if job.JobID != "67890" {
		t.Errorf("Expected job ID 67890, got %s", job.JobID)
	}
	if job.Type != "bulk_import" {
		t.Errorf("Expected job type bulk_import, got %s", job.Type)
	}
	if job.Database != "analytics" {
		t.Errorf("Expected database analytics, got %s", job.Database)
	}
}

func TestBulkImportService_Delete(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/v3/bulk_import/delete/test_session", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "POST")
		testURL(t, r, "/v3/bulk_import/delete/test_session")
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, `{"message": "Bulk import session deleted"}`)
	})

	ctx := context.Background()
	err := client.BulkImport.Delete(ctx, "test_session")
	if err != nil {
		t.Errorf("BulkImport.Delete returned error: %v", err)
	}
}

func TestBulkImportService_Delete_NotFound(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/v3/bulk_import/delete/nonexistent", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprint(w, `{"error": "Bulk import session not found"}`)
	})

	ctx := context.Background()
	err := client.BulkImport.Delete(ctx, "nonexistent")
	if err == nil {
		t.Error("Expected error to be returned")
	}

	if tdErr, ok := err.(*ErrorResponse); ok {
		if tdErr.Response.StatusCode != http.StatusNotFound {
			t.Errorf("Expected status code 404, got %d", tdErr.Response.StatusCode)
		}
	}
}

// Example tests demonstrating common bulk import operations

func ExampleBulkImportService_Create() {
	client, _ := NewClient("YOUR_API_KEY")
	ctx := context.Background()

	// Create a new bulk import session
	err := client.BulkImport.Create(ctx, "daily_events", "analytics", "events")
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	fmt.Println("Bulk import session created successfully")
}

func ExampleBulkImportService_UploadPart() {
	client, _ := NewClient("YOUR_API_KEY")
	ctx := context.Background()

	// Upload CSV data to the bulk import session
	csvData := strings.NewReader(`user_id,event_type,timestamp
1,login,2024-01-01T10:00:00Z
2,page_view,2024-01-01T10:01:00Z
3,purchase,2024-01-01T10:02:00Z`)

	err := client.BulkImport.UploadPart(ctx, "daily_events", "part001.csv", csvData)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	fmt.Println("Part uploaded successfully")
}

func ExampleBulkImportService_Show() {
	client, _ := NewClient("YOUR_API_KEY")
	ctx := context.Background()

	// Check the status of a bulk import session
	bulkImport, err := client.BulkImport.Show(ctx, "daily_events")
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	fmt.Printf("Session: %s\n", bulkImport.Name)
	fmt.Printf("Status: %s\n", bulkImport.Status)
	fmt.Printf("Valid Records: %d\n", bulkImport.ValidRecords)
	fmt.Printf("Error Records: %d\n", bulkImport.ErrorRecords)
}

func ExampleBulkImportService_Commit() {
	client, _ := NewClient("YOUR_API_KEY")
	ctx := context.Background()

	// Commit the bulk import session to make it ready for processing
	err := client.BulkImport.Commit(ctx, "daily_events")
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	fmt.Println("Bulk import session committed")
}

func ExampleBulkImportService_Perform() {
	client, _ := NewClient("YOUR_API_KEY")
	ctx := context.Background()

	// Start the actual import job
	job, err := client.BulkImport.Perform(ctx, "daily_events")
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	fmt.Printf("Import job started: %s (ID: %s)\n", job.Status, job.JobID)
}
