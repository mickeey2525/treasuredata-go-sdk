package treasuredata

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"testing"
	"time"
)

func TestWorkflowService_StartWorkflow(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/api/workflows/1/attempts", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "POST")
		testURL(t, r, "/api/workflows/1/attempts")

		var body map[string]interface{}
		json.NewDecoder(r.Body).Decode(&body)

		if params, ok := body["params"].(map[string]interface{}); ok {
			if params["key"] != "value" {
				t.Errorf("Request body params.key = %v, want %v", params["key"], "value")
			}
		}

		fmt.Fprint(w, `{
			"id": "100",
			"index": 1,
			"workflow_id": "1",
			"status": "running",
			"created_at": 1609459200,
			"session_id": "123-456",
			"session_uuid": "uuid-123",
			"session_time": 1609459200,
			"params": {"key": "value"},
			"done": false
		}`)
	})

	ctx := context.Background()
	params := map[string]interface{}{
		"key": "value",
	}
	attempt, err := client.Workflow.StartWorkflow(ctx, "1", params)
	if err != nil {
		t.Errorf("Workflows.StartWorkflow returned error: %v", err)
	}

	sessionID := "123-456"
	sessionUUID := "uuid-123"
	sessionTime := TDTime{time.Unix(1609459200, 0)}
	want := &WorkflowAttempt{
		ID:          "100",
		Index:       1,
		WorkflowID:  "1",
		Status:      "running",
		CreatedAt:   TDTime{time.Unix(1609459200, 0)},
		SessionID:   &sessionID,
		SessionUUID: &sessionUUID,
		SessionTime: &sessionTime,
		Params:      map[string]interface{}{"key": "value"},
		Done:        false,
	}

	if !reflect.DeepEqual(attempt, want) {
		t.Errorf("Workflows.StartWorkflow returned %+v, want %+v", attempt, want)
	}
}

func TestWorkflowService_ListWorkflowAttempts(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/api/workflows/1/attempts", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "GET")
		testURL(t, r, "/api/workflows/1/attempts?last_id=50&limit=10&status=success")

		fmt.Fprint(w, `{
			"attempts": [
				{
					"id": "100",
					"index": 1,
					"workflow_id": "1",
					"status": "success",
					"created_at": 1609459200,
					"finished_at": 1609462800,
					"success": true,
					"done": true
				}
			]
		}`)
	})

	ctx := context.Background()
	opts := &WorkflowAttemptListOptions{
		Limit:  10,
		LastID: 50,
		Status: "success",
	}
	resp, err := client.Workflow.ListWorkflowAttempts(ctx, "1", opts)
	if err != nil {
		t.Errorf("Workflows.ListWorkflowAttempts returned error: %v", err)
	}

	finishedAt := TDTime{time.Unix(1609462800, 0)}
	success := true
	want := &WorkflowAttemptListResponse{
		Attempts: []WorkflowAttempt{
			{
				ID:         "100",
				Index:      1,
				WorkflowID: "1",
				Status:     "success",
				CreatedAt:  TDTime{time.Unix(1609459200, 0)},
				FinishedAt: &finishedAt,
				Success:    &success,
				Done:       true,
			},
		},
	}

	if !reflect.DeepEqual(resp, want) {
		t.Errorf("Workflows.ListWorkflowAttempts returned %+v, want %+v", resp, want)
	}
}

func TestWorkflowService_GetWorkflowAttempt(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/api/workflows/1/attempts/100", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "GET")
		testURL(t, r, "/api/workflows/1/attempts/100")

		fmt.Fprint(w, `{
			"id": "100",
			"index": 1,
			"workflow_id": "1",
			"status": "success",
			"created_at": 1609459200,
			"finished_at": 1609462800,
			"log_file_size": 1024,
			"success": true,
			"done": true
		}`)
	})

	ctx := context.Background()
	attempt, err := client.Workflow.GetWorkflowAttempt(ctx, "1", "100")
	if err != nil {
		t.Errorf("Workflows.GetWorkflowAttempt returned error: %v", err)
	}

	finishedAt := TDTime{time.Unix(1609462800, 0)}
	logFileSize := int64(1024)
	success := true
	want := &WorkflowAttempt{
		ID:          "100",
		Index:       1,
		WorkflowID:  "1",
		Status:      "success",
		CreatedAt:   TDTime{time.Unix(1609459200, 0)},
		FinishedAt:  &finishedAt,
		LogFileSize: &logFileSize,
		Success:     &success,
		Done:        true,
	}

	if !reflect.DeepEqual(attempt, want) {
		t.Errorf("Workflows.GetWorkflowAttempt returned %+v, want %+v", attempt, want)
	}
}

func TestWorkflowService_KillWorkflowAttempt(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/api/workflows/1/attempts/100/kill", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "POST")
		testURL(t, r, "/api/workflows/1/attempts/100/kill")
		w.WriteHeader(http.StatusOK)
	})

	ctx := context.Background()
	err := client.Workflow.KillWorkflowAttempt(ctx, "1", "100")
	if err != nil {
		t.Errorf("Workflows.KillWorkflowAttempt returned error: %v", err)
	}
}

func TestWorkflowService_RetryWorkflowAttempt(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/api/workflows/1/attempts/100/retry", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "POST")
		testURL(t, r, "/api/workflows/1/attempts/100/retry")

		var body map[string]interface{}
		json.NewDecoder(r.Body).Decode(&body)

		if params, ok := body["params"].(map[string]interface{}); ok {
			if params["retry"] != "true" {
				t.Errorf("Request body params.retry = %v, want %v", params["retry"], "true")
			}
		}

		fmt.Fprint(w, `{
			"id": "101",
			"index": 2,
			"workflow_id": "1",
			"status": "running",
			"created_at": 1609545600,
			"params": {"retry": "true"},
			"done": false
		}`)
	})

	ctx := context.Background()
	params := map[string]interface{}{
		"retry": "true",
	}
	attempt, err := client.Workflow.RetryWorkflowAttempt(ctx, "1", "100", params)
	if err != nil {
		t.Errorf("Workflows.RetryWorkflowAttempt returned error: %v", err)
	}

	want := &WorkflowAttempt{
		ID:         "101",
		Index:      2,
		WorkflowID: "1",
		Status:     "running",
		CreatedAt:  TDTime{time.Unix(1609545600, 0)},
		Params:     map[string]interface{}{"retry": "true"},
		Done:       false,
	}

	if !reflect.DeepEqual(attempt, want) {
		t.Errorf("Workflows.RetryWorkflowAttempt returned %+v, want %+v", attempt, want)
	}
}

func TestWorkflowService_ListWorkflowTasks(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/api/workflows/1/attempts/100/tasks", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "GET")
		testURL(t, r, "/api/workflows/1/attempts/100/tasks")

		fmt.Fprint(w, `{
			"tasks": [
				{
					"id": "task-1",
					"full_name": "+main",
					"parent_id": null,
					"config": {"type": "td"},
					"upstreams": [],
					"is_group": false,
					"state": "success",
					"export_params": {},
					"store_params": {},
					"started_at": 1609459200,
					"updated_at": 1609462800
				}
			]
		}`)
	})

	ctx := context.Background()
	resp, err := client.Workflow.ListWorkflowTasks(ctx, "1", "100")
	if err != nil {
		t.Errorf("Workflows.ListWorkflowTasks returned error: %v", err)
	}

	startedAt := TDTime{time.Unix(1609459200, 0)}
	want := &WorkflowTaskListResponse{
		Tasks: []WorkflowTask{
			{
				ID:           "task-1",
				FullName:     "+main",
				ParentID:     nil,
				Config:       map[string]interface{}{"type": "td"},
				UpstreamsID:  []string{},
				IsGroup:      false,
				State:        "success",
				ExportParams: map[string]interface{}{},
				StoreParams:  map[string]interface{}{},
				StartedAt:    &startedAt,
				UpdatedAt:    TDTime{time.Unix(1609462800, 0)},
			},
		},
	}

	if !reflect.DeepEqual(resp, want) {
		t.Errorf("Workflows.ListWorkflowTasks returned %+v, want %+v", resp, want)
	}
}

func TestWorkflowService_GetWorkflowTask(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/api/workflows/1/attempts/100/tasks/task-1", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "GET")
		testURL(t, r, "/api/workflows/1/attempts/100/tasks/task-1")

		fmt.Fprint(w, `{
			"id": "task-1",
			"full_name": "+main",
			"parent_id": null,
			"config": {"type": "td"},
			"upstreams": [],
			"is_group": false,
			"state": "success",
			"export_params": {},
			"store_params": {},
			"report": "report-123",
			"started_at": 1609459200,
			"updated_at": 1609462800
		}`)
	})

	ctx := context.Background()
	task, err := client.Workflow.GetWorkflowTask(ctx, "1", "100", "task-1")
	if err != nil {
		t.Errorf("Workflows.GetWorkflowTask returned error: %v", err)
	}

	startedAt := TDTime{time.Unix(1609459200, 0)}
	reportID := "report-123"
	want := &WorkflowTask{
		ID:           "task-1",
		FullName:     "+main",
		ParentID:     nil,
		Config:       map[string]interface{}{"type": "td"},
		UpstreamsID:  []string{},
		IsGroup:      false,
		State:        "success",
		ExportParams: map[string]interface{}{},
		StoreParams:  map[string]interface{}{},
		ReportID:     &reportID,
		StartedAt:    &startedAt,
		UpdatedAt:    TDTime{time.Unix(1609462800, 0)},
	}

	if !reflect.DeepEqual(task, want) {
		t.Errorf("Workflows.GetWorkflowTask returned %+v, want %+v", task, want)
	}
}

func TestWorkflowService_GetWorkflowAttemptLog(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	expectedLog := "2021-01-01 00:00:00 +0000 Digdag Executor@ip-10-0-0-1: Starting workflow execution\n2021-01-01 00:00:01 +0000 Digdag Executor@ip-10-0-0-1: Workflow completed successfully"

	mux.HandleFunc("/api/workflows/1/attempts/100/log", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "GET")
		testURL(t, r, "/api/workflows/1/attempts/100/log")
		w.Header().Set("Content-Type", "text/plain")
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(expectedLog)))
		fmt.Fprint(w, expectedLog)
	})

	ctx := context.Background()
	log, err := client.Workflow.GetWorkflowAttemptLog(ctx, "1", "100")
	if err != nil {
		t.Errorf("Workflows.GetWorkflowAttemptLog returned error: %v", err)
	}

	if log != expectedLog {
		t.Errorf("Workflows.GetWorkflowAttemptLog returned %v, want %v", log, expectedLog)
	}
}

func TestWorkflowService_GetWorkflowTaskLog(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	expectedLog := "Task started: td>\n2021-01-01 00:00:00 +0000: Running query\nTask completed successfully"

	mux.HandleFunc("/api/workflows/1/attempts/100/tasks/task-1/log", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "GET")
		testURL(t, r, "/api/workflows/1/attempts/100/tasks/task-1/log")
		w.Header().Set("Content-Type", "text/plain")
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(expectedLog)))
		fmt.Fprint(w, expectedLog)
	})

	ctx := context.Background()
	log, err := client.Workflow.GetWorkflowTaskLog(ctx, "1", "100", "task-1")
	if err != nil {
		t.Errorf("Workflows.GetWorkflowTaskLog returned error: %v", err)
	}

	if log != expectedLog {
		t.Errorf("Workflows.GetWorkflowTaskLog returned %v, want %v", log, expectedLog)
	}
}

func ExampleWorkflowService_StartWorkflow() {
	client, _ := NewClient("YOUR_API_KEY")
	ctx := context.Background()

	// Start a workflow with parameters
	params := map[string]interface{}{
		"target_date": "2024-01-01",
		"run_type":    "manual",
	}

	attempt, err := client.Workflow.StartWorkflow(ctx, "123", params)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	fmt.Printf("Started workflow attempt: %s (Status: %s)\n", attempt.ID, attempt.Status)
}

func ExampleWorkflowService_GetWorkflowAttempt() {
	client, _ := NewClient("YOUR_API_KEY")
	ctx := context.Background()

	// Monitor workflow execution
	attempt, err := client.Workflow.GetWorkflowAttempt(ctx, "123", "456")
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	fmt.Printf("Attempt %s status: %s\n", attempt.ID, attempt.Status)
	if attempt.Done {
		if attempt.Success != nil && *attempt.Success {
			fmt.Println("Workflow completed successfully")
		} else {
			fmt.Println("Workflow failed")
		}
	}
}

func ExampleWorkflowService_GetWorkflowAttemptLog() {
	client, _ := NewClient("YOUR_API_KEY")
	ctx := context.Background()

	// Get logs for a workflow attempt
	log, err := client.Workflow.GetWorkflowAttemptLog(ctx, "123", "456")
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	fmt.Printf("Workflow logs:\n%s\n", log)
}
