package treasuredata

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

// setup sets up a test HTTP server along with a treasuredata.Client that is
// configured to talk to that test server. Tests should register handlers on
// mux which provide mock responses for the API method being tested.
func setup() (client *Client, mux *http.ServeMux, teardown func()) {
	// mux is the HTTP request multiplexer used with the test server.
	mux = http.NewServeMux()

	// server is a test HTTP server used to provide mock API responses.
	server := httptest.NewServer(mux)

	// client is the Treasure Data client being tested.
	client, _ = NewClient("test-api-key")
	url, _ := url.Parse(server.URL + "/")
	client.BaseURL = url
	client.WorkflowURL = url

	return client, mux, func() {
		server.Close()
	}
}

// testMethod is a helper function to test that the HTTP method used is correct.
func testMethod(t *testing.T, r *http.Request, want string) {
	t.Helper()
	if got := r.Method; got != want {
		t.Errorf("Request method: %v, want %v", got, want)
	}
}

// testURL is a helper function to test that the URL path is correct.
func testURL(t *testing.T, r *http.Request, want string) {
	t.Helper()
	if got := r.URL.String(); got != want {
		t.Errorf("Request URL: %v, want %v", got, want)
	}
}

func TestWorkflowService_ListWorkflows(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/api/workflows", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "GET")
		testURL(t, r, "/api/workflows")

		fmt.Fprint(w, `{
			"workflows": [
				{
					"id": 1,
					"name": "test-workflow",
					"project": "default",
					"revision": "abc123",
					"status": "active",
					"config": "timezone: UTC\n",
					"created_at": 1609459200,
					"updated_at": 1609459200,
					"timezone": "UTC"
				}
			]
		}`)
	})

	ctx := context.Background()
	resp, err := client.Workflow.ListWorkflows(ctx, nil)
	if err != nil {
		t.Errorf("Workflows.ListWorkflows returned error: %v", err)
	}

	want := &WorkflowListResponse{
		Workflows: []Workflow{
			{
				ID:        1,
				Name:      "test-workflow",
				Project:   "default",
				Revision:  "abc123",
				Status:    "active",
				Config:    "timezone: UTC\n",
				CreatedAt: TDTime{time.Unix(1609459200, 0)},
				UpdatedAt: TDTime{time.Unix(1609459200, 0)},
				Timezone:  "UTC",
			},
		},
	}

	if !reflect.DeepEqual(resp, want) {
		t.Errorf("Workflows.ListWorkflows returned %+v, want %+v", resp, want)
	}
}

func TestWorkflowService_ListWorkflows_withOptions(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/api/workflows", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "GET")
		testURL(t, r, "/api/workflows?limit=10&offset=20")

		fmt.Fprint(w, `{"workflows": []}`)
	})

	ctx := context.Background()
	opts := &WorkflowListOptions{
		Limit:  10,
		Offset: 20,
	}
	_, err := client.Workflow.ListWorkflows(ctx, opts)
	if err != nil {
		t.Errorf("Workflows.ListWorkflows returned error: %v", err)
	}
}

func TestWorkflowService_GetWorkflow(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/api/workflows/1", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "GET")
		testURL(t, r, "/api/workflows/1")

		fmt.Fprint(w, `{
			"id": 1,
			"name": "test-workflow",
			"project": "default",
			"revision": "abc123",
			"status": "active",
			"config": "timezone: UTC\n",
			"created_at": 1609459200,
			"updated_at": 1609459200,
			"last_attempt": 10,
			"next_schedule": 1609545600,
			"timezone": "UTC"
		}`)
	})

	ctx := context.Background()
	workflow, err := client.Workflow.GetWorkflow(ctx, 1)
	if err != nil {
		t.Errorf("Workflows.GetWorkflow returned error: %v", err)
	}

	lastAttempt := 10
	nextSchedule := TDTime{time.Unix(1609545600, 0)}
	want := &Workflow{
		ID:           1,
		Name:         "test-workflow",
		Project:      "default",
		Revision:     "abc123",
		Status:       "active",
		Config:       "timezone: UTC\n",
		CreatedAt:    TDTime{time.Unix(1609459200, 0)},
		UpdatedAt:    TDTime{time.Unix(1609459200, 0)},
		LastAttempt:  &lastAttempt,
		NextSchedule: &nextSchedule,
		Timezone:     "UTC",
	}

	if !reflect.DeepEqual(workflow, want) {
		t.Errorf("Workflows.GetWorkflow returned %+v, want %+v", workflow, want)
	}
}

func TestWorkflowService_CreateWorkflow(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/api/workflows", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "POST")
		testURL(t, r, "/api/workflows")

		var body map[string]string
		json.NewDecoder(r.Body).Decode(&body)

		if body["name"] != "new-workflow" {
			t.Errorf("Request body name = %v, want %v", body["name"], "new-workflow")
		}
		if body["project"] != "default" {
			t.Errorf("Request body project = %v, want %v", body["project"], "default")
		}
		if body["config"] != "timezone: UTC\n" {
			t.Errorf("Request body config = %v, want %v", body["config"], "timezone: UTC\n")
		}

		fmt.Fprint(w, `{
			"id": 2,
			"name": "new-workflow",
			"project": "default",
			"revision": "def456",
			"status": "active",
			"config": "timezone: UTC\n",
			"created_at": 1609459200,
			"updated_at": 1609459200,
			"timezone": "UTC"
		}`)
	})

	ctx := context.Background()
	workflow, err := client.Workflow.CreateWorkflow(ctx, "new-workflow", "default", "timezone: UTC\n")
	if err != nil {
		t.Errorf("Workflows.CreateWorkflow returned error: %v", err)
	}

	want := &Workflow{
		ID:        2,
		Name:      "new-workflow",
		Project:   "default",
		Revision:  "def456",
		Status:    "active",
		Config:    "timezone: UTC\n",
		CreatedAt: TDTime{time.Unix(1609459200, 0)},
		UpdatedAt: TDTime{time.Unix(1609459200, 0)},
		Timezone:  "UTC",
	}

	if !reflect.DeepEqual(workflow, want) {
		t.Errorf("Workflows.CreateWorkflow returned %+v, want %+v", workflow, want)
	}
}

func TestWorkflowService_UpdateWorkflow(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/api/workflows/1", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "PUT")
		testURL(t, r, "/api/workflows/1")

		var body map[string]string
		json.NewDecoder(r.Body).Decode(&body)

		if body["name"] != "updated-workflow" {
			t.Errorf("Request body name = %v, want %v", body["name"], "updated-workflow")
		}

		fmt.Fprint(w, `{
			"id": 1,
			"name": "updated-workflow",
			"project": "default",
			"revision": "def456",
			"status": "active",
			"config": "timezone: UTC\n",
			"created_at": 1609459200,
			"updated_at": 1609545600,
			"timezone": "UTC"
		}`)
	})

	ctx := context.Background()
	updates := map[string]string{
		"name": "updated-workflow",
	}
	workflow, err := client.Workflow.UpdateWorkflow(ctx, 1, updates)
	if err != nil {
		t.Errorf("Workflows.UpdateWorkflow returned error: %v", err)
	}

	want := &Workflow{
		ID:        1,
		Name:      "updated-workflow",
		Project:   "default",
		Revision:  "def456",
		Status:    "active",
		Config:    "timezone: UTC\n",
		CreatedAt: TDTime{time.Unix(1609459200, 0)},
		UpdatedAt: TDTime{time.Unix(1609545600, 0)},
		Timezone:  "UTC",
	}

	if !reflect.DeepEqual(workflow, want) {
		t.Errorf("Workflows.UpdateWorkflow returned %+v, want %+v", workflow, want)
	}
}

func TestWorkflowService_DeleteWorkflow(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/api/workflows/1", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "DELETE")
		testURL(t, r, "/api/workflows/1")
		w.WriteHeader(http.StatusNoContent)
	})

	ctx := context.Background()
	err := client.Workflow.DeleteWorkflow(ctx, 1)
	if err != nil {
		t.Errorf("Workflows.DeleteWorkflow returned error: %v", err)
	}
}

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
			"id": 100,
			"index": 1,
			"workflow_id": 1,
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
	attempt, err := client.Workflow.StartWorkflow(ctx, 1, params)
	if err != nil {
		t.Errorf("Workflows.StartWorkflow returned error: %v", err)
	}

	sessionID := "123-456"
	sessionUUID := "uuid-123"
	sessionTime := TDTime{time.Unix(1609459200, 0)}
	want := &WorkflowAttempt{
		ID:          100,
		Index:       1,
		WorkflowID:  1,
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
					"id": 100,
					"index": 1,
					"workflow_id": 1,
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
	resp, err := client.Workflow.ListWorkflowAttempts(ctx, 1, opts)
	if err != nil {
		t.Errorf("Workflows.ListWorkflowAttempts returned error: %v", err)
	}

	finishedAt := TDTime{time.Unix(1609462800, 0)}
	success := true
	want := &WorkflowAttemptListResponse{
		Attempts: []WorkflowAttempt{
			{
				ID:         100,
				Index:      1,
				WorkflowID: 1,
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
			"id": 100,
			"index": 1,
			"workflow_id": 1,
			"status": "success",
			"created_at": 1609459200,
			"finished_at": 1609462800,
			"log_file_size": 1024,
			"success": true,
			"done": true
		}`)
	})

	ctx := context.Background()
	attempt, err := client.Workflow.GetWorkflowAttempt(ctx, 1, 100)
	if err != nil {
		t.Errorf("Workflows.GetWorkflowAttempt returned error: %v", err)
	}

	finishedAt := TDTime{time.Unix(1609462800, 0)}
	logFileSize := int64(1024)
	success := true
	want := &WorkflowAttempt{
		ID:          100,
		Index:       1,
		WorkflowID:  1,
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
	err := client.Workflow.KillWorkflowAttempt(ctx, 1, 100)
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
			"id": 101,
			"index": 2,
			"workflow_id": 1,
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
	attempt, err := client.Workflow.RetryWorkflowAttempt(ctx, 1, 100, params)
	if err != nil {
		t.Errorf("Workflows.RetryWorkflowAttempt returned error: %v", err)
	}

	want := &WorkflowAttempt{
		ID:         101,
		Index:      2,
		WorkflowID: 1,
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
	resp, err := client.Workflow.ListWorkflowTasks(ctx, 1, 100)
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
	task, err := client.Workflow.GetWorkflowTask(ctx, 1, 100, "task-1")
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

func TestWorkflowService_GetWorkflowSchedule(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/api/workflows/1/schedule", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "GET")
		testURL(t, r, "/api/workflows/1/schedule")

		fmt.Fprint(w, `{
			"id": 10,
			"workflow_id": 1,
			"cron": "0 * * * *",
			"timezone": "UTC",
			"delay": 0,
			"next_time": 1609462800,
			"next_schedule_time": 1609462800,
			"created_at": 1609459200,
			"updated_at": 1609459200
		}`)
	})

	ctx := context.Background()
	schedule, err := client.Workflow.GetWorkflowSchedule(ctx, 1)
	if err != nil {
		t.Errorf("Workflows.GetWorkflowSchedule returned error: %v", err)
	}

	nextTime := TDTime{time.Unix(1609462800, 0)}
	nextScheduleTime := TDTime{time.Unix(1609462800, 0)}
	want := &WorkflowSchedule{
		ID:               10,
		WorkflowID:       1,
		Cron:             "0 * * * *",
		Timezone:         "UTC",
		Delay:            0,
		NextTime:         &nextTime,
		NextScheduleTime: &nextScheduleTime,
		CreatedAt:        TDTime{time.Unix(1609459200, 0)},
		UpdatedAt:        TDTime{time.Unix(1609459200, 0)},
	}

	if !reflect.DeepEqual(schedule, want) {
		t.Errorf("Workflows.GetWorkflowSchedule returned %+v, want %+v", schedule, want)
	}
}

func TestWorkflowService_EnableWorkflowSchedule(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/api/workflows/1/schedule/enable", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "POST")
		testURL(t, r, "/api/workflows/1/schedule/enable")

		fmt.Fprint(w, `{
			"id": 10,
			"workflow_id": 1,
			"cron": "0 * * * *",
			"timezone": "UTC",
			"delay": 0,
			"created_at": 1609459200,
			"updated_at": 1609545600
		}`)
	})

	ctx := context.Background()
	schedule, err := client.Workflow.EnableWorkflowSchedule(ctx, 1)
	if err != nil {
		t.Errorf("Workflows.EnableWorkflowSchedule returned error: %v", err)
	}

	want := &WorkflowSchedule{
		ID:         10,
		WorkflowID: 1,
		Cron:       "0 * * * *",
		Timezone:   "UTC",
		Delay:      0,
		CreatedAt:  TDTime{time.Unix(1609459200, 0)},
		UpdatedAt:  TDTime{time.Unix(1609545600, 0)},
	}

	if !reflect.DeepEqual(schedule, want) {
		t.Errorf("Workflows.EnableWorkflowSchedule returned %+v, want %+v", schedule, want)
	}
}

func TestWorkflowService_DisableWorkflowSchedule(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/api/workflows/1/schedule/disable", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "POST")
		testURL(t, r, "/api/workflows/1/schedule/disable")

		fmt.Fprint(w, `{
			"id": 10,
			"workflow_id": 1,
			"cron": "0 * * * *",
			"timezone": "UTC",
			"delay": 0,
			"disabled_at": 1609545600,
			"created_at": 1609459200,
			"updated_at": 1609545600
		}`)
	})

	ctx := context.Background()
	schedule, err := client.Workflow.DisableWorkflowSchedule(ctx, 1)
	if err != nil {
		t.Errorf("Workflows.DisableWorkflowSchedule returned error: %v", err)
	}

	disabledAt := TDTime{time.Unix(1609545600, 0)}
	want := &WorkflowSchedule{
		ID:         10,
		WorkflowID: 1,
		Cron:       "0 * * * *",
		Timezone:   "UTC",
		Delay:      0,
		DisabledAt: &disabledAt,
		CreatedAt:  TDTime{time.Unix(1609459200, 0)},
		UpdatedAt:  TDTime{time.Unix(1609545600, 0)},
	}

	if !reflect.DeepEqual(schedule, want) {
		t.Errorf("Workflows.DisableWorkflowSchedule returned %+v, want %+v", schedule, want)
	}
}

func TestWorkflowService_UpdateWorkflowSchedule(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/api/workflows/1/schedule", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "PUT")
		testURL(t, r, "/api/workflows/1/schedule")

		var body map[string]interface{}
		json.NewDecoder(r.Body).Decode(&body)

		if body["cron"] != "30 * * * *" {
			t.Errorf("Request body cron = %v, want %v", body["cron"], "30 * * * *")
		}
		if body["timezone"] != "America/New_York" {
			t.Errorf("Request body timezone = %v, want %v", body["timezone"], "America/New_York")
		}
		if body["delay"] != float64(300) { // JSON numbers are float64
			t.Errorf("Request body delay = %v, want %v", body["delay"], 300)
		}

		fmt.Fprint(w, `{
			"id": 10,
			"workflow_id": 1,
			"cron": "30 * * * *",
			"timezone": "America/New_York",
			"delay": 300,
			"created_at": 1609459200,
			"updated_at": 1609545600
		}`)
	})

	ctx := context.Background()
	schedule, err := client.Workflow.UpdateWorkflowSchedule(ctx, 1, "30 * * * *", "America/New_York", 300)
	if err != nil {
		t.Errorf("Workflows.UpdateWorkflowSchedule returned error: %v", err)
	}

	want := &WorkflowSchedule{
		ID:         10,
		WorkflowID: 1,
		Cron:       "30 * * * *",
		Timezone:   "America/New_York",
		Delay:      300,
		CreatedAt:  TDTime{time.Unix(1609459200, 0)},
		UpdatedAt:  TDTime{time.Unix(1609545600, 0)},
	}

	if !reflect.DeepEqual(schedule, want) {
		t.Errorf("Workflows.UpdateWorkflowSchedule returned %+v, want %+v", schedule, want)
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
	log, err := client.Workflow.GetWorkflowAttemptLog(ctx, 1, 100)
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
	log, err := client.Workflow.GetWorkflowTaskLog(ctx, 1, 100, "task-1")
	if err != nil {
		t.Errorf("Workflows.GetWorkflowTaskLog returned error: %v", err)
	}

	if log != expectedLog {
		t.Errorf("Workflows.GetWorkflowTaskLog returned %v, want %v", log, expectedLog)
	}
}

func TestWorkflowService_ErrorResponse(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/api/workflows/999", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "GET")
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprint(w, `{
			"error": "Workflow not found",
			"message": "The workflow with ID 999 does not exist"
		}`)
	})

	ctx := context.Background()
	_, err := client.Workflow.GetWorkflow(ctx, 999)
	if err == nil {
		t.Error("Expected error to be returned")
	}

	if tdErr, ok := err.(*ErrorResponse); ok {
		if tdErr.Response.StatusCode != http.StatusNotFound {
			t.Errorf("Expected status code 404, got %d", tdErr.Response.StatusCode)
		}
	} else {
		t.Errorf("Expected ErrorResponse, got %T", err)
	}
}

// Example tests demonstrating common workflow operations

func ExampleWorkflowService_ListWorkflows() {
	client, _ := NewClient("YOUR_API_KEY")
	ctx := context.Background()

	// List all workflows
	resp, err := client.Workflow.ListWorkflows(ctx, nil)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	for _, workflow := range resp.Workflows {
		fmt.Printf("Workflow: %s (ID: %d, Status: %s)\n", workflow.Name, workflow.ID, workflow.Status)
	}
}

func ExampleWorkflowService_CreateWorkflow() {
	client, _ := NewClient("YOUR_API_KEY")
	ctx := context.Background()

	// Create a new workflow
	config := `
timezone: UTC
_export:
  td:
    database: mydb

+task1:
  td>: queries/daily_summary.sql
`

	workflow, err := client.Workflow.CreateWorkflow(ctx, "daily-summary", "default", config)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	fmt.Printf("Created workflow: %s (ID: %d)\n", workflow.Name, workflow.ID)
}

func ExampleWorkflowService_StartWorkflow() {
	client, _ := NewClient("YOUR_API_KEY")
	ctx := context.Background()

	// Start a workflow with parameters
	params := map[string]interface{}{
		"target_date": "2024-01-01",
		"run_type":    "manual",
	}

	attempt, err := client.Workflow.StartWorkflow(ctx, 123, params)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	fmt.Printf("Started workflow attempt: %d (Status: %s)\n", attempt.ID, attempt.Status)
}

func ExampleWorkflowService_GetWorkflowAttempt() {
	client, _ := NewClient("YOUR_API_KEY")
	ctx := context.Background()

	// Monitor workflow execution
	attempt, err := client.Workflow.GetWorkflowAttempt(ctx, 123, 456)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	fmt.Printf("Attempt %d status: %s\n", attempt.ID, attempt.Status)
	if attempt.Done {
		if attempt.Success != nil && *attempt.Success {
			fmt.Println("Workflow completed successfully")
		} else {
			fmt.Println("Workflow failed")
		}
	}
}

func ExampleWorkflowService_UpdateWorkflowSchedule() {
	client, _ := NewClient("YOUR_API_KEY")
	ctx := context.Background()

	// Update workflow schedule to run daily at 2 AM UTC
	schedule, err := client.Workflow.UpdateWorkflowSchedule(ctx, 123, "0 2 * * *", "UTC", 0)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	fmt.Printf("Updated schedule: %s (Next run: %v)\n", schedule.Cron, schedule.NextTime)
}

func ExampleWorkflowService_GetWorkflowAttemptLog() {
	client, _ := NewClient("YOUR_API_KEY")
	ctx := context.Background()

	// Get logs for a workflow attempt
	log, err := client.Workflow.GetWorkflowAttemptLog(ctx, 123, 456)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	fmt.Printf("Workflow logs:\n%s\n", log)
}

func TestWorkflowService_ListProjects(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/api/projects", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "GET")
		testURL(t, r, "/api/projects")

		fmt.Fprint(w, `{
			"projects": [
				{
					"id": 1,
					"name": "test-project",
					"revision": "v1",
					"archiveType": "db",
					"archiveMd5": "abc123def456",
					"createdAt": 1609459200,
					"updatedAt": 1609459200,
					"deletedAt": null
				}
			]
		}`)
	})

	ctx := context.Background()
	resp, err := client.Workflow.ListProjects(ctx)
	if err != nil {
		t.Errorf("Workflow.ListProjects returned error: %v", err)
	}

	want := &WorkflowProjectListResponse{
		Projects: []WorkflowProject{
			{
				ID:          1,
				Name:        "test-project",
				Revision:    "v1",
				ArchiveType: "db",
				ArchiveMD5:  "abc123def456",
				CreatedAt:   TDTime{time.Unix(1609459200, 0)},
				UpdatedAt:   TDTime{time.Unix(1609459200, 0)},
				DeletedAt:   nil,
			},
		},
	}

	if !reflect.DeepEqual(resp, want) {
		t.Errorf("Workflow.ListProjects returned %+v, want %+v", resp, want)
	}
}

func TestWorkflowService_GetProject(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/api/projects/1", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "GET")
		testURL(t, r, "/api/projects/1")

		fmt.Fprint(w, `{
			"id": 1,
			"name": "test-project",
			"revision": "v2",
			"archiveType": "db",
			"archiveMd5": "def456ghi789",
			"createdAt": 1609459200,
			"updatedAt": 1609545600,
			"deletedAt": null
		}`)
	})

	ctx := context.Background()
	project, err := client.Workflow.GetProject(ctx, 1)
	if err != nil {
		t.Errorf("Workflow.GetProject returned error: %v", err)
	}

	want := &WorkflowProject{
		ID:          1,
		Name:        "test-project",
		Revision:    "v2",
		ArchiveType: "db",
		ArchiveMD5:  "def456ghi789",
		CreatedAt:   TDTime{time.Unix(1609459200, 0)},
		UpdatedAt:   TDTime{time.Unix(1609545600, 0)},
		DeletedAt:   nil,
	}

	if !reflect.DeepEqual(project, want) {
		t.Errorf("Workflow.GetProject returned %+v, want %+v", project, want)
	}
}

func TestWorkflowService_CreateProject(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/api/projects", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "PUT")
		testURL(t, r, "/api/projects?project=new-project")

		fmt.Fprint(w, `{
			"id": 2,
			"name": "new-project",
			"revision": "v1",
			"archiveType": "db",
			"archiveMd5": "new123archive456",
			"createdAt": 1609459200,
			"updatedAt": 1609459200,
			"deletedAt": null
		}`)
	})

	ctx := context.Background()
	archive := []byte("sample archive data")
	project, err := client.Workflow.CreateProject(ctx, "new-project", archive)
	if err != nil {
		t.Errorf("Workflow.CreateProject returned error: %v", err)
	}

	want := &WorkflowProject{
		ID:          2,
		Name:        "new-project",
		Revision:    "v1",
		ArchiveType: "db",
		ArchiveMD5:  "new123archive456",
		CreatedAt:   TDTime{time.Unix(1609459200, 0)},
		UpdatedAt:   TDTime{time.Unix(1609459200, 0)},
		DeletedAt:   nil,
	}

	if !reflect.DeepEqual(project, want) {
		t.Errorf("Workflow.CreateProject returned %+v, want %+v", project, want)
	}
}

func TestWorkflowService_ListProjectWorkflows(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/api/projects/1/workflows", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "GET")
		testURL(t, r, "/api/projects/1/workflows")

		fmt.Fprint(w, `{
			"workflows": [
				{
					"id": 10,
					"name": "project-workflow",
					"project": "test-project",
					"revision": "abc123",
					"status": "active",
					"config": "timezone: UTC\n",
					"created_at": 1609459200,
					"updated_at": 1609459200,
					"timezone": "UTC"
				}
			]
		}`)
	})

	ctx := context.Background()
	resp, err := client.Workflow.ListProjectWorkflows(ctx, 1)
	if err != nil {
		t.Errorf("Workflow.ListProjectWorkflows returned error: %v", err)
	}

	want := &WorkflowListResponse{
		Workflows: []Workflow{
			{
				ID:        10,
				Name:      "project-workflow",
				Project:   "test-project",
				Revision:  "abc123",
				Status:    "active",
				Config:    "timezone: UTC\n",
				CreatedAt: TDTime{time.Unix(1609459200, 0)},
				UpdatedAt: TDTime{time.Unix(1609459200, 0)},
				Timezone:  "UTC",
			},
		},
	}

	if !reflect.DeepEqual(resp, want) {
		t.Errorf("Workflow.ListProjectWorkflows returned %+v, want %+v", resp, want)
	}
}

func TestWorkflowService_GetProjectSecrets(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/api/projects/1/secrets", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "GET")
		testURL(t, r, "/api/projects/1/secrets")

		fmt.Fprint(w, `{
			"secrets": {
				"api_key": "****",
				"database_url": "****"
			}
		}`)
	})

	ctx := context.Background()
	resp, err := client.Workflow.GetProjectSecrets(ctx, 1)
	if err != nil {
		t.Errorf("Workflow.GetProjectSecrets returned error: %v", err)
	}

	want := &WorkflowProjectSecretsResponse{
		Secrets: map[string]string{
			"api_key":      "****",
			"database_url": "****",
		},
	}

	if !reflect.DeepEqual(resp, want) {
		t.Errorf("Workflow.GetProjectSecrets returned %+v, want %+v", resp, want)
	}
}

func TestWorkflowService_SetProjectSecret(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/api/projects/1/secrets/test_secret", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "PUT")
		testURL(t, r, "/api/projects/1/secrets/test_secret")

		var body map[string]string
		json.NewDecoder(r.Body).Decode(&body)

		if body["value"] != "secret_value" {
			t.Errorf("Request body value = %v, want %v", body["value"], "secret_value")
		}

		w.WriteHeader(http.StatusOK)
	})

	ctx := context.Background()
	err := client.Workflow.SetProjectSecret(ctx, 1, "test_secret", "secret_value")
	if err != nil {
		t.Errorf("Workflow.SetProjectSecret returned error: %v", err)
	}
}

func TestWorkflowService_DeleteProjectSecret(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/api/projects/1/secrets/test_secret", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "DELETE")
		testURL(t, r, "/api/projects/1/secrets/test_secret")
		w.WriteHeader(http.StatusNoContent)
	})

	ctx := context.Background()
	err := client.Workflow.DeleteProjectSecret(ctx, 1, "test_secret")
	if err != nil {
		t.Errorf("Workflow.DeleteProjectSecret returned error: %v", err)
	}
}

func ExampleWorkflowService_ListProjects() {
	client, _ := NewClient("YOUR_API_KEY")
	ctx := context.Background()

	// List all workflow projects
	resp, err := client.Workflow.ListProjects(ctx)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	for _, project := range resp.Projects {
		fmt.Printf("Project: %s (ID: %d, Revision: %s)\n", project.Name, project.ID, project.Revision)
	}
}

func ExampleWorkflowService_CreateProject() {
	client, _ := NewClient("YOUR_API_KEY")
	ctx := context.Background()

	// Create a new workflow project with archive data
	archive := []byte("compressed workflow files")
	project, err := client.Workflow.CreateProject(ctx, "my-new-project", archive)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	fmt.Printf("Created project: %s (ID: %d)\n", project.Name, project.ID)
}

func ExampleWorkflowService_SetProjectSecret() {
	client, _ := NewClient("YOUR_API_KEY")
	ctx := context.Background()

	// Set a secret for a project
	err := client.Workflow.SetProjectSecret(ctx, 123, "database_password", "my_secret_password")
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	fmt.Println("Secret set successfully")
}

func TestWorkflowService_CreateProjectFromDirectory(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/api/projects", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "PUT")
		testURL(t, r, "/api/projects?project=test-project")

		// Verify we received some archive data
		if r.ContentLength == 0 {
			t.Error("Expected non-empty request body")
		}

		fmt.Fprint(w, `{
			"id": 3,
			"name": "test-project",
			"revision": "v1",
			"archiveType": "db",
			"archiveMd5": "directory123hash456",
			"createdAt": 1609459200,
			"updatedAt": 1609459200,
			"deletedAt": null
		}`)
	})

	// Create a temporary directory with test files
	tempDir := t.TempDir()
	testFile := filepath.Join(tempDir, "test.txt")
	err := os.WriteFile(testFile, []byte("test content"), 0644)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	ctx := context.Background()
	project, err := client.Workflow.CreateProjectFromDirectory(ctx, "test-project", tempDir)
	if err != nil {
		t.Errorf("Workflow.CreateProjectFromDirectory returned error: %v", err)
	}

	want := &WorkflowProject{
		ID:          3,
		Name:        "test-project",
		Revision:    "v1",
		ArchiveType: "db",
		ArchiveMD5:  "directory123hash456",
		CreatedAt:   TDTime{time.Unix(1609459200, 0)},
		UpdatedAt:   TDTime{time.Unix(1609459200, 0)},
		DeletedAt:   nil,
	}

	if !reflect.DeepEqual(project, want) {
		t.Errorf("Workflow.CreateProjectFromDirectory returned %+v, want %+v", project, want)
	}
}

func TestCreateTarGz(t *testing.T) {
	// Create a temporary directory with test files
	tempDir := t.TempDir()

	// Create some test files
	err := os.WriteFile(filepath.Join(tempDir, "workflow.dig"), []byte("timezone: UTC\n"), 0644)
	if err != nil {
		t.Fatalf("Failed to create workflow.dig: %v", err)
	}

	err = os.WriteFile(filepath.Join(tempDir, "query.sql"), []byte("SELECT 1"), 0644)
	if err != nil {
		t.Fatalf("Failed to create query.sql: %v", err)
	}

	// Create a subdirectory
	subDir := filepath.Join(tempDir, "subdir")
	err = os.Mkdir(subDir, 0755)
	if err != nil {
		t.Fatalf("Failed to create subdirectory: %v", err)
	}

	err = os.WriteFile(filepath.Join(subDir, "nested.txt"), []byte("nested content"), 0644)
	if err != nil {
		t.Fatalf("Failed to create nested file: %v", err)
	}

	// Create a hidden file (should be skipped)
	err = os.WriteFile(filepath.Join(tempDir, ".hidden"), []byte("hidden content"), 0644)
	if err != nil {
		t.Fatalf("Failed to create hidden file: %v", err)
	}

	// Test the createTarGz function
	archive, err := createTarGz(tempDir)
	if err != nil {
		t.Fatalf("createTarGz failed: %v", err)
	}

	// Verify we got some archive data
	if len(archive) == 0 {
		t.Error("Expected non-empty archive")
	}

	// Archive should be at least a few hundred bytes for our test files
	if len(archive) < 100 {
		t.Errorf("Archive seems too small: %d bytes", len(archive))
	}
}
