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

func TestWorkflowService_ListWorkflows(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/api/workflows", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "GET")
		testURL(t, r, "/api/workflows")

		fmt.Fprint(w, `{
			"workflows": [
				{
					"id": "1",
					"name": "test-workflow",
					"project": {"id": "default", "name": "default"},
					"revision": "abc123",
					"status": "active",
					"config": {"timezone": "UTC"},
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
				ID:        "1",
				Name:      "test-workflow",
				Project:   WorkflowProjectRef{ID: "default", Name: "default"},
				Revision:  "abc123",
				Status:    "active",
				Config:    map[string]interface{}{"timezone": "UTC"},
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
			"id": "1",
			"name": "test-workflow",
			"project": {"id": "default", "name": "default"},
			"revision": "abc123",
			"status": "active",
			"config": {"timezone": "UTC"},
			"created_at": 1609459200,
			"updated_at": 1609459200,
			"last_attempt": 10,
			"next_schedule": 1609545600,
			"timezone": "UTC"
		}`)
	})

	ctx := context.Background()
	workflow, err := client.Workflow.GetWorkflow(ctx, "1")
	if err != nil {
		t.Errorf("Workflows.GetWorkflow returned error: %v", err)
	}

	lastAttempt := 10
	nextSchedule := TDTime{time.Unix(1609545600, 0)}
	want := &Workflow{
		ID:           "1",
		Name:         "test-workflow",
		Project:      WorkflowProjectRef{ID: "default", Name: "default"},
		Revision:     "abc123",
		Status:       "active",
		Config:       map[string]interface{}{"timezone": "UTC"},
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
			"id": "2",
			"name": "new-workflow",
			"project": {"id": "default", "name": "default"},
			"revision": "def456",
			"status": "active",
			"config": {"timezone": "UTC"},
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
		ID:        "2",
		Name:      "new-workflow",
		Project:   WorkflowProjectRef{ID: "default", Name: "default"},
		Revision:  "def456",
		Status:    "active",
		Config:    map[string]interface{}{"timezone": "UTC"},
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
			"id": "1",
			"name": "updated-workflow",
			"project": {"id": "default", "name": "default"},
			"revision": "def456",
			"status": "active",
			"config": {"timezone": "UTC"},
			"created_at": 1609459200,
			"updated_at": 1609545600,
			"timezone": "UTC"
		}`)
	})

	ctx := context.Background()
	updates := map[string]string{
		"name": "updated-workflow",
	}
	workflow, err := client.Workflow.UpdateWorkflow(ctx, "1", updates)
	if err != nil {
		t.Errorf("Workflows.UpdateWorkflow returned error: %v", err)
	}

	want := &Workflow{
		ID:        "1",
		Name:      "updated-workflow",
		Project:   WorkflowProjectRef{ID: "default", Name: "default"},
		Revision:  "def456",
		Status:    "active",
		Config:    map[string]interface{}{"timezone": "UTC"},
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
	err := client.Workflow.DeleteWorkflow(ctx, "1")
	if err != nil {
		t.Errorf("Workflows.DeleteWorkflow returned error: %v", err)
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
	_, err := client.Workflow.GetWorkflow(ctx, "999")
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
		fmt.Printf("Workflow: %s (ID: %s, Status: %s)\n", workflow.Name, workflow.ID, workflow.Status)
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

	fmt.Printf("Created workflow: %s (ID: %s)\n", workflow.Name, workflow.ID)
}
