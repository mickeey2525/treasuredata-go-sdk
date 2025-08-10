package workflow

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
)

func TestHandleWorkflowTaskList(t *testing.T) {
	tests := []struct {
		name           string
		args           []string
		format         string
		mockResponse   string
		expectedOutput []string
	}{
		{
			name:   "Table format with tasks",
			args:   []string{"123", "456"},
			format: "table",
			mockResponse: `{
				"tasks": [
					{
						"id": "task1",
						"name": "setup",
						"full_name": "+setup",
						"state": "success",
						"started_at": "2024-01-01T09:00:00Z",
						"finished_at": "2024-01-01T09:01:00Z"
					},
					{
						"id": "task2",
						"name": "query",
						"full_name": "+query_and_export+query",
						"state": "running",
						"started_at": "2024-01-01T09:01:00Z",
						"finished_at": null
					}
				]
			}`,
			expectedOutput: []string{
				"ID", "NAME", "STATE", "GROUP", "STARTED",
				"task1", "+setup", "success", "false", "2024-01-01 09:00:00",
				"task2", "+query_and_export+query", "running", "false", "2024-01-01 09:01:00",
				"Total: 2 tasks",
			},
		},
		{
			name:   "CSV format with tasks",
			args:   []string{"123", "456"},
			format: "csv",
			mockResponse: `{
				"tasks": [
					{
						"id": "task1",
						"full_name": "+setup",
						"state": "success",
						"is_group": false,
						"started_at": "2024-01-01T09:00:00Z",
						"updated_at": "0001-01-01T00:00:00Z"
					}
				]
			}`,
			expectedOutput: []string{
				"id,full_name,state,is_group,started_at,updated_at",
				"task1,+setup,success,false,2024-01-01 09:00:00,0001-01-01 00:00:00",
			},
		},
		{
			name:   "Empty tasks list",
			args:   []string{"123", "456"},
			format: "table",
			mockResponse: `{
				"tasks": []
			}`,
			expectedOutput: []string{
				"No tasks found",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, mux, teardown := setupWorkflowTest()
			defer teardown()

			mux.HandleFunc("/api/workflows/"+tt.args[0]+"/attempts/"+tt.args[1]+"/tasks", func(w http.ResponseWriter, r *http.Request) {
				if r.Method != "GET" {
					t.Errorf("Expected GET request, got %s", r.Method)
				}
				fmt.Fprint(w, tt.mockResponse)
			})

			// Capture stdout
			oldStdout := os.Stdout
			r, w, _ := os.Pipe()
			os.Stdout = w

			// Call the function
			flags := Flags{Format: tt.format}
			HandleWorkflowTaskList(context.Background(), client, tt.args, flags)

			// Restore stdout and read output
			w.Close()
			os.Stdout = oldStdout
			output, _ := io.ReadAll(r)
			outputStr := string(output)

			// Verify all expected strings are in the output
			for _, expected := range tt.expectedOutput {
				if !strings.Contains(outputStr, expected) {
					t.Errorf("Expected output to contain %q, but got:\n%s", expected, outputStr)
				}
			}
		})
	}
}

func TestHandleWorkflowTaskGet(t *testing.T) {
	tests := []struct {
		name           string
		args           []string
		format         string
		mockResponse   string
		expectedOutput []string
	}{
		{
			name:   "Valid task - table format",
			args:   []string{"123", "456", "task1"},
			format: "table",
			mockResponse: `{
				"id": "task1",
				"name": "setup",
				"full_name": "+setup",
				"state": "success",
				"parent_id": null,
				"upstream_ids": [],
				"started_at": "2024-01-01T09:00:00Z",
				"finished_at": "2024-01-01T09:01:00Z",
				"config": {
					"echo>": "Setting up the project..."
				}
			}`,
			expectedOutput: []string{
				"ID: task1",
				"Full Name: +setup",
				"State: success",
				"Is Group: false",
				"Started: 2024-01-01 09:00:00",
				"Config:",
				"echo\\u003e",
			},
		},
		{
			name:   "Valid task - CSV format",
			args:   []string{"123", "456", "task1"},
			format: "csv",
			mockResponse: `{
				"id": "task1",
				"full_name": "+setup",
				"state": "success",
				"is_group": false,
				"started_at": "2024-01-01T09:00:00Z",
				"updated_at": "0001-01-01T00:00:00Z"
			}`,
			expectedOutput: []string{
				"id,full_name,state,is_group,started_at,updated_at",
				"task1,+setup,success,false,2024-01-01 09:00:00,0001-01-01 00:00:00",
			},
		},
		{
			name:   "Running task without finished_at",
			args:   []string{"123", "456", "task2"},
			format: "table",
			mockResponse: `{
				"id": "task2",
				"name": "query",
				"full_name": "+query_and_export+query",
				"state": "running",
				"started_at": "2024-01-01T09:01:00Z",
				"finished_at": null
			}`,
			expectedOutput: []string{
				"ID: task2",
				"State: running",
				"Started: 2024-01-01 09:01:00",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, mux, teardown := setupWorkflowTest()
			defer teardown()

			mux.HandleFunc("/api/workflows/"+tt.args[0]+"/attempts/"+tt.args[1]+"/tasks/"+tt.args[2], func(w http.ResponseWriter, r *http.Request) {
				if r.Method != "GET" {
					t.Errorf("Expected GET request, got %s", r.Method)
				}
				fmt.Fprint(w, tt.mockResponse)
			})

			// Capture stdout
			oldStdout := os.Stdout
			r, w, _ := os.Pipe()
			os.Stdout = w

			// Call the function
			flags := Flags{Format: tt.format}
			HandleWorkflowTaskGet(context.Background(), client, tt.args, flags)

			// Restore stdout and read output
			w.Close()
			os.Stdout = oldStdout
			output, _ := io.ReadAll(r)
			outputStr := string(output)

			// Verify all expected strings are in the output
			for _, expected := range tt.expectedOutput {
				if !strings.Contains(outputStr, expected) {
					t.Errorf("Expected output to contain %q, but got:\n%s", expected, outputStr)
				}
			}
		})
	}
}

func TestHandleWorkflowTaskListJSON(t *testing.T) {
	client, mux, teardown := setupWorkflowTest()
	defer teardown()

	mux.HandleFunc("/api/workflows/123/attempts/456/tasks", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{
			"tasks": [
				{
					"id": "task1",
					"name": "setup",
					"full_name": "+setup",
					"state": "success",
					"started_at": "2024-01-01T09:00:00Z",
					"finished_at": "2024-01-01T09:01:00Z"
				}
			]
		}`)
	})

	// Capture stdout
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	// Call the function with JSON format
	flags := Flags{Format: "json"}
	HandleWorkflowTaskList(context.Background(), client, []string{"123", "456"}, flags)

	// Restore stdout and read output
	w.Close()
	os.Stdout = oldStdout
	output, _ := io.ReadAll(r)
	outputStr := string(output)

	// Verify JSON output contains expected fields
	expectedFields := []string{
		`"tasks"`,
		`"id": "task1"`,
		`"full_name": "+setup"`,
		`"state": "success"`,
	}

	for _, expected := range expectedFields {
		if !strings.Contains(outputStr, expected) {
			t.Errorf("Expected JSON output to contain %q, but got:\n%s", expected, outputStr)
		}
	}
}
