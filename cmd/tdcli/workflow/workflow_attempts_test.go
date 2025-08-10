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

func TestHandleWorkflowAttemptList(t *testing.T) {
	tests := []struct {
		name           string
		args           []string
		format         string
		mockResponse   string
		expectedOutput []string
		expectError    bool
	}{
		{
			name:   "Table format with attempts",
			args:   []string{"123"},
			format: "table",
			mockResponse: `{
				"attempts": [
					{
						"id": "456",
						"index": 1,
						"status": "success",
						"created_at": "2024-01-01T09:00:00Z",
						"finished_at": "2024-01-01T09:05:00Z"
					},
					{
						"id": "789",
						"index": 2,
						"status": "running",
						"created_at": "2024-01-01T10:00:00Z",
						"finished_at": null
					}
				]
			}`,
			expectedOutput: []string{
				"ID", "INDEX", "STATUS", "CREATED", "FINISHED",
				"456", "1", "success", "2024-01-01 09:00:00", "2024-01-01 09:05:00",
				"789", "2", "running", "2024-01-01 10:00:00", "-",
				"Total: 2 attempts",
			},
		},
		{
			name:   "CSV format with attempts",
			args:   []string{"123"},
			format: "csv",
			mockResponse: `{
				"attempts": [
					{
						"id": "456",
						"index": 1,
						"status": "success",
						"created_at": "2024-01-01T09:00:00Z",
						"finished_at": "2024-01-01T09:05:00Z"
					}
				]
			}`,
			expectedOutput: []string{
				"id,index,status,created_at,finished_at",
				"456,1,success,2024-01-01 09:00:00,2024-01-01 09:05:00",
			},
		},
		{
			name:   "Empty attempts list",
			args:   []string{"123"},
			format: "table",
			mockResponse: `{
				"attempts": []
			}`,
			expectedOutput: []string{
				"No attempts found",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, mux, teardown := setupWorkflowTest()
			defer teardown()

			mux.HandleFunc("/api/workflows/"+tt.args[0]+"/attempts", func(w http.ResponseWriter, r *http.Request) {
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
			HandleWorkflowAttemptList(context.Background(), client, tt.args, flags)

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

func TestHandleWorkflowAttemptGet(t *testing.T) {
	tests := []struct {
		name           string
		args           []string
		format         string
		mockResponse   string
		expectedOutput []string
	}{
		{
			name:   "Valid attempt - table format",
			args:   []string{"123", "456"},
			format: "table",
			mockResponse: `{
				"id": "456",
				"index": 1,
				"workflow_id": "123",
				"status": "success",
				"created_at": "2024-01-01T09:00:00Z",
				"finished_at": "2024-01-01T09:05:00Z",
				"done": true,
				"success": true,
				"session_id": "sess-123",
				"params": {"key": "value"}
			}`,
			expectedOutput: []string{
				"ID: 456",
				"Index: 1",
				"Workflow ID: 123",
				"Status: success",
				"Created: 2024-01-01 09:00:00",
				"Finished: 2024-01-01 09:05:00",
				"Done: true",
				"Success: true",
				"Session ID: sess-123",
				"Parameters:",
				"\"key\": \"value\"",
			},
		},
		{
			name:   "Valid attempt - CSV format",
			args:   []string{"123", "456"},
			format: "csv",
			mockResponse: `{
				"id": "456",
				"index": 1,
				"status": "success",
				"created_at": "2024-01-01T09:00:00Z",
				"finished_at": "2024-01-01T09:05:00Z",
				"done": true,
				"success": true
			}`,
			expectedOutput: []string{
				"id,index,status,created_at,finished_at,done,success",
				"456,1,success,2024-01-01 09:00:00,2024-01-01 09:05:00,true,true",
			},
		},
		{
			name:   "Running attempt without finished_at",
			args:   []string{"123", "789"},
			format: "table",
			mockResponse: `{
				"id": "789",
				"index": 2,
				"workflow_id": "123",
				"status": "running",
				"created_at": "2024-01-01T10:00:00Z",
				"finished_at": null,
				"done": false,
				"success": null
			}`,
			expectedOutput: []string{
				"ID: 789",
				"Index: 2",
				"Status: running",
				"Done: false",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, mux, teardown := setupWorkflowTest()
			defer teardown()

			mux.HandleFunc("/api/workflows/"+tt.args[0]+"/attempts/"+tt.args[1], func(w http.ResponseWriter, r *http.Request) {
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
			HandleWorkflowAttemptGet(context.Background(), client, tt.args, flags)

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

func TestHandleWorkflowAttemptKill(t *testing.T) {
	client, mux, teardown := setupWorkflowTest()
	defer teardown()

	mux.HandleFunc("/api/workflows/123/attempts/456/kill", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			t.Errorf("Expected POST request, got %s", r.Method)
		}
		w.WriteHeader(http.StatusOK)
	})

	// Capture stdout
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	// Call the function
	flags := Flags{Format: "table"}
	args := []string{"123", "456"}
	HandleWorkflowAttemptKill(context.Background(), client, args, flags)

	// Restore stdout and read output
	w.Close()
	os.Stdout = oldStdout
	output, _ := io.ReadAll(r)
	outputStr := string(output)

	expectedOutput := "Workflow attempt 456 killed successfully"
	if !strings.Contains(outputStr, expectedOutput) {
		t.Errorf("Expected output to contain %q, but got:\n%s", expectedOutput, outputStr)
	}
}

func TestHandleWorkflowAttemptRetry(t *testing.T) {
	tests := []struct {
		name           string
		args           []string
		mockResponse   string
		expectedOutput []string
	}{
		{
			name: "Retry without parameters",
			args: []string{"123", "456"},
			mockResponse: `{
				"id": "789",
				"status": "pending"
			}`,
			expectedOutput: []string{
				"Workflow attempt retried successfully",
				"New Attempt ID: 789",
				"Status: pending",
			},
		},
		{
			name: "Retry with parameters",
			args: []string{"123", "456", `{"param1": "value1"}`},
			mockResponse: `{
				"id": "789",
				"status": "pending"
			}`,
			expectedOutput: []string{
				"Workflow attempt retried successfully",
				"New Attempt ID: 789",
				"Status: pending",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, mux, teardown := setupWorkflowTest()
			defer teardown()

			mux.HandleFunc("/api/workflows/123/attempts/456/retry", func(w http.ResponseWriter, r *http.Request) {
				if r.Method != "POST" {
					t.Errorf("Expected POST request, got %s", r.Method)
				}
				fmt.Fprint(w, tt.mockResponse)
			})

			// Capture stdout
			oldStdout := os.Stdout
			r, w, _ := os.Pipe()
			os.Stdout = w

			// Call the function
			flags := Flags{Format: "table"}
			HandleWorkflowAttemptRetry(context.Background(), client, tt.args, flags)

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

func TestHandleWorkflowAttemptListJSON(t *testing.T) {
	client, mux, teardown := setupWorkflowTest()
	defer teardown()

	mux.HandleFunc("/api/workflows/123/attempts", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{
			"attempts": [
				{
					"id": "456",
					"index": 1,
					"status": "success",
					"created_at": "2024-01-01T09:00:00Z",
					"finished_at": "2024-01-01T09:05:00Z"
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
	HandleWorkflowAttemptList(context.Background(), client, []string{"123"}, flags)

	// Restore stdout and read output
	w.Close()
	os.Stdout = oldStdout
	output, _ := io.ReadAll(r)
	outputStr := string(output)

	// Verify JSON output contains expected fields
	expectedFields := []string{
		`"attempts"`,
		`"id": "456"`,
		`"index": 1`,
		`"status": "success"`,
	}

	for _, expected := range expectedFields {
		if !strings.Contains(outputStr, expected) {
			t.Errorf("Expected JSON output to contain %q, but got:\n%s", expected, outputStr)
		}
	}
}
