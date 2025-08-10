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

func TestHandleWorkflowAttemptLog(t *testing.T) {
	tests := []struct {
		name           string
		args           []string
		mockResponse   string
		expectedOutput []string
	}{
		{
			name: "Valid attempt log",
			args: []string{"123", "456"},
			mockResponse: `2024-01-01 09:00:00 +0000 Digdag Executor: Starting workflow execution
2024-01-01 09:00:10 +0000 Digdag Executor: Task setup completed successfully  
2024-01-01 09:00:20 +0000 Digdag Executor: Query execution failed`,
			expectedOutput: []string{
				"2024-01-01 09:00:00", "Starting workflow execution",
				"2024-01-01 09:00:10", "Task setup completed successfully",
				"2024-01-01 09:00:20", "Query execution failed",
			},
		},
		{
			name:         "Empty log",
			args:         []string{"123", "456"},
			mockResponse: ``,
			expectedOutput: []string{
				"", // Empty log will just output nothing
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, mux, teardown := setupWorkflowTest()
			defer teardown()

			mux.HandleFunc("/api/workflows/"+tt.args[0]+"/attempts/"+tt.args[1]+"/log", func(w http.ResponseWriter, r *http.Request) {
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
			flags := Flags{Format: "table"}
			HandleWorkflowAttemptLog(context.Background(), client, tt.args, flags)

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

func TestHandleWorkflowTaskLog(t *testing.T) {
	tests := []struct {
		name           string
		args           []string
		mockResponse   string
		expectedOutput []string
	}{
		{
			name: "Valid task log",
			args: []string{"123", "456", "task1"},
			mockResponse: `2024-01-01 09:00:00 +0000 Digdag Executor: Task started
2024-01-01 09:00:30 +0000 Digdag Executor: Processing data...
2024-01-01 09:01:00 +0000 Digdag Executor: Task completed successfully`,
			expectedOutput: []string{
				"2024-01-01 09:00:00", "Task started",
				"2024-01-01 09:00:30", "Processing data...",
				"2024-01-01 09:01:00", "Task completed successfully",
			},
		},
		{
			name: "Task with warnings and errors",
			args: []string{"123", "456", "task2"},
			mockResponse: `2024-01-01 09:02:00 +0000 Digdag Executor: Deprecated function used
2024-01-01 09:02:10 +0000 Digdag Executor: Connection timeout`,
			expectedOutput: []string{
				"2024-01-01 09:02:00", "Deprecated function used",
				"2024-01-01 09:02:10", "Connection timeout",
			},
		},
		{
			name:         "Empty task log",
			args:         []string{"123", "456", "task3"},
			mockResponse: ``,
			expectedOutput: []string{
				"", // Empty log will just output nothing
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, mux, teardown := setupWorkflowTest()
			defer teardown()

			mux.HandleFunc("/api/workflows/"+tt.args[0]+"/attempts/"+tt.args[1]+"/tasks/"+tt.args[2]+"/log", func(w http.ResponseWriter, r *http.Request) {
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
			flags := Flags{Format: "table"}
			HandleWorkflowTaskLog(context.Background(), client, tt.args, flags)

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

func TestHandleWorkflowAttemptLogWithContent(t *testing.T) {
	client, mux, teardown := setupWorkflowTest()
	defer teardown()

	expectedLog := "2024-01-01 09:00:00 +0000 Digdag Executor: Starting workflow execution\n2024-01-01 09:00:01 +0000 Digdag Executor: Workflow completed successfully"

	mux.HandleFunc("/api/workflows/123/attempts/456/log", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, expectedLog)
	})

	// Capture stdout
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	// Call the function
	flags := Flags{Format: "table"}
	HandleWorkflowAttemptLog(context.Background(), client, []string{"123", "456"}, flags)

	// Restore stdout and read output
	w.Close()
	os.Stdout = oldStdout
	output, _ := io.ReadAll(r)
	outputStr := string(output)

	// Verify the log content is output correctly
	if outputStr != expectedLog {
		t.Errorf("Expected log output %q, but got: %q", expectedLog, outputStr)
	}
}

func TestHandleWorkflowTaskLogWithContent(t *testing.T) {
	client, mux, teardown := setupWorkflowTest()
	defer teardown()

	expectedLog := "2024-01-01 09:00:00 +0000 Digdag Executor: Task started\n2024-01-01 09:00:01 +0000 Digdag Executor: Task completed"

	mux.HandleFunc("/api/workflows/123/attempts/456/tasks/task1/log", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, expectedLog)
	})

	// Capture stdout
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	// Call the function
	flags := Flags{Format: "table"}
	HandleWorkflowTaskLog(context.Background(), client, []string{"123", "456", "task1"}, flags)

	// Restore stdout and read output
	w.Close()
	os.Stdout = oldStdout
	output, _ := io.ReadAll(r)
	outputStr := string(output)

	// Verify the log content is output correctly
	if outputStr != expectedLog {
		t.Errorf("Expected log output %q, but got: %q", expectedLog, outputStr)
	}
}
