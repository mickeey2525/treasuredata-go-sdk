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

func TestHandleWorkflowScheduleGet(t *testing.T) {
	tests := []struct {
		name           string
		args           []string
		format         string
		mockResponse   string
		expectedOutput []string
	}{
		{
			name:   "Valid schedule - table format",
			args:   []string{"123"},
			format: "table",
			mockResponse: `{
				"id": "123",
				"workflow_id": "123",
				"cron": "0 9 * * *",
				"timezone": "UTC",
				"delay": 0,
				"next_time": "2024-01-02T09:00:00Z",
				"disabled_at": null
			}`,
			expectedOutput: []string{
				"ID: 123",
				"Workflow ID: 123",
				"Cron: 0 9 * * *",
				"Timezone: UTC",
				"Delay: 0 seconds",
				"Status: Enabled",
				"Next Time: 2024-01-02 09:00:00",
			},
		},
		{
			name:   "Valid schedule - CSV format",
			args:   []string{"123"},
			format: "csv",
			mockResponse: `{
				"id": "123",
				"workflow_id": "123",
				"cron": "0 9 * * *",
				"timezone": "UTC",
				"delay": 0,
				"next_time": "2024-01-02T09:00:00Z",
				"disabled_at": null
			}`,
			expectedOutput: []string{
				"id,workflow_id,cron,timezone,delay,next_time,disabled_at",
				"123,123,0 9 * * *,UTC,0,2024-01-02 09:00:00,",
			},
		},
		{
			name:   "Disabled schedule",
			args:   []string{"123"},
			format: "table",
			mockResponse: `{
				"id": "123",
				"workflow_id": "123",
				"cron": "0 9 * * *",
				"timezone": "UTC",
				"delay": 0,
				"next_time": null,
				"disabled_at": "2024-01-01T10:00:00Z"
			}`,
			expectedOutput: []string{
				"Disabled At: 2024-01-01 10:00:00",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, mux, teardown := setupWorkflowTest()
			defer teardown()

			mux.HandleFunc("/api/workflows/"+tt.args[0]+"/schedule", func(w http.ResponseWriter, r *http.Request) {
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
			HandleWorkflowScheduleGet(context.Background(), client, tt.args, flags)

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

func TestHandleWorkflowScheduleEnable(t *testing.T) {
	client, mux, teardown := setupWorkflowTest()
	defer teardown()

	mux.HandleFunc("/api/workflows/123/schedule/enable", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			t.Errorf("Expected POST request, got %s", r.Method)
		}
		fmt.Fprint(w, `{
			"id": "123",
			"enabled": true
		}`)
	})

	// Capture stdout
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	// Call the function
	flags := Flags{Format: "table"}
	args := []string{"123"}
	HandleWorkflowScheduleEnable(context.Background(), client, args, flags)

	// Restore stdout and read output
	w.Close()
	os.Stdout = oldStdout
	output, _ := io.ReadAll(r)
	outputStr := string(output)

	expectedOutput := "Workflow schedule enabled successfully"
	if !strings.Contains(outputStr, expectedOutput) {
		t.Errorf("Expected output to contain %q, but got:\n%s", expectedOutput, outputStr)
	}
}

func TestHandleWorkflowScheduleDisable(t *testing.T) {
	client, mux, teardown := setupWorkflowTest()
	defer teardown()

	mux.HandleFunc("/api/workflows/123/schedule/disable", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			t.Errorf("Expected POST request, got %s", r.Method)
		}
		fmt.Fprint(w, `{
			"id": "123",
			"enabled": false
		}`)
	})

	// Capture stdout
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	// Call the function
	flags := Flags{Format: "table"}
	args := []string{"123"}
	HandleWorkflowScheduleDisable(context.Background(), client, args, flags)

	// Restore stdout and read output
	w.Close()
	os.Stdout = oldStdout
	output, _ := io.ReadAll(r)
	outputStr := string(output)

	expectedOutput := "Workflow schedule disabled successfully"
	if !strings.Contains(outputStr, expectedOutput) {
		t.Errorf("Expected output to contain %q, but got:\n%s", expectedOutput, outputStr)
	}
}

func TestHandleWorkflowScheduleUpdate(t *testing.T) {
	client, mux, teardown := setupWorkflowTest()
	defer teardown()

	mux.HandleFunc("/api/workflows/123/schedule", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "PUT" {
			t.Errorf("Expected PUT request, got %s", r.Method)
		}
		fmt.Fprint(w, `{
			"id": "123",
			"cron": "0 10 * * *",
			"timezone": "America/New_York",
			"delay": 60
		}`)
	})

	// Capture stdout
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	// Call the function
	flags := Flags{Format: "table"}
	args := []string{"123", "0 10 * * *", "America/New_York", "60"}
	HandleWorkflowScheduleUpdate(context.Background(), client, args, flags)

	// Restore stdout and read output
	w.Close()
	os.Stdout = oldStdout
	output, _ := io.ReadAll(r)
	outputStr := string(output)

	expectedOutputs := []string{
		"Workflow schedule updated successfully",
		"Cron: 0 10 * * *",
		"Timezone: America/New_York",
		"Delay: 60 seconds",
	}

	for _, expected := range expectedOutputs {
		if !strings.Contains(outputStr, expected) {
			t.Errorf("Expected output to contain %q, but got:\n%s", expected, outputStr)
		}
	}
}

func TestHandleWorkflowScheduleGetJSON(t *testing.T) {
	client, mux, teardown := setupWorkflowTest()
	defer teardown()

	mux.HandleFunc("/api/workflows/123/schedule", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{
			"id": "123",
			"workflow_id": "123",
			"cron": "0 9 * * *",
			"timezone": "UTC",
			"enabled": true
		}`)
	})

	// Capture stdout
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	// Call the function with JSON format
	flags := Flags{Format: "json"}
	HandleWorkflowScheduleGet(context.Background(), client, []string{"123"}, flags)

	// Restore stdout and read output
	w.Close()
	os.Stdout = oldStdout
	output, _ := io.ReadAll(r)
	outputStr := string(output)

	// Verify JSON output contains expected fields
	expectedFields := []string{
		`"id": "123"`,
		`"workflow_id": "123"`,
		`"cron": "0 9 * * *"`,
		`"timezone": "UTC"`,
	}

	for _, expected := range expectedFields {
		if !strings.Contains(outputStr, expected) {
			t.Errorf("Expected JSON output to contain %q, but got:\n%s", expected, outputStr)
		}
	}
}
