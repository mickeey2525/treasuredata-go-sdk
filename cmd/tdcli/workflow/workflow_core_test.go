package workflow

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestHandleWorkflowList(t *testing.T) {
	tests := []struct {
		name           string
		format         string
		mockResponse   string
		expectedOutput []string
	}{
		{
			name:   "Table format with workflows",
			format: "table",
			mockResponse: `{
				"workflows": [
					{
						"id": "1",
						"name": "daily-etl",
						"project": {"id": "proj1", "name": "test-project"},
						"status": "active",
						"timezone": "UTC",
						"created_at": "2024-01-01T09:00:00Z",
						"updated_at": "2024-01-01T09:00:00Z"
					},
					{
						"id": "2", 
						"name": "hourly-sync",
						"project": {"id": "proj1", "name": "test-project"},
						"status": "inactive",
						"timezone": "Asia/Tokyo",
						"created_at": "2024-01-01T10:00:00Z",
						"updated_at": "2024-01-01T10:00:00Z"
					}
				]
			}`,
			expectedOutput: []string{
				"ID", "NAME", "PROJECT", "STATUS", "TIMEZONE",
				"1", "daily-etl", "test-project", "active", "UTC",
				"2", "hourly-sync", "test-project", "inactive", "Asia/Tokyo",
				"Total: 2 workflows",
			},
		},
		{
			name:   "CSV format with workflows",
			format: "csv",
			mockResponse: `{
				"workflows": [
					{
						"id": "1",
						"name": "daily-etl",
						"project": {"id": "proj1", "name": "test-project"},
						"status": "active",
						"timezone": "UTC"
					}
				]
			}`,
			expectedOutput: []string{
				"id,name,project,status,created_at,updated_at",
				"1,daily-etl,test-project,active,,",
			},
		},
		{
			name:   "Empty workflows list",
			format: "table",
			mockResponse: `{
				"workflows": []
			}`,
			expectedOutput: []string{
				"No workflows found",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, mux, teardown := setupWorkflowTest()
			defer teardown()

			mux.HandleFunc("/api/workflows", func(w http.ResponseWriter, r *http.Request) {
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
			HandleWorkflowList(context.Background(), client, flags)

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

func TestHandleWorkflowGet(t *testing.T) {
	tests := []struct {
		name           string
		args           []string
		format         string
		mockResponse   string
		expectedOutput []string
	}{
		{
			name:   "Valid workflow - table format",
			args:   []string{"123"},
			format: "table",
			mockResponse: `{
				"id": "123",
				"name": "test-workflow",
				"project": {"id": "proj1", "name": "test-project"},
				"status": "active",
				"revision": "v1.0",
				"timezone": "UTC",
				"last_attempt": 5,
				"next_schedule": "2024-01-02T09:00:00Z",
				"config": {"key": "value"}
			}`,
			expectedOutput: []string{
				"ID: 123",
				"Name: test-workflow",
				"Project: test-project (proj1)",
				"Status: active",
				"Revision: v1.0",
				"Timezone: UTC",
				"Last Attempt: 5",
				"Next Schedule: 2024-01-02 09:00:00",
				"Config:",
			},
		},
		{
			name:   "Valid workflow - CSV format",
			args:   []string{"123"},
			format: "csv",
			mockResponse: `{
				"id": "123",
				"name": "test-workflow",
				"project": {"id": "proj1", "name": "test-project"},
				"status": "active",
				"revision": "v1.0",
				"timezone": "UTC"
			}`,
			expectedOutput: []string{
				"id,name,project,status,revision,timezone,created_at,updated_at",
				"123,test-workflow,test-project,active,v1.0,UTC,,",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, mux, teardown := setupWorkflowTest()
			defer teardown()

			mux.HandleFunc("/api/workflows/"+tt.args[0], func(w http.ResponseWriter, r *http.Request) {
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
			HandleWorkflowGet(context.Background(), client, tt.args, flags)

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

func TestHandleWorkflowInit(t *testing.T) {
	// Create a temporary directory to run the test in
	tempDir := t.TempDir()
	// Change to the temporary directory
	oldWd, _ := os.Getwd()
	os.Chdir(tempDir)
	defer os.Chdir(oldWd)

	projectName := "my-new-workflow"

	// Capture stdout
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	// Call the function
	flags := Flags{Format: "table"}
	HandleWorkflowInit(context.Background(), []string{projectName}, flags)

	// Restore stdout and read output
	w.Close()
	os.Stdout = oldStdout
	output, _ := io.ReadAll(r)
	outputStr := string(output)

	// Verify output message
	expectedMsg := fmt.Sprintf("Sample workflow project '%s' created successfully", projectName)
	if !strings.Contains(outputStr, expectedMsg) {
		t.Errorf("Expected output to contain %q, but got: %s", expectedMsg, outputStr)
	}

	// Verify directory and files were created
	// 1. Project directory
	if _, err := os.Stat(projectName); os.IsNotExist(err) {
		t.Errorf("Project directory '%s' was not created", projectName)
	}

	// 2. workflow.dig file
	digFilePath := filepath.Join(projectName, "workflow.dig")
	if _, err := os.Stat(digFilePath); os.IsNotExist(err) {
		t.Errorf("workflow.dig file was not created at %s", digFilePath)
	}

	// 3. queries subdirectory
	queriesDirPath := filepath.Join(projectName, "queries")
	if _, err := os.Stat(queriesDirPath); os.IsNotExist(err) {
		t.Errorf("queries subdirectory was not created at %s", queriesDirPath)
	}

	// 4. sample_query.sql file
	sqlFilePath := filepath.Join(queriesDirPath, "sample_query.sql")
	if _, err := os.Stat(sqlFilePath); os.IsNotExist(err) {
		t.Errorf("sample_query.sql file was not created at %s", sqlFilePath)
	}

	// 5. Verify content of sample_query.sql
	sqlContent, err := os.ReadFile(sqlFilePath)
	if err != nil {
		t.Fatalf("Failed to read sample_query.sql: %v", err)
	}
	expectedSQL := "SELECT count(1) FROM www_access;"
	if !strings.Contains(string(sqlContent), expectedSQL) {
		t.Errorf("Expected SQL content to contain %q, but got: %s", expectedSQL, string(sqlContent))
	}

	// 6. Verify content of workflow.dig
	digContent, err := os.ReadFile(digFilePath)
	if err != nil {
		t.Fatalf("Failed to read workflow.dig: %v", err)
	}
	expectedDigContent := []string{"timezone: UTC", "+setup:", "+query_and_export:", "td>: queries/sample_query.sql"}
	for _, expected := range expectedDigContent {
		if !strings.Contains(string(digContent), expected) {
			t.Errorf("Expected workflow.dig content to contain %q, but got: %s", expected, string(digContent))
		}
	}
}

func TestHandleWorkflowCreate(t *testing.T) {
	client, mux, teardown := setupWorkflowTest()
	defer teardown()

	mux.HandleFunc("/api/workflows", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			t.Errorf("Expected POST request, got %s", r.Method)
		}
		fmt.Fprint(w, `{
			"id": "456",
			"name": "test-workflow"
		}`)
	})

	// Capture stdout
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	// Call the function
	flags := Flags{Format: "table"}
	args := []string{"test-workflow", "test-project", "timezone: UTC"}
	HandleWorkflowCreate(context.Background(), client, args, flags)

	// Restore stdout and read output
	w.Close()
	os.Stdout = oldStdout
	output, _ := io.ReadAll(r)
	outputStr := string(output)

	expectedOutputs := []string{
		"Workflow created successfully",
		"ID: 456",
		"Name: test-workflow",
	}

	for _, expected := range expectedOutputs {
		if !strings.Contains(outputStr, expected) {
			t.Errorf("Expected output to contain %q, but got:\n%s", expected, outputStr)
		}
	}
}

func TestHandleWorkflowUpdate(t *testing.T) {
	client, mux, teardown := setupWorkflowTest()
	defer teardown()

	mux.HandleFunc("/api/workflows/123", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "PUT" {
			t.Errorf("Expected PUT request, got %s", r.Method)
		}
		fmt.Fprint(w, `{
			"id": "123",
			"name": "updated-workflow"
		}`)
	})

	// Capture stdout
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	// Call the function
	flags := Flags{Format: "table"}
	args := []string{"123", "name=updated-workflow", "timezone=UTC"}
	HandleWorkflowUpdate(context.Background(), client, args, flags)

	// Restore stdout and read output
	w.Close()
	os.Stdout = oldStdout
	output, _ := io.ReadAll(r)
	outputStr := string(output)

	expectedOutput := "Workflow 123 updated successfully"
	if !strings.Contains(outputStr, expectedOutput) {
		t.Errorf("Expected output to contain %q, but got:\n%s", expectedOutput, outputStr)
	}
}

func TestHandleWorkflowDelete(t *testing.T) {
	client, mux, teardown := setupWorkflowTest()
	defer teardown()

	mux.HandleFunc("/api/workflows/123", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "DELETE" {
			t.Errorf("Expected DELETE request, got %s", r.Method)
		}
		w.WriteHeader(http.StatusNoContent)
	})

	// Capture stdout
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	// Call the function
	flags := Flags{Format: "table"}
	args := []string{"123"}
	HandleWorkflowDelete(context.Background(), client, args, flags)

	// Restore stdout and read output
	w.Close()
	os.Stdout = oldStdout
	output, _ := io.ReadAll(r)
	outputStr := string(output)

	expectedOutput := "Workflow 123 deleted successfully"
	if !strings.Contains(outputStr, expectedOutput) {
		t.Errorf("Expected output to contain %q, but got:\n%s", expectedOutput, outputStr)
	}
}

func TestHandleWorkflowStart(t *testing.T) {
	tests := []struct {
		name           string
		args           []string
		mockResponse   string
		expectedOutput []string
	}{
		{
			name: "Start without parameters",
			args: []string{"123"},
			mockResponse: `{
				"id": "456",
				"status": "pending"
			}`,
			expectedOutput: []string{
				"Workflow started successfully",
				"Attempt ID: 456",
				"Status: pending",
			},
		},
		{
			name: "Start with parameters",
			args: []string{"123", `{"param1": "value1"}`},
			mockResponse: `{
				"id": "456",
				"status": "pending"
			}`,
			expectedOutput: []string{
				"Workflow started successfully",
				"Attempt ID: 456",
				"Status: pending",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, mux, teardown := setupWorkflowTest()
			defer teardown()

			mux.HandleFunc("/api/workflows/123/attempts", func(w http.ResponseWriter, r *http.Request) {
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
			HandleWorkflowStart(context.Background(), client, tt.args, flags)

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

func TestHandleWorkflowListJSON(t *testing.T) {
	client, mux, teardown := setupWorkflowTest()
	defer teardown()

	mux.HandleFunc("/api/workflows", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{
			"workflows": [
				{
					"id": "1",
					"name": "test-workflow",
					"project": {"id": "proj1", "name": "test-project"},
					"status": "active",
					"created_at": "2024-01-01T09:00:00Z",
					"updated_at": "2024-01-01T09:00:00Z"
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
	HandleWorkflowList(context.Background(), client, flags)

	// Restore stdout and read output
	w.Close()
	os.Stdout = oldStdout
	output, _ := io.ReadAll(r)
	outputStr := string(output)

	// Verify JSON output contains expected fields
	expectedFields := []string{
		`"workflows"`,
		`"id": "1"`,
		`"name": "test-workflow"`,
		`"status": "active"`,
	}

	for _, expected := range expectedFields {
		if !strings.Contains(outputStr, expected) {
			t.Errorf("Expected JSON output to contain %q, but got:\n%s", expected, outputStr)
		}
	}
}
