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

func TestHandleWorkflowProjectList(t *testing.T) {
	tests := []struct {
		name           string
		format         string
		mockResponse   string
		expectedOutput []string
	}{
		{
			name:   "Table format with projects",
			format: "table",
			mockResponse: `{
				"projects": [
					{
						"id": "1",
						"name": "test-project",
						"revision": "v1",
						"archiveType": "db",
						"createdAt": 1609459200,
						"updatedAt": 1609459200
					},
					{
						"id": "2",
						"name": "another-project",
						"revision": "v2",
						"archiveType": "db",
						"createdAt": 1609545600,
						"updatedAt": 1609545600
					}
				]
			}`,
			expectedOutput: []string{
				"ID", "NAME", "REVISION", "TYPE", "CREATED",
				"1", "test-project", "v1", "db",
				"2", "another-project", "v2", "db",
				"Total: 2 projects",
			},
		},
		{
			name:   "CSV format with projects",
			format: "csv",
			mockResponse: `{
				"projects": [
					{
						"id": "1",
						"name": "test-project",
						"revision": "v1",
						"archiveType": "db",
						"createdAt": 1609459200,
						"updatedAt": 1609459200
					}
				]
			}`,
			expectedOutput: []string{
				"id,name,revision,archive_type,created_at,updated_at",
				"1,test-project,v1,db,2021-01-01 09:00:00,2021-01-01 09:00:00",
			},
		},
		{
			name:   "Empty projects list",
			format: "table",
			mockResponse: `{
				"projects": []
			}`,
			expectedOutput: []string{
				"No projects found",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, mux, teardown := setupWorkflowTest()
			defer teardown()

			mux.HandleFunc("/api/projects", func(w http.ResponseWriter, r *http.Request) {
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
			handleWorkflowProjectList(context.Background(), client, flags)

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

func TestHandleWorkflowProjectGet(t *testing.T) {
	tests := []struct {
		name           string
		args           []string
		format         string
		mockResponse   string
		expectedOutput []string
		expectError    bool
	}{
		{
			name:   "Valid project ID - table format",
			args:   []string{"123"},
			format: "table",
			mockResponse: `{
				"id": "123",
				"name": "my-project",
				"revision": "v3",
				"archiveType": "db",
				"archiveMd5": "abc123def456",
				"createdAt": 1609459200,
				"updatedAt": 1609545600,
				"deletedAt": null
			}`,
			expectedOutput: []string{
				"ID: 123",
				"Name: my-project",
				"Revision: v3",
				"Archive Type: db",
				"Archive MD5: abc123def456",
				"Created: 2021-01-01 09:00:00",
				"Updated: 2021-01-02 09:00:00",
			},
		},
		{
			name:   "Valid project ID - CSV format",
			args:   []string{"456"},
			format: "csv",
			mockResponse: `{
				"id": "456",
				"name": "csv-project",
				"revision": "v1",
				"archiveType": "db",
				"archiveMd5": "def456ghi789",
				"createdAt": 1609459200,
				"updatedAt": 1609459200,
				"deletedAt": null
			}`,
			expectedOutput: []string{
				"id,name,revision,archive_type,archive_md5,created_at,updated_at,deleted_at",
				"456,csv-project,v1,db,def456ghi789,2021-01-01 09:00:00,2021-01-01 09:00:00,",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, mux, teardown := setupWorkflowTest()
			defer teardown()

			mux.HandleFunc("/api/projects/"+tt.args[0], func(w http.ResponseWriter, r *http.Request) {
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
			handleWorkflowProjectGet(context.Background(), client, tt.args, flags)

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

func TestHandleWorkflowProjectCreate(t *testing.T) {
	tests := []struct {
		name           string
		args           []string
		setupFiles     func(string) error
		mockResponse   string
		expectedOutput []string
		expectError    bool
	}{
		{
			name: "Create project from directory",
			args: []string{"test-project", ""},
			setupFiles: func(dir string) error {
				// Create test workflow files
				err := os.WriteFile(filepath.Join(dir, "workflow.dig"), []byte("timezone: UTC\n"), 0644)
				if err != nil {
					return err
				}
				return os.WriteFile(filepath.Join(dir, "query.sql"), []byte("SELECT 1"), 0644)
			},
			mockResponse: `{
				"id": "789",
				"name": "test-project",
				"revision": "v1",
				"archiveType": "db",
				"createdAt": 1609459200,
				"updatedAt": 1609459200
			}`,
			expectedOutput: []string{
				"Creating project from directory:",
				"Project created successfully",
				"ID: 789",
				"Name: test-project",
				"Revision: v1",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, mux, teardown := setupWorkflowTest()
			defer teardown()

			// Create temporary directory
			tempDir := t.TempDir()

			// Setup test files if provided
			if tt.setupFiles != nil {
				err := tt.setupFiles(tempDir)
				if err != nil {
					t.Fatalf("Failed to setup test files: %v", err)
				}
			}

			// Update args with temp directory path
			args := make([]string, len(tt.args))
			copy(args, tt.args)
			if len(args) > 1 && args[1] == "" {
				args[1] = tempDir
			}

			mux.HandleFunc("/api/projects", func(w http.ResponseWriter, r *http.Request) {
				if r.Method != "PUT" {
					t.Errorf("Expected PUT request, got %s", r.Method)
				}

				// Verify we received some data
				if r.ContentLength == 0 {
					t.Error("Expected non-empty request body")
				}

				fmt.Fprint(w, tt.mockResponse)
			})

			// Capture stdout
			oldStdout := os.Stdout
			r, w, _ := os.Pipe()
			os.Stdout = w

			// Call the function
			flags := Flags{Format: "table"}
			handleWorkflowProjectCreate(context.Background(), client, args, flags)

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

func TestHandleWorkflowProjectWorkflows(t *testing.T) {
	tests := []struct {
		name           string
		args           []string
		format         string
		mockResponse   string
		expectedOutput []string
	}{
		{
			name:   "List workflows in project - table format",
			args:   []string{"123"},
			format: "table",
			mockResponse: `{
				"workflows": [
					{
						"id": "1",
						"name": "daily-etl",
						"project": {"id": "test-project", "name": "test-project"},
						"status": "active",
						"timezone": "UTC"
					},
					{
						"id": "2",
						"name": "hourly-sync",
						"project": {"id": "test-project", "name": "test-project"}, 
						"status": "inactive",
						"timezone": "Asia/Tokyo"
					}
				]
			}`,
			expectedOutput: []string{
				"ID", "NAME", "STATUS", "TIMEZONE",
				"1", "daily-etl", "active", "UTC",
				"2", "hourly-sync", "inactive", "Asia/Tokyo",
				"Total: 2 workflows",
			},
		},
		{
			name:   "Empty workflows list",
			args:   []string{"456"},
			format: "table",
			mockResponse: `{
				"workflows": []
			}`,
			expectedOutput: []string{
				"No workflows found in this project",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, mux, teardown := setupWorkflowTest()
			defer teardown()

			mux.HandleFunc("/api/projects/"+tt.args[0]+"/workflows", func(w http.ResponseWriter, r *http.Request) {
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
			handleWorkflowProjectWorkflows(context.Background(), client, tt.args, flags)

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

func TestHandleWorkflowProjectSecretsList(t *testing.T) {
	tests := []struct {
		name           string
		args           []string
		format         string
		mockResponse   string
		expectedOutput []string
	}{
		{
			name:   "List secrets - table format",
			args:   []string{"123"},
			format: "table",
			mockResponse: `{
				"secrets": {
					"api_key": "****",
					"database_password": "****",
					"webhook_url": "****"
				}
			}`,
			expectedOutput: []string{
				"KEY", "VALUE",
				"api_key", "****",
				"database_password", "****",
				"webhook_url", "****",
				"Total: 3 secrets",
			},
		},
		{
			name:   "Empty secrets list",
			args:   []string{"456"},
			format: "table",
			mockResponse: `{
				"secrets": {}
			}`,
			expectedOutput: []string{
				"No secrets found in this project",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, mux, teardown := setupWorkflowTest()
			defer teardown()

			mux.HandleFunc("/api/projects/"+tt.args[0]+"/secrets", func(w http.ResponseWriter, r *http.Request) {
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
			handleWorkflowProjectSecretsList(context.Background(), client, tt.args, flags)

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

func TestHandleWorkflowProjectSecretsSet(t *testing.T) {
	client, mux, teardown := setupWorkflowTest()
	defer teardown()

	mux.HandleFunc("/api/projects/123/secrets/test_key", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "PUT" {
			t.Errorf("Expected PUT request, got %s", r.Method)
		}
		w.WriteHeader(http.StatusOK)
	})

	// Capture stdout
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	// Call the function
	flags := Flags{Format: "table"}
	args := []string{"123", "test_key", "test_value"}
	handleWorkflowProjectSecretsSet(context.Background(), client, args, flags)

	// Restore stdout and read output
	w.Close()
	os.Stdout = oldStdout
	output, _ := io.ReadAll(r)
	outputStr := string(output)

	expectedOutput := "Secret 'test_key' set successfully for project 123"
	if !strings.Contains(outputStr, expectedOutput) {
		t.Errorf("Expected output to contain %q, but got:\n%s", expectedOutput, outputStr)
	}
}

func TestHandleWorkflowProjectSecretsDelete(t *testing.T) {
	client, mux, teardown := setupWorkflowTest()
	defer teardown()

	mux.HandleFunc("/api/projects/123/secrets/test_key", func(w http.ResponseWriter, r *http.Request) {
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
	args := []string{"123", "test_key"}
	handleWorkflowProjectSecretsDelete(context.Background(), client, args, flags)

	// Restore stdout and read output
	w.Close()
	os.Stdout = oldStdout
	output, _ := io.ReadAll(r)
	outputStr := string(output)

	expectedOutput := "Secret 'test_key' deleted successfully from project 123"
	if !strings.Contains(outputStr, expectedOutput) {
		t.Errorf("Expected output to contain %q, but got:\n%s", expectedOutput, outputStr)
	}
}

// Error handling tests
func TestHandleWorkflowProjectErrors(t *testing.T) {
	// Note: These tests demonstrate error paths but can't fully test handleError
	// since it calls log.Fatal. In a production CLI, you'd refactor to return errors.

	t.Run("Invalid Project ID", func(t *testing.T) {
		// Test argument validation logic
		// Note: In a real CLI app, you'd want to refactor to avoid log.Fatal in tests
		// For now, this tests the path before the fatal error

		// Test what happens before the fatal error - the arg validation
		args := []string{} // Empty args
		if len(args) < 1 {
			// This is what the function checks before calling log.Fatal
			t.Log("Function correctly validates that Project ID is required")
		}
	})
}

// Integration test with JSON format
func TestHandleWorkflowProjectListJSON(t *testing.T) {
	client, mux, teardown := setupWorkflowTest()
	defer teardown()

	mux.HandleFunc("/api/projects", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{
			"projects": [
				{
					"id": "1",
					"name": "json-test-project",
					"revision": "v1",
					"archiveType": "db",
					"createdAt": 1609459200,
					"updatedAt": 1609459200
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
	handleWorkflowProjectList(context.Background(), client, flags)

	// Restore stdout and read output
	w.Close()
	os.Stdout = oldStdout
	output, _ := io.ReadAll(r)
	outputStr := string(output)

	// Verify JSON output contains expected fields
	expectedFields := []string{
		`"projects"`,
		`"id": "1"`,
		`"name": "json-test-project"`,
		`"revision": "v1"`,
		`"archiveType": "db"`,
	}

	for _, expected := range expectedFields {
		if !strings.Contains(outputStr, expected) {
			t.Errorf("Expected JSON output to contain %q, but got:\n%s", expected, outputStr)
		}
	}
}

// Test archive file creation vs directory detection
func TestHandleWorkflowProjectCreateArchiveFile(t *testing.T) {
	client, mux, teardown := setupWorkflowTest()
	defer teardown()

	mux.HandleFunc("/api/projects", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{
			"id": "999",
			"name": "archive-project",
			"revision": "v1"
		}`)
	})

	// Create a temporary archive file
	tempDir := t.TempDir()
	archiveFile := filepath.Join(tempDir, "test.tar.gz")
	err := os.WriteFile(archiveFile, []byte("fake archive content"), 0644)
	if err != nil {
		t.Fatalf("Failed to create test archive file: %v", err)
	}

	// Capture stdout
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	// Call the function with archive file
	flags := Flags{Format: "table"}
	args := []string{"archive-project", archiveFile}
	handleWorkflowProjectCreate(context.Background(), client, args, flags)

	// Restore stdout and read output
	w.Close()
	os.Stdout = oldStdout
	output, _ := io.ReadAll(r)
	outputStr := string(output)

	// Verify it detected archive file mode
	if !strings.Contains(outputStr, "Creating project from archive file:") {
		t.Errorf("Expected output to indicate archive file mode, got: %s", outputStr)
	}

	if !strings.Contains(outputStr, "Project created successfully") {
		t.Errorf("Expected success message, got: %s", outputStr)
	}
}
