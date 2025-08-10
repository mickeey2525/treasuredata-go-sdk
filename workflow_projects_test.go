package treasuredata

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestWorkflowService_ListProjects(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/api/projects", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "GET")
		testURL(t, r, "/api/projects")

		fmt.Fprint(w, `{
			"projects": [
				{
					"id": "1",
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
				ID:          "1",
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
			"id": "1",
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
	project, err := client.Workflow.GetProject(ctx, "1")
	if err != nil {
		t.Errorf("Workflow.GetProject returned error: %v", err)
	}

	want := &WorkflowProject{
		ID:          "1",
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

	// Calculate expected MD5 hash for test archive data
	archive := []byte("sample archive data")
	hash := md5.Sum(archive)
	expectedRevision := hex.EncodeToString(hash[:])

	mux.HandleFunc("/api/projects", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "PUT")
		testURL(t, r, fmt.Sprintf("/api/projects?project=new-project&revision=%s", expectedRevision))

		fmt.Fprint(w, `{
			"id": "2",
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
	project, err := client.Workflow.CreateProject(ctx, "new-project", archive)
	if err != nil {
		t.Errorf("Workflow.CreateProject returned error: %v", err)
	}

	want := &WorkflowProject{
		ID:          "2",
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

func TestWorkflowService_CreateProjectWithRevision(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/api/projects", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "PUT")
		testURL(t, r, "/api/projects?project=new-project&revision=v2.0")

		fmt.Fprint(w, `{
			"id": "2",
			"name": "new-project",
			"revision": "v2.0",
			"archiveType": "db",
			"archiveMd5": "new123archive456",
			"createdAt": 1609459200,
			"updatedAt": 1609459200,
			"deletedAt": null
		}`)
	})

	ctx := context.Background()
	archive := []byte("sample archive data")
	project, err := client.Workflow.CreateProjectWithRevision(ctx, "new-project", "v2.0", archive)
	if err != nil {
		t.Errorf("Workflow.CreateProjectWithRevision returned error: %v", err)
	}

	want := &WorkflowProject{
		ID:          "2",
		Name:        "new-project",
		Revision:    "v2.0",
		ArchiveType: "db",
		ArchiveMD5:  "new123archive456",
		CreatedAt:   TDTime{time.Unix(1609459200, 0)},
		UpdatedAt:   TDTime{time.Unix(1609459200, 0)},
		DeletedAt:   nil,
	}

	if !reflect.DeepEqual(project, want) {
		t.Errorf("Workflow.CreateProjectWithRevision returned %+v, want %+v", project, want)
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
					"id": "10",
					"name": "project-workflow",
					"project": {"id": "test-project", "name": "test-project"},
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
	resp, err := client.Workflow.ListProjectWorkflows(ctx, "1")
	if err != nil {
		t.Errorf("Workflow.ListProjectWorkflows returned error: %v", err)
	}

	want := &WorkflowListResponse{
		Workflows: []Workflow{
			{
				ID:        "10",
				Name:      "project-workflow",
				Project:   WorkflowProjectRef{ID: "test-project", Name: "test-project"},
				Revision:  "abc123",
				Status:    "active",
				Config:    map[string]interface{}{"timezone": "UTC"},
				CreatedAt: &TDTime{time.Unix(1609459200, 0)},
				UpdatedAt: &TDTime{time.Unix(1609459200, 0)},
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
	resp, err := client.Workflow.GetProjectSecrets(ctx, "1")
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
	err := client.Workflow.SetProjectSecret(ctx, "1", "test_secret", "secret_value")
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
	err := client.Workflow.DeleteProjectSecret(ctx, "1", "test_secret")
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
		fmt.Printf("Project: %s (ID: %s, Revision: %s)\n", project.Name, project.ID, project.Revision)
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

	fmt.Printf("Created project: %s (ID: %s)\n", project.Name, project.ID)
}

func ExampleWorkflowService_SetProjectSecret() {
	client, _ := NewClient("YOUR_API_KEY")
	ctx := context.Background()

	// Set a secret for a project
	err := client.Workflow.SetProjectSecret(ctx, "123", "database_password", "my_secret_password")
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
		// We can't predict exact hash due to tar timestamps, so just check prefix
		if !strings.HasPrefix(r.URL.String(), "/api/projects?project=test-project&revision=") {
			t.Errorf("Expected URL to start with /api/projects?project=test-project&revision=, got %s", r.URL.String())
		}

		// Verify we received some archive data
		if r.ContentLength == 0 {
			t.Error("Expected non-empty request body")
		}

		fmt.Fprint(w, `{
			"id": "3",
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
		ID:          "3",
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

func TestWorkflowService_CreateProjectFromDirectoryWithRevision(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/api/projects", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "PUT")
		testURL(t, r, "/api/projects?project=test-project&revision=custom-rev")

		// Verify we received some archive data
		if r.ContentLength == 0 {
			t.Error("Expected non-empty request body")
		}

		fmt.Fprint(w, `{
			"id": "3",
			"name": "test-project",
			"revision": "custom-rev",
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
	project, err := client.Workflow.CreateProjectFromDirectoryWithRevision(ctx, "test-project", "custom-rev", tempDir)
	if err != nil {
		t.Errorf("Workflow.CreateProjectFromDirectoryWithRevision returned error: %v", err)
	}

	want := &WorkflowProject{
		ID:          "3",
		Name:        "test-project",
		Revision:    "custom-rev",
		ArchiveType: "db",
		ArchiveMD5:  "directory123hash456",
		CreatedAt:   TDTime{time.Unix(1609459200, 0)},
		UpdatedAt:   TDTime{time.Unix(1609459200, 0)},
		DeletedAt:   nil,
	}

	if !reflect.DeepEqual(project, want) {
		t.Errorf("Workflow.CreateProjectFromDirectoryWithRevision returned %+v, want %+v", project, want)
	}
}

func TestWorkflowService_DownloadProjectToDirectory(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	// Create a dummy archive
	sourceDir := t.TempDir()
	os.WriteFile(filepath.Join(sourceDir, "test.txt"), []byte("hello"), 0644)
	archive, _ := createTarGz(sourceDir)

	mux.HandleFunc("/api/projects/1/archive", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "GET")
		w.Header().Set("Content-Type", "application/gzip")
		w.Write(archive)
	})

	destDir := t.TempDir()
	err := client.Workflow.DownloadProjectToDirectory(context.Background(), "1", destDir)
	if err != nil {
		t.Errorf("DownloadProjectToDirectory returned error: %v", err)
	}

	// Verify extracted content
	content, err := os.ReadFile(filepath.Join(destDir, "test.txt"))
	if err != nil {
		t.Fatalf("Failed to read extracted file: %v", err)
	}
	if string(content) != "hello" {
		t.Errorf("Extracted file content mismatch: got %q, want %q", string(content), "hello")
	}
}

func TestWorkflowService_GetProjectByName(t *testing.T) {
	client, mux, teardown := setup()
	defer teardown()

	mux.HandleFunc("/api/projects", func(w http.ResponseWriter, r *http.Request) {
		testMethod(t, r, "GET")
		if r.URL.Query().Get("name") == "test-project" {
			fmt.Fprint(w, `{
				"projects": [
					{
						"id": "1",
						"name": "test-project",
						"revision": "v1"
					}
				]
			}`)
		} else {
			fmt.Fprint(w, `{"projects": []}`)
		}
	})

	// Test found
	project, err := client.Workflow.GetProjectByName(context.Background(), "test-project")
	if err != nil {
		t.Errorf("GetProjectByName returned error: %v", err)
	}
	if project.ID != "1" {
		t.Errorf("GetProjectByName returned wrong project ID: got %s, want 1", project.ID)
	}

	// Test not found
	_, err = client.Workflow.GetProjectByName(context.Background(), "not-found")
	if err == nil {
		t.Errorf("GetProjectByName should have returned an error for not-found project")
	}
}
