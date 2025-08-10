package workflow

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestWorkflowInitCmd(t *testing.T) {
	// Create a temporary directory to run the test in
	tempDir := t.TempDir()
	// Change to the temporary directory
	oldWd, _ := os.Getwd()
	os.Chdir(tempDir)
	defer os.Chdir(oldWd)

	projectName := "my-new-workflow"
	cmd := &WorkflowInitCmd{
		ProjectName: projectName,
	}

	// Capture stdout
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	// Run the command
	err := cmd.Run(&CLIContext{})
	if err != nil {
		t.Fatalf("WorkflowInitCmd.Run() returned an error: %v", err)
	}

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
}
