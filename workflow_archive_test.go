package treasuredata

import (
	"os"
	"path/filepath"
	"testing"
)

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

func TestExtractTarGz(t *testing.T) {
	// 1. Create a source directory and some files
	sourceDir := t.TempDir()
	file1Content := "hello world"
	file2Content := "nested content"
	subDir := "subdir"
	err := os.WriteFile(filepath.Join(sourceDir, "file1.txt"), []byte(file1Content), 0644)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}
	err = os.Mkdir(filepath.Join(sourceDir, subDir), 0755)
	if err != nil {
		t.Fatalf("Failed to create subdirectory: %v", err)
	}
	err = os.WriteFile(filepath.Join(sourceDir, subDir, "file2.txt"), []byte(file2Content), 0644)
	if err != nil {
		t.Fatalf("Failed to create nested file: %v", err)
	}

	// 2. Create a tar.gz archive from the source directory
	archiveData, err := createTarGz(sourceDir)
	if err != nil {
		t.Fatalf("createTarGz failed: %v", err)
	}

	// 3. Create a destination directory for extraction
	destDir := t.TempDir()

	// 4. Extract the archive
	err = extractTarGz(archiveData, destDir)
	if err != nil {
		t.Fatalf("extractTarGz failed: %v", err)
	}

	// 5. Verify the extracted content
	// Check file1.txt
	extractedFile1Content, err := os.ReadFile(filepath.Join(destDir, "file1.txt"))
	if err != nil {
		t.Fatalf("Failed to read extracted file1.txt: %v", err)
	}
	if string(extractedFile1Content) != file1Content {
		t.Errorf("Extracted file1.txt content mismatch: got %q, want %q", string(extractedFile1Content), file1Content)
	}

	// Check subdir/file2.txt
	extractedFile2Content, err := os.ReadFile(filepath.Join(destDir, subDir, "file2.txt"))
	if err != nil {
		t.Fatalf("Failed to read extracted file2.txt: %v", err)
	}
	if string(extractedFile2Content) != file2Content {
		t.Errorf("Extracted file2.txt content mismatch: got %q, want %q", string(extractedFile2Content), file2Content)
	}
}
