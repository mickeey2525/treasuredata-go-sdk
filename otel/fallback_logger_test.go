package otel

import (
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"time"
)

func TestFallbackLogger(t *testing.T) {
	logger := NewFallbackLogger()
	defer logger.Close()

	if !logger.IsEnabled() {
		t.Skip("Fallback logger not enabled, skipping test")
	}

	// Test logging export failure
	testErr := errors.New("test export failure")
	logger.LogExportFailure("test_operation", testErr, 1)

	if logger.GetLogCount() != 1 {
		t.Errorf("Expected log count to be 1, got %d", logger.GetLogCount())
	}

	// Test logging recovery
	logger.LogRecovery("test_operation", 3)

	if logger.GetLogCount() != 2 {
		t.Errorf("Expected log count to be 2, got %d", logger.GetLogCount())
	}

	// Verify log file exists and has content
	logPath := logger.GetLogPath()
	if logPath == "" {
		t.Error("Expected log path to be set")
	}

	// Read log file content
	content, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("Failed to read log file: %v", err)
	}

	lines := strings.Split(strings.TrimSpace(string(content)), "\n")
	if len(lines) != 2 {
		t.Errorf("Expected 2 log lines, got %d", len(lines))
	}

	// Parse first log entry
	var entry FallbackLogEntry
	if err := json.Unmarshal([]byte(lines[0]), &entry); err != nil {
		t.Fatalf("Failed to parse log entry: %v", err)
	}

	if entry.Operation != "test_operation" {
		t.Errorf("Expected operation to be 'test_operation', got %s", entry.Operation)
	}

	if entry.Error != "test export failure" {
		t.Errorf("Expected error to be 'test export failure', got %s", entry.Error)
	}

	if entry.Failures != 1 {
		t.Errorf("Expected failures to be 1, got %d", entry.Failures)
	}

	if entry.ServiceName != "tdcli" {
		t.Errorf("Expected service name to be 'tdcli', got %s", entry.ServiceName)
	}

	// Parse second log entry (recovery)
	if err := json.Unmarshal([]byte(lines[1]), &entry); err != nil {
		t.Fatalf("Failed to parse recovery log entry: %v", err)
	}

	if !strings.Contains(entry.Error, "RECOVERED") {
		t.Errorf("Expected recovery message, got %s", entry.Error)
	}

	if entry.Failures != 0 {
		t.Errorf("Expected failures to be 0 for recovery, got %d", entry.Failures)
	}
}

func TestFallbackLoggerReset(t *testing.T) {
	logger := NewFallbackLogger()
	defer logger.Close()

	if !logger.IsEnabled() {
		t.Skip("Fallback logger not enabled, skipping test")
	}

	// Log some entries
	testErr := errors.New("test error")
	logger.LogExportFailure("test_op", testErr, 1)
	logger.LogExportFailure("test_op", testErr, 2)

	if logger.GetLogCount() != 2 {
		t.Errorf("Expected log count to be 2, got %d", logger.GetLogCount())
	}

	originalPath := logger.GetLogPath()

	// Reset logger
	logger.Reset()

	if logger.GetLogCount() != 0 {
		t.Errorf("Expected log count to be 0 after reset, got %d", logger.GetLogCount())
	}

	newPath := logger.GetLogPath()
	if newPath == originalPath {
		t.Error("Expected new log path after reset")
	}

	// Verify new log file is created
	logger.LogExportFailure("test_op", testErr, 1)

	if logger.GetLogCount() != 1 {
		t.Errorf("Expected log count to be 1 after reset and new log, got %d", logger.GetLogCount())
	}
}

func TestFallbackLoggerClose(t *testing.T) {
	logger := NewFallbackLogger()

	if !logger.IsEnabled() {
		t.Skip("Fallback logger not enabled, skipping test")
	}

	// Log an entry
	testErr := errors.New("test error")
	logger.LogExportFailure("test_op", testErr, 1)

	// Close logger
	err := logger.Close()
	if err != nil {
		t.Errorf("Expected no error on close, got %v", err)
	}

	// Verify logging after close doesn't crash
	logger.LogExportFailure("test_op", testErr, 2)

	// Second close should not error
	err = logger.Close()
	if err != nil {
		t.Errorf("Expected no error on second close, got %v", err)
	}
}

func TestFallbackLogEntry(t *testing.T) {
	entry := FallbackLogEntry{
		Timestamp:   time.Now(),
		Operation:   "test_operation",
		Error:       "test error message",
		Failures:    5,
		ServiceName: "test_service",
	}

	// Test JSON marshaling
	data, err := json.Marshal(entry)
	if err != nil {
		t.Fatalf("Failed to marshal log entry: %v", err)
	}

	// Test JSON unmarshaling
	var unmarshaled FallbackLogEntry
	if err := json.Unmarshal(data, &unmarshaled); err != nil {
		t.Fatalf("Failed to unmarshal log entry: %v", err)
	}

	if unmarshaled.Operation != entry.Operation {
		t.Errorf("Expected operation %s, got %s", entry.Operation, unmarshaled.Operation)
	}

	if unmarshaled.Error != entry.Error {
		t.Errorf("Expected error %s, got %s", entry.Error, unmarshaled.Error)
	}

	if unmarshaled.Failures != entry.Failures {
		t.Errorf("Expected failures %d, got %d", entry.Failures, unmarshaled.Failures)
	}

	if unmarshaled.ServiceName != entry.ServiceName {
		t.Errorf("Expected service name %s, got %s", entry.ServiceName, unmarshaled.ServiceName)
	}
}
