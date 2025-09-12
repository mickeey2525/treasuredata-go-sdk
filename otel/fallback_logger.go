package otel

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// FallbackLogEntry represents a single fallback log entry
type FallbackLogEntry struct {
	Timestamp   time.Time `json:"timestamp"`
	Operation   string    `json:"operation"`
	Error       string    `json:"error"`
	Failures    int       `json:"consecutive_failures"`
	ServiceName string    `json:"service_name"`
}

// FallbackLogger handles logging when OTEL exports consistently fail
type FallbackLogger struct {
	logFile  *os.File
	logCount int
	mutex    sync.RWMutex
	enabled  bool
	logPath  string
}

// NewFallbackLogger creates a new fallback logger
func NewFallbackLogger() *FallbackLogger {
	logger := &FallbackLogger{
		enabled: true,
	}

	// Try to initialize log file
	if err := logger.initLogFile(); err != nil {
		log.Printf("Warning: Failed to initialize fallback logger: %v", err)
		logger.enabled = false
	}

	return logger
}

// initLogFile initializes the fallback log file
func (fl *FallbackLogger) initLogFile() error {
	// Create logs directory in temp or current directory
	logDir := filepath.Join(os.TempDir(), "tdcli-otel-fallback")
	if err := os.MkdirAll(logDir, 0755); err != nil {
		// Fallback to current directory
		logDir = "."
	}

	// Create log file with timestamp (include nanoseconds for uniqueness)
	timestamp := time.Now().Format("20060102-150405.000000000")
	fl.logPath = filepath.Join(logDir, fmt.Sprintf("otel-export-failures-%s.log", timestamp))

	file, err := os.OpenFile(fl.logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("failed to create fallback log file: %w", err)
	}

	fl.logFile = file
	log.Printf("Fallback logger initialized: %s", fl.logPath)
	return nil
}

// LogExportFailure logs an export failure to the fallback log
func (fl *FallbackLogger) LogExportFailure(operation string, err error, consecutiveFailures int) {
	if !fl.enabled {
		return
	}

	fl.mutex.Lock()
	defer fl.mutex.Unlock()

	entry := FallbackLogEntry{
		Timestamp:   time.Now(),
		Operation:   operation,
		Error:       err.Error(),
		Failures:    consecutiveFailures,
		ServiceName: "tdcli",
	}

	// Write to log file as JSON
	if fl.logFile != nil {
		jsonData, err := json.Marshal(entry)
		if err == nil {
			fl.logFile.WriteString(string(jsonData) + "\n")
			fl.logFile.Sync() // Ensure data is written to disk
			fl.logCount++
		}
	}

	// Also log to standard logger
	log.Printf("OTEL Export Failure [%s]: %v (consecutive failures: %d)",
		operation, err, consecutiveFailures)
}

// LogRecovery logs when exports recover after failures
func (fl *FallbackLogger) LogRecovery(operation string, failureCount int) {
	if !fl.enabled {
		return
	}

	fl.mutex.Lock()
	defer fl.mutex.Unlock()

	entry := FallbackLogEntry{
		Timestamp:   time.Now(),
		Operation:   operation,
		Error:       fmt.Sprintf("RECOVERED after %d failures", failureCount),
		Failures:    0,
		ServiceName: "tdcli",
	}

	// Write to log file as JSON
	if fl.logFile != nil {
		jsonData, err := json.Marshal(entry)
		if err == nil {
			fl.logFile.WriteString(string(jsonData) + "\n")
			fl.logFile.Sync()
			fl.logCount++
		}
	}

	// Also log to standard logger
	log.Printf("OTEL Export Recovery [%s]: Recovered after %d consecutive failures",
		operation, failureCount)
}

// GetLogCount returns the number of entries written to the fallback log
func (fl *FallbackLogger) GetLogCount() int {
	fl.mutex.RLock()
	defer fl.mutex.RUnlock()
	return fl.logCount
}

// GetLogPath returns the path to the fallback log file
func (fl *FallbackLogger) GetLogPath() string {
	fl.mutex.RLock()
	defer fl.mutex.RUnlock()
	return fl.logPath
}

// IsEnabled returns whether the fallback logger is enabled
func (fl *FallbackLogger) IsEnabled() bool {
	fl.mutex.RLock()
	defer fl.mutex.RUnlock()
	return fl.enabled
}

// Reset resets the fallback logger state
func (fl *FallbackLogger) Reset() {
	fl.mutex.Lock()
	defer fl.mutex.Unlock()

	fl.logCount = 0

	// Close and recreate log file
	if fl.logFile != nil {
		fl.logFile.Close()
	}

	if err := fl.initLogFile(); err != nil {
		log.Printf("Warning: Failed to reset fallback logger: %v", err)
		fl.enabled = false
	}
}

// Close closes the fallback logger and its resources
func (fl *FallbackLogger) Close() error {
	fl.mutex.Lock()
	defer fl.mutex.Unlock()

	if fl.logFile != nil {
		err := fl.logFile.Close()
		fl.logFile = nil
		return err
	}

	return nil
}
