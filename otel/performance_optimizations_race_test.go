package otel

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestPerformanceMonitorRaceCondition tests for race conditions in concurrent access
func TestPerformanceMonitorRaceCondition(t *testing.T) {
	monitor := NewPerformanceMonitor(true)

	const numGoroutines = 100
	const operationsPerGoroutine = 100
	const operationName = "race_test_operation"

	var wg sync.WaitGroup

	// Start multiple goroutines that record stats concurrently
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for j := 0; j < operationsPerGoroutine; j++ {
				// Simulate different operation durations
				duration := time.Duration(goroutineID*j+1) * time.Microsecond
				monitor.recordStats(operationName, duration)
			}
		}(i)
	}

	// Wait for all goroutines to complete
	wg.Wait()

	// Verify the final stats
	stats := monitor.GetStats()
	opStats, exists := stats[operationName]
	if !exists {
		t.Fatal("Expected operation stats not found")
	}

	expectedCount := int64(numGoroutines * operationsPerGoroutine)
	if opStats.Count != expectedCount {
		t.Errorf("Expected count %d, got %d", expectedCount, opStats.Count)
	}

	if opStats.MinTime <= 0 {
		t.Errorf("Expected positive min time, got %v", opStats.MinTime)
	}

	if opStats.MaxTime <= 0 {
		t.Errorf("Expected positive max time, got %v", opStats.MaxTime)
	}

	if opStats.TotalTime <= 0 {
		t.Errorf("Expected positive total time, got %v", opStats.TotalTime)
	}

	if opStats.MinTime > opStats.MaxTime {
		t.Errorf("Min time (%v) should not be greater than max time (%v)", opStats.MinTime, opStats.MaxTime)
	}

	t.Logf("Race condition test completed successfully:")
	t.Logf("  Operations: %d", opStats.Count)
	t.Logf("  Min time: %v", opStats.MinTime)
	t.Logf("  Max time: %v", opStats.MaxTime)
	t.Logf("  Total time: %v", opStats.TotalTime)
	t.Logf("  Average time: %v", opStats.TotalTime/time.Duration(opStats.Count))
}

// TestPerformanceMonitorConcurrentOperations tests concurrent access to different operations
func TestPerformanceMonitorConcurrentOperations(t *testing.T) {
	monitor := NewPerformanceMonitor(true)

	const numOperations = 10
	const numGoroutines = 20
	const recordsPerGoroutine = 50

	var wg sync.WaitGroup

	// Start goroutines that record stats for different operations concurrently
	for opID := 0; opID < numOperations; opID++ {
		for goroutineID := 0; goroutineID < numGoroutines; goroutineID++ {
			wg.Add(1)
			go func(operationID, gID int) {
				defer wg.Done()
				operationName := fmt.Sprintf("operation_%d", operationID)
				for i := 0; i < recordsPerGoroutine; i++ {
					duration := time.Duration((gID+1)*(i+1)) * time.Microsecond
					monitor.recordStats(operationName, duration)
				}
			}(opID, goroutineID)
		}
	}

	// Wait for all goroutines to complete
	wg.Wait()

	// Verify the final stats
	stats := monitor.GetStats()
	if len(stats) != numOperations {
		t.Errorf("Expected %d operations, got %d", numOperations, len(stats))
	}

	expectedCountPerOperation := int64(numGoroutines * recordsPerGoroutine)
	for i := 0; i < numOperations; i++ {
		operationName := fmt.Sprintf("operation_%d", i)
		opStats, exists := stats[operationName]
		if !exists {
			t.Errorf("Expected operation %s not found", operationName)
			continue
		}

		if opStats.Count != expectedCountPerOperation {
			t.Errorf("Operation %s: expected count %d, got %d", operationName, expectedCountPerOperation, opStats.Count)
		}

		if opStats.MinTime <= 0 {
			t.Errorf("Operation %s: expected positive min time, got %v", operationName, opStats.MinTime)
		}

		if opStats.MaxTime <= 0 {
			t.Errorf("Operation %s: expected positive max time, got %v", operationName, opStats.MaxTime)
		}

		if opStats.MinTime > opStats.MaxTime {
			t.Errorf("Operation %s: min time (%v) should not be greater than max time (%v)", 
				operationName, opStats.MinTime, opStats.MaxTime)
		}
	}

	t.Logf("Concurrent operations test completed successfully with %d operations", len(stats))
}

// TestPerformanceMonitorStatsConsistency tests that stats remain consistent under concurrent access
func TestPerformanceMonitorStatsConsistency(t *testing.T) {
	monitor := NewPerformanceMonitor(true)

	const operationName = "consistency_test"
	const numReaders = 10
	const numWriters = 10
	const writesPerWriter = 100
	const testDuration = 100 * time.Millisecond

	var wg sync.WaitGroup
	stopCh := make(chan struct{})

	// Start writer goroutines
	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()
			for j := 0; j < writesPerWriter; j++ {
				select {
				case <-stopCh:
					return
				default:
					duration := time.Duration(writerID*j+1) * time.Microsecond
					monitor.recordStats(operationName, duration)
				}
			}
		}(i)
	}

	// Start reader goroutines
	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()
			for {
				select {
				case <-stopCh:
					return
				default:
					stats := monitor.GetStats()
					if opStats, exists := stats[operationName]; exists {
						// Verify consistency: total time should be >= count * min time
						if opStats.Count > 0 {
							minExpectedTotal := time.Duration(opStats.Count) * opStats.MinTime
							if opStats.TotalTime < minExpectedTotal {
								t.Errorf("Reader %d: inconsistent stats - total time (%v) < count (%d) * min time (%v)",
									readerID, opStats.TotalTime, opStats.Count, opStats.MinTime)
							}

							if opStats.MinTime > opStats.MaxTime {
								t.Errorf("Reader %d: min time (%v) > max time (%v)",
									readerID, opStats.MinTime, opStats.MaxTime)
							}
						}
					}
					time.Sleep(time.Millisecond)
				}
			}
		}(i)
	}

	// Let the test run for a short duration
	time.Sleep(testDuration)
	close(stopCh)

	// Wait for all goroutines to complete
	wg.Wait()

	// Final consistency check
	stats := monitor.GetStats()
	if opStats, exists := stats[operationName]; exists {
		expectedCount := int64(numWriters * writesPerWriter)
		if opStats.Count != expectedCount {
			t.Errorf("Final count mismatch: expected %d, got %d", expectedCount, opStats.Count)
		}

		if opStats.MinTime > opStats.MaxTime {
			t.Errorf("Final stats inconsistent: min time (%v) > max time (%v)", opStats.MinTime, opStats.MaxTime)
		}

		t.Logf("Consistency test completed successfully:")
		t.Logf("  Final count: %d", opStats.Count)
		t.Logf("  Min time: %v", opStats.MinTime)
		t.Logf("  Max time: %v", opStats.MaxTime)
		t.Logf("  Total time: %v", opStats.TotalTime)
	} else {
		t.Error("Expected operation stats not found in final check")
	}
}

// TestNewOperationStats tests the constructor function
func TestNewOperationStats(t *testing.T) {
	duration := 100 * time.Microsecond
	stats := newOperationStats(duration)

	if stats.Count != 0 {
		t.Errorf("Expected initial count 0, got %d", stats.Count)
	}

	if stats.TotalTime != 0 {
		t.Errorf("Expected initial total time 0, got %v", stats.TotalTime)
	}

	if stats.MinTime != 0 {
		t.Errorf("Expected initial min time 0, got %v", stats.MinTime)
	}

	if stats.MaxTime != 0 {
		t.Errorf("Expected initial max time 0, got %v", stats.MaxTime)
	}

	if stats.LastTime != 0 {
		t.Errorf("Expected initial last time 0, got %v", stats.LastTime)
	}

	// Test that the mutex is properly initialized by trying to lock it
	stats.mutex.Lock()
	stats.mutex.Unlock()

	stats.mutex.RLock()
	stats.mutex.RUnlock()
}
