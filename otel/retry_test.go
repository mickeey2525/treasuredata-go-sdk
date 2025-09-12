package otel

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRetryWithBackoff(t *testing.T) {
	tests := []struct {
		name          string
		config        *RetryConfig
		failAttempts  int
		expectedCalls int
		expectSuccess bool
	}{
		{
			name: "success on first attempt",
			config: &RetryConfig{
				MaxAttempts:   3,
				InitialDelay:  10 * time.Millisecond,
				MaxDelay:      100 * time.Millisecond,
				BackoffFactor: 2.0,
				Jitter:        false,
			},
			failAttempts:  0,
			expectedCalls: 1,
			expectSuccess: true,
		},
		{
			name: "success on second attempt",
			config: &RetryConfig{
				MaxAttempts:   3,
				InitialDelay:  10 * time.Millisecond,
				MaxDelay:      100 * time.Millisecond,
				BackoffFactor: 2.0,
				Jitter:        false,
			},
			failAttempts:  1,
			expectedCalls: 2,
			expectSuccess: true,
		},
		{
			name: "failure after all attempts",
			config: &RetryConfig{
				MaxAttempts:   3,
				InitialDelay:  10 * time.Millisecond,
				MaxDelay:      100 * time.Millisecond,
				BackoffFactor: 2.0,
				Jitter:        false,
			},
			failAttempts:  3,
			expectedCalls: 3,
			expectSuccess: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			callCount := 0
			testFunc := func() error {
				callCount++
				if callCount <= tt.failAttempts {
					return errors.New("test error")
				}
				return nil
			}

			ctx := context.Background()
			err := RetryWithBackoff(ctx, tt.config, "test_operation", testFunc)

			if callCount != tt.expectedCalls {
				t.Errorf("Expected %d calls, got %d", tt.expectedCalls, callCount)
			}

			if tt.expectSuccess && err != nil {
				t.Errorf("Expected success, got error: %v", err)
			}

			if !tt.expectSuccess && err == nil {
				t.Errorf("Expected failure, got success")
			}
		})
	}
}

func TestRetryWithBackoffContextCancellation(t *testing.T) {
	config := &RetryConfig{
		MaxAttempts:   5,
		InitialDelay:  100 * time.Millisecond,
		MaxDelay:      1 * time.Second,
		BackoffFactor: 2.0,
		Jitter:        false,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	callCount := 0
	testFunc := func() error {
		callCount++
		return errors.New("test error")
	}

	err := RetryWithBackoff(ctx, config, "test_operation", testFunc)

	if err == nil {
		t.Error("Expected error due to context cancellation")
	}

	if callCount != 1 {
		t.Errorf("Expected 1 call before cancellation, got %d", callCount)
	}
}

func TestCircuitBreaker(t *testing.T) {
	config := &CircuitBreakerConfig{
		FailureThreshold: 3,
		RecoveryTimeout:  100 * time.Millisecond,
		HalfOpenMaxCalls: 2,
	}

	cb := NewCircuitBreaker(config)

	// Test initial state
	if cb.GetState() != CircuitClosed {
		t.Errorf("Expected initial state to be CLOSED, got %s", cb.GetState())
	}

	// Test failures leading to open state
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		err := cb.Execute(ctx, "test_op", func() error {
			return errors.New("test error")
		})
		if err == nil {
			t.Error("Expected error from failing operation")
		}
	}

	if cb.GetState() != CircuitOpen {
		t.Errorf("Expected state to be OPEN after failures, got %s", cb.GetState())
	}

	// Test that operations are rejected when circuit is open
	err := cb.Execute(ctx, "test_op", func() error {
		return nil
	})
	if err == nil {
		t.Error("Expected error when circuit is open")
	}

	// Wait for recovery timeout
	time.Sleep(150 * time.Millisecond)

	// Test half-open state - first call should transition to half-open and succeed
	successCount := 0
	err = cb.Execute(ctx, "test_op", func() error {
		successCount++
		return nil
	})
	if err != nil {
		t.Errorf("Expected success in half-open state, got error: %v", err)
	}

	// Second call should also succeed
	err = cb.Execute(ctx, "test_op", func() error {
		successCount++
		return nil
	})
	if err != nil {
		t.Errorf("Expected success in half-open state, got error: %v", err)
	}

	if cb.GetState() != CircuitClosed {
		t.Errorf("Expected state to be CLOSED after successful half-open calls, got %s", cb.GetState())
	}

	if successCount != 2 {
		t.Errorf("Expected 2 successful calls, got %d", successCount)
	}
}

func TestCircuitBreakerHalfOpenFailure(t *testing.T) {
	config := &CircuitBreakerConfig{
		FailureThreshold: 2,
		RecoveryTimeout:  50 * time.Millisecond,
		HalfOpenMaxCalls: 3,
	}

	cb := NewCircuitBreaker(config)
	ctx := context.Background()

	// Trigger circuit to open
	for i := 0; i < 2; i++ {
		cb.Execute(ctx, "test_op", func() error {
			return errors.New("test error")
		})
	}

	if cb.GetState() != CircuitOpen {
		t.Errorf("Expected state to be OPEN, got %s", cb.GetState())
	}

	// Wait for recovery timeout
	time.Sleep(60 * time.Millisecond)

	// Fail in half-open state
	err := cb.Execute(ctx, "test_op", func() error {
		return errors.New("test error")
	})
	if err == nil {
		t.Error("Expected error from failing operation")
	}

	if cb.GetState() != CircuitOpen {
		t.Errorf("Expected state to return to OPEN after half-open failure, got %s", cb.GetState())
	}
}

func TestExportFailureHandler(t *testing.T) {
	retryConfig := &RetryConfig{
		MaxAttempts:   2,
		InitialDelay:  10 * time.Millisecond,
		MaxDelay:      50 * time.Millisecond,
		BackoffFactor: 2.0,
		Jitter:        false,
	}

	cbConfig := &CircuitBreakerConfig{
		FailureThreshold: 3,
		RecoveryTimeout:  100 * time.Millisecond,
		HalfOpenMaxCalls: 2,
	}

	handler := NewExportFailureHandler(retryConfig, cbConfig)
	ctx := context.Background()

	// Test successful operation
	callCount := 0
	err := handler.HandleExport(ctx, "test_export", func() error {
		callCount++
		return nil
	})

	if err != nil {
		t.Errorf("Expected success, got error: %v", err)
	}

	if callCount != 1 {
		t.Errorf("Expected 1 call, got %d", callCount)
	}

	// Test operation that succeeds after retry
	callCount = 0
	err = handler.HandleExport(ctx, "test_export", func() error {
		callCount++
		if callCount == 1 {
			return errors.New("first attempt fails")
		}
		return nil
	})

	if err != nil {
		t.Errorf("Expected success after retry, got error: %v", err)
	}

	if callCount != 2 {
		t.Errorf("Expected 2 calls, got %d", callCount)
	}

	// Test operation that fails all retries
	callCount = 0
	err = handler.HandleExport(ctx, "test_export", func() error {
		callCount++
		return errors.New("persistent failure")
	})

	if err == nil {
		t.Error("Expected error after all retries failed")
	}

	if callCount != 2 {
		t.Errorf("Expected 2 calls (max attempts), got %d", callCount)
	}

	// Check stats - should have 1 consecutive failure since this was one failed export operation
	stats := handler.GetStats()
	if stats["consecutive_failures"].(int) != 1 {
		t.Errorf("Expected 1 consecutive failure, got %v", stats["consecutive_failures"])
	}
}

func TestExportFailureHandlerCircuitBreaker(t *testing.T) {
	retryConfig := &RetryConfig{
		MaxAttempts:   1,
		InitialDelay:  10 * time.Millisecond,
		MaxDelay:      50 * time.Millisecond,
		BackoffFactor: 2.0,
		Jitter:        false,
	}

	cbConfig := &CircuitBreakerConfig{
		FailureThreshold: 2,
		RecoveryTimeout:  100 * time.Millisecond,
		HalfOpenMaxCalls: 1,
	}

	handler := NewExportFailureHandler(retryConfig, cbConfig)
	ctx := context.Background()

	// Trigger circuit breaker to open
	for i := 0; i < 2; i++ {
		handler.HandleExport(ctx, "test_export", func() error {
			return errors.New("test error")
		})
	}

	// Verify circuit is open
	stats := handler.GetStats()
	if stats["circuit_breaker_state"].(string) != "OPEN" {
		t.Errorf("Expected circuit breaker to be OPEN, got %s", stats["circuit_breaker_state"])
	}

	// Test that operations are rejected
	err := handler.HandleExport(ctx, "test_export", func() error {
		return nil
	})

	if err == nil {
		t.Error("Expected error when circuit breaker is open")
	}
}

func TestDefaultConfigs(t *testing.T) {
	retryConfig := DefaultRetryConfig()
	if retryConfig.MaxAttempts != 3 {
		t.Errorf("Expected default max attempts to be 3, got %d", retryConfig.MaxAttempts)
	}

	cbConfig := DefaultCircuitBreakerConfig()
	if cbConfig.FailureThreshold != 5 {
		t.Errorf("Expected default failure threshold to be 5, got %d", cbConfig.FailureThreshold)
	}
}
