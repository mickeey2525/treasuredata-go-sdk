package otel

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"
)

// RetryConfig holds configuration for retry behavior
type RetryConfig struct {
	MaxAttempts   int
	InitialDelay  time.Duration
	MaxDelay      time.Duration
	BackoffFactor float64
	Jitter        bool
}

// DefaultRetryConfig returns sensible defaults for retry configuration
func DefaultRetryConfig() *RetryConfig {
	return &RetryConfig{
		MaxAttempts:   3,
		InitialDelay:  time.Second,
		MaxDelay:      30 * time.Second,
		BackoffFactor: 2.0,
		Jitter:        true,
	}
}

// CircuitBreakerState represents the state of a circuit breaker
type CircuitBreakerState int

const (
	CircuitClosed CircuitBreakerState = iota
	CircuitOpen
	CircuitHalfOpen
)

// String returns the string representation of the circuit breaker state
func (s CircuitBreakerState) String() string {
	switch s {
	case CircuitClosed:
		return "CLOSED"
	case CircuitOpen:
		return "OPEN"
	case CircuitHalfOpen:
		return "HALF_OPEN"
	default:
		return "UNKNOWN"
	}
}

// CircuitBreakerConfig holds configuration for circuit breaker behavior
type CircuitBreakerConfig struct {
	FailureThreshold int
	RecoveryTimeout  time.Duration
	HalfOpenMaxCalls int
}

// DefaultCircuitBreakerConfig returns sensible defaults for circuit breaker configuration
func DefaultCircuitBreakerConfig() *CircuitBreakerConfig {
	return &CircuitBreakerConfig{
		FailureThreshold: 5,
		RecoveryTimeout:  60 * time.Second,
		HalfOpenMaxCalls: 3,
	}
}

// CircuitBreaker implements the circuit breaker pattern for export operations
type CircuitBreaker struct {
	config        *CircuitBreakerConfig
	state         CircuitBreakerState
	failures      int
	lastFailure   time.Time
	halfOpenCalls int
	mutex         sync.RWMutex
}

// NewCircuitBreaker creates a new circuit breaker with the given configuration
func NewCircuitBreaker(config *CircuitBreakerConfig) *CircuitBreaker {
	if config == nil {
		config = DefaultCircuitBreakerConfig()
	}

	return &CircuitBreaker{
		config: config,
		state:  CircuitClosed,
	}
}

// Execute runs the given function with circuit breaker protection
func (cb *CircuitBreaker) Execute(ctx context.Context, operation string, fn func() error) error {
	if !cb.canExecute() {
		return NewError(operation, fmt.Errorf("circuit breaker is open"))
	}

	err := fn()
	cb.recordResult(err == nil)

	return err
}

// canExecute checks if the circuit breaker allows execution
func (cb *CircuitBreaker) canExecute() bool {
	cb.mutex.Lock()
	defer cb.mutex.Unlock()

	switch cb.state {
	case CircuitClosed:
		return true
	case CircuitOpen:
		if time.Since(cb.lastFailure) >= cb.config.RecoveryTimeout {
			cb.state = CircuitHalfOpen
			cb.halfOpenCalls = 0
			log.Printf("Circuit breaker transitioned to HALF_OPEN state")
			return true
		}
		return false
	case CircuitHalfOpen:
		return cb.halfOpenCalls < cb.config.HalfOpenMaxCalls
	default:
		return false
	}
}

// recordResult records the result of an operation and updates circuit breaker state
func (cb *CircuitBreaker) recordResult(success bool) {
	cb.mutex.Lock()
	defer cb.mutex.Unlock()

	if success {
		cb.onSuccess()
	} else {
		cb.onFailure()
	}
}

// onSuccess handles successful operation results
func (cb *CircuitBreaker) onSuccess() {
	switch cb.state {
	case CircuitHalfOpen:
		cb.halfOpenCalls++
		if cb.halfOpenCalls >= cb.config.HalfOpenMaxCalls {
			cb.state = CircuitClosed
			cb.failures = 0
			cb.halfOpenCalls = 0
			log.Printf("Circuit breaker transitioned to CLOSED state")
		}
	case CircuitClosed:
		cb.failures = 0
	}
}

// onFailure handles failed operation results
func (cb *CircuitBreaker) onFailure() {
	cb.failures++
	cb.lastFailure = time.Now()

	switch cb.state {
	case CircuitClosed:
		if cb.failures >= cb.config.FailureThreshold {
			cb.state = CircuitOpen
			log.Printf("Circuit breaker opened after %d failures", cb.failures)
		}
	case CircuitHalfOpen:
		cb.state = CircuitOpen
		cb.halfOpenCalls = 0
		log.Printf("Circuit breaker returned to OPEN state after failure in half-open state")
	}
}

// GetState returns the current state of the circuit breaker
func (cb *CircuitBreaker) GetState() CircuitBreakerState {
	cb.mutex.RLock()
	defer cb.mutex.RUnlock()
	return cb.state
}

// GetFailures returns the current failure count
func (cb *CircuitBreaker) GetFailures() int {
	cb.mutex.RLock()
	defer cb.mutex.RUnlock()
	return cb.failures
}

// Reset resets the circuit breaker to its initial state
func (cb *CircuitBreaker) Reset() {
	cb.mutex.Lock()
	defer cb.mutex.Unlock()

	cb.state = CircuitClosed
	cb.failures = 0
	cb.halfOpenCalls = 0
	log.Printf("Circuit breaker reset to CLOSED state")
}

// RetryWithBackoff executes a function with exponential backoff retry logic
func RetryWithBackoff(ctx context.Context, config *RetryConfig, operation string, fn func() error) error {
	if config == nil {
		config = DefaultRetryConfig()
	}

	var lastErr error
	delay := config.InitialDelay

	for attempt := 0; attempt < config.MaxAttempts; attempt++ {
		// Execute the function
		err := fn()
		if err == nil {
			if attempt > 0 {
				log.Printf("Operation %s succeeded after %d attempts", operation, attempt+1)
			}
			return nil
		}

		lastErr = err

		// Don't retry on the last attempt
		if attempt == config.MaxAttempts-1 {
			break
		}

		// Log the retry attempt
		log.Printf("Operation %s failed (attempt %d/%d): %v. Retrying in %v",
			operation, attempt+1, config.MaxAttempts, err, delay)

		// Wait for the delay period
		select {
		case <-ctx.Done():
			return fmt.Errorf("operation %s cancelled during retry: %w", operation, ctx.Err())
		case <-time.After(delay):
		}

		// Calculate next delay with exponential backoff
		delay = time.Duration(float64(delay) * config.BackoffFactor)
		if delay > config.MaxDelay {
			delay = config.MaxDelay
		}

		// Add jitter to prevent thundering herd
		if config.Jitter {
			jitter := time.Duration(float64(delay) * 0.1 * (2*rand.Float64() - 1))
			delay += jitter
			if delay < 0 {
				delay = config.InitialDelay
			}
		}
	}

	return NewError(operation, fmt.Errorf("operation failed after %d attempts: %w", config.MaxAttempts, lastErr))
}

// ExportFailureHandler handles export failures with retry and circuit breaker logic
type ExportFailureHandler struct {
	retryConfig         *RetryConfig
	circuitBreaker      *CircuitBreaker
	fallbackLogger      *FallbackLogger
	consecutiveFailures int
	mutex               sync.RWMutex
}

// NewExportFailureHandler creates a new export failure handler
func NewExportFailureHandler(retryConfig *RetryConfig, cbConfig *CircuitBreakerConfig) *ExportFailureHandler {
	if retryConfig == nil {
		retryConfig = DefaultRetryConfig()
	}

	return &ExportFailureHandler{
		retryConfig:    retryConfig,
		circuitBreaker: NewCircuitBreaker(cbConfig),
		fallbackLogger: NewFallbackLogger(),
	}
}

// HandleExport executes an export operation with retry and circuit breaker protection
func (h *ExportFailureHandler) HandleExport(ctx context.Context, operation string, exportFn func() error) error {
	return h.circuitBreaker.Execute(ctx, operation, func() error {
		err := RetryWithBackoff(ctx, h.retryConfig, operation, exportFn)
		if err != nil {
			h.recordFailure(operation, err)
			return err
		}
		h.recordSuccess(operation)
		return nil
	})
}

// recordSuccess records a successful export
func (h *ExportFailureHandler) recordSuccess(operation string) {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	if h.consecutiveFailures > 0 {
		log.Printf("Export operation %s recovered after %d consecutive failures", operation, h.consecutiveFailures)
		h.consecutiveFailures = 0
	}
}

// recordFailure records a failed export and triggers fallback logging if needed
func (h *ExportFailureHandler) recordFailure(operation string, err error) {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	h.consecutiveFailures++

	// Log to fallback logger if circuit breaker is open or we have many consecutive failures
	if h.circuitBreaker.GetState() == CircuitOpen || h.consecutiveFailures >= 3 {
		h.fallbackLogger.LogExportFailure(operation, err, h.consecutiveFailures)
	}
}

// GetStats returns statistics about export failures
func (h *ExportFailureHandler) GetStats() map[string]interface{} {
	h.mutex.RLock()
	defer h.mutex.RUnlock()

	return map[string]interface{}{
		"consecutive_failures":     h.consecutiveFailures,
		"circuit_breaker_state":    h.circuitBreaker.GetState().String(),
		"circuit_breaker_failures": h.circuitBreaker.GetFailures(),
		"fallback_logs_written":    h.fallbackLogger.GetLogCount(),
	}
}

// Reset resets the failure handler state
func (h *ExportFailureHandler) Reset() {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	h.consecutiveFailures = 0
	h.circuitBreaker.Reset()
	h.fallbackLogger.Reset()
	log.Printf("Export failure handler reset")
}
