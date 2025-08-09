package treasuredata

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// WorkflowHook represents a single hook configuration
type WorkflowHook struct {
	Name        string   `json:"name"`
	Command     []string `json:"command"`
	Timeout     int      `json:"timeout,omitempty"`     // timeout in seconds, default 60
	FailOnError bool     `json:"fail_on_error"`         // whether to fail upload if hook fails
	WorkingDir  string   `json:"working_dir,omitempty"` // working directory, default is project directory
}

// WorkflowHooksConfig represents the hooks configuration file
type WorkflowHooksConfig struct {
	PreUploadHooks []WorkflowHook `json:"pre_upload_hooks"`
}

// Hook execution constants
const (
	DefaultHookTimeout = 60 * time.Second
	MaxHookTimeout     = 600 * time.Second // 10 minutes max
	MaxCommandLength   = 1000              // Maximum command string length
)

// validateHookCommand validates a hook command for security
func validateHookCommand(command []string) error {
	if len(command) == 0 {
		return fmt.Errorf("command cannot be empty")
	}

	// Validate each command argument
	for i, arg := range command {
		if len(arg) > MaxCommandLength {
			return fmt.Errorf("command argument %d too long (max %d characters)", i, MaxCommandLength)
		}

		// Block dangerous characters that could be used for command injection
		if strings.ContainsAny(arg, ";|&$`\n\r") {
			return fmt.Errorf("command argument %d contains dangerous characters", i)
		}
	}

	// Validate executable path (first argument)
	executable := command[0]
	if strings.Contains(executable, "..") {
		return fmt.Errorf("executable path cannot contain '..' for security reasons")
	}

	return nil
}

// validateWorkingDir validates and cleans a working directory path
func validateWorkingDir(workingDir, projectDir string) (string, error) {
	if workingDir == "" {
		return projectDir, nil
	}

	// Convert to absolute path
	var absWorkingDir string
	if filepath.IsAbs(workingDir) {
		absWorkingDir = workingDir
	} else {
		absWorkingDir = filepath.Join(projectDir, workingDir)
	}

	// Clean the path to resolve any .., ., etc.
	absWorkingDir = filepath.Clean(absWorkingDir)

	// Get absolute path of project directory for comparison
	absProjectDir, err := filepath.Abs(projectDir)
	if err != nil {
		return "", fmt.Errorf("failed to get absolute project directory: %w", err)
	}
	absProjectDir = filepath.Clean(absProjectDir)

	// Security check: ensure working directory is within or equal to project directory
	relPath, err := filepath.Rel(absProjectDir, absWorkingDir)
	if err != nil {
		return "", fmt.Errorf("failed to compute relative path: %w", err)
	}

	// Check if the relative path tries to escape the project directory
	if strings.HasPrefix(relPath, "..") {
		return "", fmt.Errorf("working directory cannot be outside project directory (attempted: %s)", workingDir)
	}

	return absWorkingDir, nil
}

// validateHook validates a single hook configuration
func validateHook(hook WorkflowHook, projectDir string) error {
	if hook.Name == "" {
		return fmt.Errorf("hook name cannot be empty")
	}

	if err := validateHookCommand(hook.Command); err != nil {
		return fmt.Errorf("invalid command for hook '%s': %w", hook.Name, err)
	}

	// Validate timeout
	if hook.Timeout < 0 {
		return fmt.Errorf("hook '%s' timeout cannot be negative", hook.Name)
	}
	timeout := time.Duration(hook.Timeout) * time.Second
	if timeout > MaxHookTimeout {
		return fmt.Errorf("hook '%s' timeout %v exceeds maximum %v", hook.Name, timeout, MaxHookTimeout)
	}

	// Validate working directory
	if _, err := validateWorkingDir(hook.WorkingDir, projectDir); err != nil {
		return fmt.Errorf("invalid working directory for hook '%s': %w", hook.Name, err)
	}

	return nil
}

// loadHooksConfig loads hooks configuration from a directory
func loadHooksConfig(dirPath string) (*WorkflowHooksConfig, error) {
	configPath := filepath.Join(dirPath, ".td-hooks.json")

	// Check if config file exists
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		// No hooks config found, return empty config
		return &WorkflowHooksConfig{}, nil
	}

	// Read and parse config file
	configData, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read hooks config file %s: %w", configPath, err)
	}

	var config WorkflowHooksConfig
	if err := json.Unmarshal(configData, &config); err != nil {
		return nil, fmt.Errorf("failed to parse hooks config file %s: %w", configPath, err)
	}

	// Validate all hooks in the configuration
	for _, hook := range config.PreUploadHooks {
		if err := validateHook(hook, dirPath); err != nil {
			return nil, fmt.Errorf("hook validation failed: %w", err)
		}
	}

	return &config, nil
}

// executeHook executes a single hook with security validations
func executeHook(hook WorkflowHook, projectDir string) error {
	// Validate hook configuration before execution
	if err := validateHook(hook, projectDir); err != nil {
		return err
	}

	// Set timeout (default 60 seconds)
	timeout := time.Duration(hook.Timeout) * time.Second
	if timeout == 0 {
		timeout = DefaultHookTimeout
	}

	// Validate and set working directory
	workingDir, err := validateWorkingDir(hook.WorkingDir, projectDir)
	if err != nil {
		return fmt.Errorf("hook '%s': %w", hook.Name, err)
	}

	// Create command with timeout context
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// Use validated command (already validated in validateHook)
	cmd := exec.CommandContext(ctx, hook.Command[0], hook.Command[1:]...)
	cmd.Dir = workingDir
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	fmt.Printf("Running hook '%s': %s\n", hook.Name, strings.Join(hook.Command, " "))
	fmt.Printf("Working directory: %s\n", workingDir)

	if err := cmd.Run(); err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			return fmt.Errorf("hook '%s' timed out after %v", hook.Name, timeout)
		}
		return fmt.Errorf("hook '%s' failed: %w", hook.Name, err)
	}

	fmt.Printf("Hook '%s' completed successfully\n", hook.Name)
	return nil
}

// executePreUploadHooks executes all pre-upload hooks
func executePreUploadHooks(dirPath string) error {
	config, err := loadHooksConfig(dirPath)
	if err != nil {
		return err
	}

	if len(config.PreUploadHooks) == 0 {
		return nil // No hooks to execute
	}

	fmt.Printf("Executing %d pre-upload hook(s)...\n", len(config.PreUploadHooks))

	for _, hook := range config.PreUploadHooks {
		if err := executeHook(hook, dirPath); err != nil {
			if hook.FailOnError {
				return fmt.Errorf("pre-upload hook failed: %w", err)
			}
			fmt.Printf("Warning: Hook '%s' failed but continuing: %v\n", hook.Name, err)
		}
	}

	fmt.Println("All pre-upload hooks completed")
	return nil
}
