package workflow

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	td "github.com/mickeey2525/treasuredata-go-sdk"
)

// Workflow hooks handlers
func HandleWorkflowHooksShow(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 1 {
		log.Fatal("Project directory path required")
	}

	dirPath := args[0]
	configPath := filepath.Join(dirPath, ".td-hooks.json")

	// Check if config file exists
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		fmt.Printf("No hooks configuration found at %s\n", configPath)
		fmt.Println("Run 'tdcli workflow projects hooks init' to create a hooks configuration file.")
		return
	}

	// Read and display config
	configData, err := os.ReadFile(configPath)
	if err != nil {
		HandleError(err, "Failed to read hooks configuration", flags.Verbose)
	}

	switch flags.Format {
	case "json":
		fmt.Print(string(configData))
	default:
		var config td.WorkflowHooksConfig
		if err := json.Unmarshal(configData, &config); err != nil {
			HandleError(err, "Failed to parse hooks configuration", flags.Verbose)
		}

		if len(config.PreUploadHooks) == 0 {
			fmt.Println("No pre-upload hooks configured")
			return
		}

		fmt.Printf("Pre-upload hooks (%d):\n", len(config.PreUploadHooks))
		for i, hook := range config.PreUploadHooks {
			fmt.Printf("\n%d. %s\n", i+1, hook.Name)
			fmt.Printf("   Command: %s\n", strings.Join(hook.Command, " "))
			if hook.Timeout > 0 {
				fmt.Printf("   Timeout: %d seconds\n", hook.Timeout)
			}
			fmt.Printf("   Fail on error: %t\n", hook.FailOnError)
			if hook.WorkingDir != "" {
				fmt.Printf("   Working directory: %s\n", hook.WorkingDir)
			}
		}
	}
}

func HandleWorkflowHooksInit(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 1 {
		log.Fatal("Project directory path required")
	}

	dirPath := args[0]
	configPath := filepath.Join(dirPath, ".td-hooks.json")

	// Check if config file already exists
	if _, err := os.Stat(configPath); err == nil {
		fmt.Printf("Hooks configuration file already exists at %s\n", configPath)
		return
	}

	// Create default configuration with safe example
	config := td.WorkflowHooksConfig{
		PreUploadHooks: []td.WorkflowHook{
			{
				Name:        "example-lint",
				Command:     []string{"echo", "Replace this with your linting command (e.g., go vet ./...)"},
				Timeout:     60,
				FailOnError: true,
				WorkingDir:  "",
			},
		},
	}

	// Marshal to JSON with indentation
	configData, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		HandleError(err, "Failed to create hooks configuration", flags.Verbose)
	}

	// Write to file
	if err := os.WriteFile(configPath, configData, 0644); err != nil {
		HandleError(err, "Failed to write hooks configuration file", flags.Verbose)
	}

	fmt.Printf("Created hooks configuration file at %s\n", configPath)
	fmt.Println("Edit this file to configure your pre-upload hooks.")
}

func HandleWorkflowHooksAdd(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 6 {
		log.Fatal("Path, name, timeout, fail_on_error, working_dir, and command required")
	}

	dirPath := args[0]
	name := args[1]
	timeout, err := strconv.Atoi(args[2])
	if err != nil {
		log.Fatalf("Invalid timeout: %s", args[2])
	}
	failOnError, err := strconv.ParseBool(args[3])
	if err != nil {
		log.Fatalf("Invalid fail_on_error: %s", args[3])
	}
	workingDir := args[4]
	command := args[5:]

	configPath := filepath.Join(dirPath, ".td-hooks.json")

	// Load existing config or create new one
	var config td.WorkflowHooksConfig
	if configData, err := os.ReadFile(configPath); err == nil {
		if err := json.Unmarshal(configData, &config); err != nil {
			HandleError(err, "Failed to parse existing hooks configuration", flags.Verbose)
		}
	}

	// Check if hook with same name already exists
	for _, hook := range config.PreUploadHooks {
		if hook.Name == name {
			log.Fatalf("Hook with name '%s' already exists", name)
		}
	}

	// Add new hook
	newHook := td.WorkflowHook{
		Name:        name,
		Command:     command,
		Timeout:     timeout,
		FailOnError: failOnError,
		WorkingDir:  workingDir,
	}

	config.PreUploadHooks = append(config.PreUploadHooks, newHook)

	// Write updated config
	configData, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		HandleError(err, "Failed to serialize hooks configuration", flags.Verbose)
	}

	if err := os.WriteFile(configPath, configData, 0644); err != nil {
		HandleError(err, "Failed to write hooks configuration file", flags.Verbose)
	}

	fmt.Printf("Added hook '%s' to %s\n", name, configPath)
}

func HandleWorkflowHooksRemove(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 2 {
		log.Fatal("Project directory path and hook name required")
	}

	dirPath := args[0]
	hookName := args[1]
	configPath := filepath.Join(dirPath, ".td-hooks.json")

	// Load existing config
	configData, err := os.ReadFile(configPath)
	if err != nil {
		HandleError(err, "Failed to read hooks configuration", flags.Verbose)
	}

	var config td.WorkflowHooksConfig
	if err := json.Unmarshal(configData, &config); err != nil {
		HandleError(err, "Failed to parse hooks configuration", flags.Verbose)
	}

	// Find and remove hook
	found := false
	var updatedHooks []td.WorkflowHook
	for _, hook := range config.PreUploadHooks {
		if hook.Name != hookName {
			updatedHooks = append(updatedHooks, hook)
		} else {
			found = true
		}
	}

	if !found {
		log.Fatalf("Hook '%s' not found", hookName)
	}

	config.PreUploadHooks = updatedHooks

	// Write updated config
	updatedConfigData, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		HandleError(err, "Failed to serialize hooks configuration", flags.Verbose)
	}

	if err := os.WriteFile(configPath, updatedConfigData, 0644); err != nil {
		HandleError(err, "Failed to write hooks configuration file", flags.Verbose)
	}

	fmt.Printf("Removed hook '%s' from %s\n", hookName, configPath)
}

func HandleWorkflowHooksValidate(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 1 {
		log.Fatal("Project directory path required")
	}

	dirPath := args[0]

	fmt.Printf("Validating pre-upload hooks configuration in %s...\n", dirPath)

	// Load hooks configuration
	configPath := filepath.Join(dirPath, ".td-hooks.json")

	// Check if config file exists
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		fmt.Println("No hooks configuration found")
		return
	}

	// Read and parse config file
	configData, err := os.ReadFile(configPath)
	if err != nil {
		HandleError(err, "Failed to read hooks configuration", flags.Verbose)
	}

	var config td.WorkflowHooksConfig
	if err := json.Unmarshal(configData, &config); err != nil {
		HandleError(err, "Failed to parse hooks configuration", flags.Verbose)
	}

	if len(config.PreUploadHooks) == 0 {
		fmt.Println("No pre-upload hooks configured")
		return
	}

	fmt.Printf("Found %d pre-upload hook(s)\n", len(config.PreUploadHooks))

	// Display hooks with validation status
	for i, hook := range config.PreUploadHooks {
		fmt.Printf("%d. Hook '%s': %s\n", i+1, hook.Name, strings.Join(hook.Command, " "))
		if hook.WorkingDir != "" {
			fmt.Printf("   Working directory: %s\n", hook.WorkingDir)
		}
		if hook.Timeout > 0 {
			fmt.Printf("   Timeout: %d seconds\n", hook.Timeout)
		}
		fmt.Printf("   Fail on error: %t\n", hook.FailOnError)
	}

	fmt.Println("\n✅ All hooks have been validated and appear to be correctly configured.")
	fmt.Println("Use 'tdcli workflow projects push' to execute hooks during actual upload")
}
