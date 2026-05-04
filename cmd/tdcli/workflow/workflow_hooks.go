package workflow

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	td "github.com/mickeey2525/treasuredata-go-sdk"
)

// HandleWorkflowHooksShow displays the configured hooks for a project.
func HandleWorkflowHooksShow(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	if len(args) < 1 {
		return usageError("Project directory path required")
	}

	dirPath := args[0]
	configPath := filepath.Join(dirPath, ".td-hooks.json")

	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		fmt.Printf("No hooks configuration found at %s\n", configPath)
		fmt.Println("Run 'tdcli workflow projects hooks init' to create a hooks configuration file.")
		return nil
	}

	configData, err := os.ReadFile(configPath)
	if err != nil {
		return wrapError(err, "failed to read hooks configuration", flags.Verbose)
	}

	switch flags.Format {
	case "json":
		fmt.Print(string(configData))
	default:
		var config td.WorkflowHooksConfig
		if err := json.Unmarshal(configData, &config); err != nil {
			return wrapError(err, "failed to parse hooks configuration", flags.Verbose)
		}

		if len(config.PreUploadHooks) == 0 {
			fmt.Println("No pre-upload hooks configured")
			return nil
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
	return nil
}

// HandleWorkflowHooksInit creates a default hooks configuration file.
func HandleWorkflowHooksInit(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	if len(args) < 1 {
		return usageError("Project directory path required")
	}

	dirPath := args[0]
	configPath := filepath.Join(dirPath, ".td-hooks.json")

	if _, err := os.Stat(configPath); err == nil {
		fmt.Printf("Hooks configuration file already exists at %s\n", configPath)
		return nil
	}

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

	configData, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return wrapError(err, "failed to create hooks configuration", flags.Verbose)
	}

	if err := os.WriteFile(configPath, configData, 0644); err != nil {
		return wrapError(err, "failed to write hooks configuration file", flags.Verbose)
	}

	fmt.Printf("Created hooks configuration file at %s\n", configPath)
	fmt.Println("Edit this file to configure your pre-upload hooks.")
	return nil
}

// HandleWorkflowHooksAdd appends a new hook to the configuration.
func HandleWorkflowHooksAdd(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	if len(args) < 6 {
		return usageError("Path, name, timeout, fail_on_error, working_dir, and command required")
	}

	dirPath := args[0]
	name := args[1]
	timeout, err := strconv.Atoi(args[2])
	if err != nil {
		return usageError(fmt.Sprintf("Invalid timeout: %s", args[2]))
	}
	failOnError, err := strconv.ParseBool(args[3])
	if err != nil {
		return usageError(fmt.Sprintf("Invalid fail_on_error: %s", args[3]))
	}
	workingDir := args[4]
	command := args[5:]

	configPath := filepath.Join(dirPath, ".td-hooks.json")

	var config td.WorkflowHooksConfig
	if configData, err := os.ReadFile(configPath); err == nil {
		if err := json.Unmarshal(configData, &config); err != nil {
			return wrapError(err, "failed to parse existing hooks configuration", flags.Verbose)
		}
	}

	for _, hook := range config.PreUploadHooks {
		if hook.Name == name {
			return fmt.Errorf("hook with name %q already exists", name)
		}
	}

	newHook := td.WorkflowHook{
		Name:        name,
		Command:     command,
		Timeout:     timeout,
		FailOnError: failOnError,
		WorkingDir:  workingDir,
	}

	config.PreUploadHooks = append(config.PreUploadHooks, newHook)

	configData, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return wrapError(err, "failed to serialize hooks configuration", flags.Verbose)
	}

	if err := os.WriteFile(configPath, configData, 0644); err != nil {
		return wrapError(err, "failed to write hooks configuration file", flags.Verbose)
	}

	fmt.Printf("Added hook '%s' to %s\n", name, configPath)
	return nil
}

// HandleWorkflowHooksRemove removes a hook from the configuration.
func HandleWorkflowHooksRemove(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	if len(args) < 2 {
		return usageError("Project directory path and hook name required")
	}

	dirPath := args[0]
	hookName := args[1]
	configPath := filepath.Join(dirPath, ".td-hooks.json")

	configData, err := os.ReadFile(configPath)
	if err != nil {
		return wrapError(err, "failed to read hooks configuration", flags.Verbose)
	}

	var config td.WorkflowHooksConfig
	if err := json.Unmarshal(configData, &config); err != nil {
		return wrapError(err, "failed to parse hooks configuration", flags.Verbose)
	}

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
		return fmt.Errorf("hook %q not found", hookName)
	}

	config.PreUploadHooks = updatedHooks

	updatedConfigData, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return wrapError(err, "failed to serialize hooks configuration", flags.Verbose)
	}

	if err := os.WriteFile(configPath, updatedConfigData, 0644); err != nil {
		return wrapError(err, "failed to write hooks configuration file", flags.Verbose)
	}

	fmt.Printf("Removed hook '%s' from %s\n", hookName, configPath)
	return nil
}

// HandleWorkflowHooksValidate validates the hooks configuration.
func HandleWorkflowHooksValidate(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	if len(args) < 1 {
		return usageError("Project directory path required")
	}

	dirPath := args[0]

	fmt.Printf("Validating pre-upload hooks configuration in %s...\n", dirPath)

	configPath := filepath.Join(dirPath, ".td-hooks.json")

	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		fmt.Println("No hooks configuration found")
		return nil
	}

	configData, err := os.ReadFile(configPath)
	if err != nil {
		return wrapError(err, "failed to read hooks configuration", flags.Verbose)
	}

	var config td.WorkflowHooksConfig
	if err := json.Unmarshal(configData, &config); err != nil {
		return wrapError(err, "failed to parse hooks configuration", flags.Verbose)
	}

	if len(config.PreUploadHooks) == 0 {
		fmt.Println("No pre-upload hooks configured")
		return nil
	}

	fmt.Printf("Found %d pre-upload hook(s)\n", len(config.PreUploadHooks))

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
	return nil
}
