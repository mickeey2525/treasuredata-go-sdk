package treasuredata

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestValidateHookCommand(t *testing.T) {
	longString := strings.Repeat("a", MaxCommandLength+1)
	testCases := []struct {
		name    string
		command []string
		wantErr bool
	}{
		{"valid command", []string{"echo", "hello"}, false},
		{"empty command", []string{}, true},
		{"command too long", []string{longString}, true},
		{"argument too long", []string{"echo", longString}, true},
		{"dangerous character ;", []string{"echo", "hello;"}, true},
		{"dangerous character |", []string{"echo", "hello|"}, true},
		{"dangerous character &", []string{"echo", "hello&"}, true},
		{"dangerous character $", []string{"echo", "hello$"}, true},
		{"dangerous character `", []string{"echo", "hello`"}, true},
		{"dangerous character newline", []string{"echo", "hello\n"}, true},
		{"path traversal", []string{"../bin/evil"}, true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateHookCommand(tc.command)
			if (err != nil) != tc.wantErr {
				t.Errorf("validateHookCommand() error = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

func TestLoadHooksConfig(t *testing.T) {
	projectDir := t.TempDir()

	// No config file
	_, err := loadHooksConfig(projectDir)
	if err != nil {
		t.Errorf("loadHooksConfig() with no file should not return error, got %v", err)
	}

	// Valid config file
	validConfig := `{"pre_upload_hooks": [{"name": "test", "command": ["echo", "hello"]}]}`
	os.WriteFile(filepath.Join(projectDir, ".td-hooks.json"), []byte(validConfig), 0644)
	config, err := loadHooksConfig(projectDir)
	if err != nil {
		t.Errorf("loadHooksConfig() with valid file returned error: %v", err)
	}
	if len(config.PreUploadHooks) != 1 {
		t.Errorf("loadHooksConfig() expected 1 hook, got %d", len(config.PreUploadHooks))
	}

	// Invalid JSON
	invalidJSON := `{"pre_upload_hooks": [`
	os.WriteFile(filepath.Join(projectDir, ".td-hooks.json"), []byte(invalidJSON), 0644)
	_, err = loadHooksConfig(projectDir)
	if err == nil {
		t.Errorf("loadHooksConfig() with invalid JSON should return error")
	}

	// Invalid hook in config
	invalidHookConfig := `{"pre_upload_hooks": [{"name": "", "command": ["echo", "hello"]}]}`
	os.WriteFile(filepath.Join(projectDir, ".td-hooks.json"), []byte(invalidHookConfig), 0644)
	_, err = loadHooksConfig(projectDir)
	if err == nil {
		t.Errorf("loadHooksConfig() with invalid hook should return error")
	}
}

func TestExecutePreUploadHooks(t *testing.T) {
	projectDir := t.TempDir()

	// No hooks
	err := executePreUploadHooks(projectDir)
	if err != nil {
		t.Errorf("executePreUploadHooks() with no hooks should not return error, got %v", err)
	}

	// Successful hook
	successConfig := `{"pre_upload_hooks": [{"name": "test", "command": ["echo", "hello"], "fail_on_error": true}]}`
	os.WriteFile(filepath.Join(projectDir, ".td-hooks.json"), []byte(successConfig), 0644)
	err = executePreUploadHooks(projectDir)
	if err != nil {
		t.Errorf("executePreUploadHooks() with successful hook returned error: %v", err)
	}

	// Failing hook with FailOnError=true
	failConfig := `{"pre_upload_hooks": [{"name": "test", "command": ["sh", "-c", "exit 1"], "fail_on_error": true}]}`
	os.WriteFile(filepath.Join(projectDir, ".td-hooks.json"), []byte(failConfig), 0644)
	err = executePreUploadHooks(projectDir)
	if err == nil {
		t.Errorf("executePreUploadHooks() with failing hook (FailOnError=true) should return error")
	}

	// Failing hook with FailOnError=false
	noFailConfig := `{"pre_upload_hooks": [{"name": "test", "command": ["sh", "-c", "exit 1"], "fail_on_error": false}]}`
	os.WriteFile(filepath.Join(projectDir, ".td-hooks.json"), []byte(noFailConfig), 0644)
	err = executePreUploadHooks(projectDir)
	if err != nil {
		t.Errorf("executePreUploadHooks() with failing hook (FailOnError=false) should not return error, got %v", err)
	}
}

func TestValidateWorkingDir(t *testing.T) {
	projectDir := t.TempDir()
	testCases := []struct {
		name       string
		workingDir string
		wantErr    bool
	}{
		{"empty working dir", "", false},
		{"project dir", ".", false},
		{"sub dir", "subdir", false},
		{"absolute path inside project", filepath.Join(projectDir, "subdir"), false},
		{"path traversal", "..", true},
		{"absolute path outside project", t.TempDir(), true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.name == "sub dir" {
				os.Mkdir(filepath.Join(projectDir, "subdir"), 0755)
			}
			_, err := validateWorkingDir(tc.workingDir, projectDir)
			if (err != nil) != tc.wantErr {
				t.Errorf("validateWorkingDir() error = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

func TestValidateHook(t *testing.T) {
	projectDir := t.TempDir()
	testCases := []struct {
		name    string
		hook    WorkflowHook
		wantErr bool
	}{
		{"valid hook", WorkflowHook{Name: "test", Command: []string{"echo", "hello"}}, false},
		{"empty name", WorkflowHook{Command: []string{"echo", "hello"}}, true},
		{"invalid command", WorkflowHook{Name: "test", Command: []string{}}, true},
		{"negative timeout", WorkflowHook{Name: "test", Command: []string{"echo"}, Timeout: -1}, true},
		{"timeout too large", WorkflowHook{Name: "test", Command: []string{"echo"}, Timeout: int(MaxHookTimeout.Seconds()) + 1}, true},
		{"invalid working dir", WorkflowHook{Name: "test", Command: []string{"echo"}, WorkingDir: ".."}, true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateHook(tc.hook, projectDir)
			if (err != nil) != tc.wantErr {
				t.Errorf("validateHook() error = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}
