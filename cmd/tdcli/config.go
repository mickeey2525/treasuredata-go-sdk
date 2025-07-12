package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/BurntSushi/toml"
)

// Config represents the CLI configuration
type Config struct {
	APIKey string `toml:"api_key"`
	Region string `toml:"region"`
	Format string `toml:"format"`
	Output string `toml:"output"`
}

// DefaultConfig returns a config with default values
func DefaultConfig() *Config {
	return &Config{
		Region: "us",
		Format: "table",
	}
}

// LoadConfig loads configuration from TOML files
// Priority: current directory > home directory > defaults
func LoadConfig() (*Config, error) {
	config := DefaultConfig()

	// Try to load from home directory first
	if homeConfig, err := loadConfigFromHome(); err == nil {
		mergeConfig(config, homeConfig)
	}

	// Try to load from current directory (higher priority)
	if localConfig, err := loadConfigFromCurrentDir(); err == nil {
		mergeConfig(config, localConfig)
	}

	return config, nil
}

// loadConfigFromHome loads config from ~/.tdcli/.tdcli.toml
func loadConfigFromHome() (*Config, error) {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return nil, err
	}

	configPath := filepath.Join(homeDir, ".tdcli", ".tdcli.toml")
	return loadConfigFromFile(configPath)
}

// loadConfigFromCurrentDir loads config from ./tdcli.toml or ./.tdcli.toml
func loadConfigFromCurrentDir() (*Config, error) {
	// Try ./tdcli.toml first
	if config, err := loadConfigFromFile("tdcli.toml"); err == nil {
		return config, nil
	}

	// Try ./.tdcli.toml
	return loadConfigFromFile(".tdcli.toml")
}

// loadConfigFromFile loads config from a specific file path
func loadConfigFromFile(path string) (*Config, error) {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return nil, err
	}

	var config Config
	if _, err := toml.DecodeFile(path, &config); err != nil {
		return nil, err
	}

	return &config, nil
}

// mergeConfig merges source config into target config (non-empty values only)
func mergeConfig(target, source *Config) {
	if source.APIKey != "" {
		target.APIKey = source.APIKey
	}
	if source.Region != "" {
		target.Region = source.Region
	}
	if source.Format != "" {
		target.Format = source.Format
	}
	if source.Output != "" {
		target.Output = source.Output
	}
}

// SaveConfig saves configuration to the specified path
func SaveConfig(config *Config, path string) error {
	// Create directory if it doesn't exist
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := toml.NewEncoder(file)
	return encoder.Encode(config)
}

// GetConfigPaths returns the paths where config files are looked for
func GetConfigPaths() []string {
	paths := []string{}

	// Home directory config
	if homeDir, err := os.UserHomeDir(); err == nil {
		paths = append(paths, filepath.Join(homeDir, ".tdcli", ".tdcli.toml"))
	}

	// Current directory configs
	paths = append(paths, "./tdcli.toml", "./.tdcli.toml")

	return paths
}

// ConfigCmd represents the config command group
type ConfigCmd struct {
	Show ConfigShowCmd `kong:"cmd,help='Show current configuration'"`
	Set  ConfigSetCmd  `kong:"cmd,help='Set configuration value'"`
	Get  ConfigGetCmd  `kong:"cmd,help='Get configuration value'"`
	Init ConfigInitCmd `kong:"cmd,help='Initialize configuration file'"`
}

// ConfigShowCmd shows the current configuration
type ConfigShowCmd struct{}

func (c *ConfigShowCmd) Run(ctx *CLIContext) error {
	config, err := LoadConfig()
	if err != nil {
		return fmt.Errorf("failed to load config: %v", err)
	}

	fmt.Println("Current Configuration:")
	fmt.Printf("API Key: %s\n", maskAPIKey(config.APIKey))
	fmt.Printf("Region: %s\n", config.Region)
	fmt.Printf("Format: %s\n", config.Format)
	fmt.Printf("Output: %s\n", config.Output)

	fmt.Println("\nConfiguration file locations (in priority order):")
	for i, path := range GetConfigPaths() {
		if _, err := os.Stat(path); err == nil {
			fmt.Printf("  %d. %s ✓\n", i+1, path)
		} else {
			fmt.Printf("  %d. %s\n", i+1, path)
		}
	}

	return nil
}

// ConfigSetCmd sets a configuration value
type ConfigSetCmd struct {
	Key    string `kong:"arg,help='Configuration key (api_key, region, format, output)'"`
	Value  string `kong:"arg,help='Configuration value'"`
	Global bool   `kong:"help='Save to global config (~/.tdcli/.tdcli.toml)'"`
}

func (c *ConfigSetCmd) Run(ctx *CLIContext) error {
	// Load current config
	config, err := LoadConfig()
	if err != nil {
		config = DefaultConfig()
	}

	// Set the value
	switch c.Key {
	case "api_key":
		config.APIKey = c.Value
	case "region":
		// Validate region
		validRegions := []string{"us", "eu", "tokyo", "ap02"}
		isValid := false
		for _, valid := range validRegions {
			if c.Value == valid {
				isValid = true
				break
			}
		}
		if !isValid {
			return fmt.Errorf("invalid region: %s. Valid regions are: %s", c.Value, strings.Join(validRegions, ", "))
		}
		config.Region = c.Value
	case "format":
		// Validate format
		validFormats := []string{"table", "json", "csv"}
		isValid := false
		for _, valid := range validFormats {
			if c.Value == valid {
				isValid = true
				break
			}
		}
		if !isValid {
			return fmt.Errorf("invalid format: %s. Valid formats are: %s", c.Value, strings.Join(validFormats, ", "))
		}
		config.Format = c.Value
	case "output":
		config.Output = c.Value
	default:
		return fmt.Errorf("unknown configuration key: %s", c.Key)
	}

	// Determine save path
	var savePath string
	if c.Global {
		homeDir, err := os.UserHomeDir()
		if err != nil {
			return fmt.Errorf("failed to get home directory: %v", err)
		}
		savePath = filepath.Join(homeDir, ".tdcli", ".tdcli.toml")
	} else {
		savePath = ".tdcli.toml"
	}

	// Save config
	if err := SaveConfig(config, savePath); err != nil {
		return fmt.Errorf("failed to save config: %v", err)
	}

	fmt.Printf("Configuration saved to %s\n", savePath)
	fmt.Printf("Set %s = %s\n", c.Key, maskValue(c.Key, c.Value))

	return nil
}

// ConfigGetCmd gets a configuration value
type ConfigGetCmd struct {
	Key string `kong:"arg,help='Configuration key (api_key, region, format, output)'"`
}

func (c *ConfigGetCmd) Run(ctx *CLIContext) error {
	config, err := LoadConfig()
	if err != nil {
		return fmt.Errorf("failed to load config: %v", err)
	}

	var value string
	switch c.Key {
	case "api_key":
		value = config.APIKey
	case "region":
		value = config.Region
	case "format":
		value = config.Format
	case "output":
		value = config.Output
	default:
		return fmt.Errorf("unknown configuration key: %s", c.Key)
	}

	if value == "" {
		fmt.Printf("%s: (not set)\n", c.Key)
	} else {
		fmt.Printf("%s: %s\n", c.Key, maskValue(c.Key, value))
	}

	return nil
}

// ConfigInitCmd initializes a configuration file
type ConfigInitCmd struct {
	Global bool `kong:"help='Create global config (~/.tdcli/.tdcli.toml)'"`
}

func (c *ConfigInitCmd) Run(ctx *CLIContext) error {
	// Determine save path
	var savePath string
	if c.Global {
		homeDir, err := os.UserHomeDir()
		if err != nil {
			return fmt.Errorf("failed to get home directory: %v", err)
		}
		savePath = filepath.Join(homeDir, ".tdcli", ".tdcli.toml")
	} else {
		savePath = ".tdcli.toml"
	}

	// Check if file already exists
	if _, err := os.Stat(savePath); err == nil {
		fmt.Printf("Configuration file already exists: %s\n", savePath)
		if !promptConfirmation("Overwrite existing configuration?") {
			fmt.Println("Configuration initialization cancelled.")
			return nil
		}
	}

	fmt.Println("Welcome to Treasure Data CLI configuration setup!")
	fmt.Println("Please provide the following information to configure your CLI:")
	fmt.Println()

	config := DefaultConfig()

	// Prompt for API Key
	apiKey, err := promptInput("API Key (format: account_id/api_key)", "", validateAPIKey)
	if err != nil {
		return err
	}
	config.APIKey = apiKey

	// Prompt for Region
	region, err := promptChoice("Region", []string{"us", "eu", "tokyo", "ap02"}, "us", map[string]string{
		"us":    "United States (api.treasuredata.com)",
		"eu":    "Europe (api.eu01.treasuredata.com)",
		"tokyo": "Japan (api.treasuredata.co.jp)",
		"ap02":  "Asia Pacific (api.ap02.treasuredata.com)",
	})
	if err != nil {
		return err
	}
	config.Region = region

	// Prompt for Format
	format, err := promptChoice("Output Format", []string{"table", "json", "csv"}, "table", map[string]string{
		"table": "Human-readable table format",
		"json":  "JSON format for programmatic use",
		"csv":   "CSV format for spreadsheet import",
	})
	if err != nil {
		return err
	}
	config.Format = format

	// Prompt for Output (optional)
	output, err := promptInput("Output file (leave empty for stdout)", "", nil)
	if err != nil {
		return err
	}
	config.Output = output

	// Save config
	if err := SaveConfig(config, savePath); err != nil {
		return fmt.Errorf("failed to create config: %v", err)
	}

	fmt.Println()
	fmt.Printf("✓ Configuration successfully created: %s\n", savePath)
	fmt.Println()
	fmt.Println("Your configuration:")
	fmt.Printf("  API Key: %s\n", maskAPIKey(config.APIKey))
	fmt.Printf("  Region: %s\n", config.Region)
	fmt.Printf("  Format: %s\n", config.Format)
	if config.Output != "" {
		fmt.Printf("  Output: %s\n", config.Output)
	} else {
		fmt.Printf("  Output: stdout\n")
	}

	return nil
}

// promptInput prompts the user for text input with optional validation
func promptInput(prompt, defaultValue string, validator func(string) error) (string, error) {
	reader := bufio.NewReader(os.Stdin)
	
	for {
		if defaultValue != "" {
			fmt.Printf("%s [%s]: ", prompt, defaultValue)
		} else {
			fmt.Printf("%s: ", prompt)
		}
		
		input, err := reader.ReadString('\n')
		if err != nil {
			return "", fmt.Errorf("failed to read input: %v", err)
		}
		
		input = strings.TrimSpace(input)
		if input == "" && defaultValue != "" {
			input = defaultValue
		}
		
		if validator != nil {
			if err := validator(input); err != nil {
				fmt.Printf("Invalid input: %v\n", err)
				continue
			}
		}
		
		return input, nil
	}
}

// promptChoice prompts the user to choose from a list of options
func promptChoice(prompt string, choices []string, defaultChoice string, descriptions map[string]string) (string, error) {
	reader := bufio.NewReader(os.Stdin)
	
	for {
		fmt.Printf("%s:\n", prompt)
		for i, choice := range choices {
			desc := ""
			if descriptions != nil && descriptions[choice] != "" {
				desc = fmt.Sprintf(" - %s", descriptions[choice])
			}
			if choice == defaultChoice {
				fmt.Printf("  %d. %s (default)%s\n", i+1, choice, desc)
			} else {
				fmt.Printf("  %d. %s%s\n", i+1, choice, desc)
			}
		}
		fmt.Printf("Enter choice [1-%d] or press Enter for default: ", len(choices))
		
		input, err := reader.ReadString('\n')
		if err != nil {
			return "", fmt.Errorf("failed to read input: %v", err)
		}
		
		input = strings.TrimSpace(input)
		if input == "" {
			return defaultChoice, nil
		}
		
		// Try to parse as number
		for i, choice := range choices {
			if input == fmt.Sprintf("%d", i+1) {
				return choice, nil
			}
		}
		
		// Try to match exact string
		for _, choice := range choices {
			if strings.EqualFold(input, choice) {
				return choice, nil
			}
		}
		
		fmt.Printf("Invalid choice. Please enter a number between 1 and %d, or the exact option name.\n\n", len(choices))
	}
}

// promptConfirmation prompts the user for yes/no confirmation
func promptConfirmation(prompt string) bool {
	reader := bufio.NewReader(os.Stdin)
	
	fmt.Printf("%s [y/N]: ", prompt)
	input, err := reader.ReadString('\n')
	if err != nil {
		return false
	}
	
	input = strings.TrimSpace(strings.ToLower(input))
	return input == "y" || input == "yes"
}

// validateAPIKey validates the API key format
func validateAPIKey(apiKey string) error {
	if apiKey == "" {
		return fmt.Errorf("API key cannot be empty")
	}
	
	// API key should be in format: account_id/api_key
	if !strings.Contains(apiKey, "/") {
		return fmt.Errorf("API key must be in format: account_id/api_key")
	}
	
	parts := strings.Split(apiKey, "/")
	if len(parts) != 2 {
		return fmt.Errorf("API key must be in format: account_id/api_key")
	}
	
	if parts[0] == "" || parts[1] == "" {
		return fmt.Errorf("both account_id and api_key parts must be non-empty")
	}
	
	return nil
}

// maskAPIKey masks the API key for display
func maskAPIKey(apiKey string) string {
	if apiKey == "" {
		return "(not set)"
	}
	if len(apiKey) <= 8 {
		return "***"
	}
	return apiKey[:4] + "***" + apiKey[len(apiKey)-4:]
}

// maskValue masks sensitive values for display
func maskValue(key, value string) string {
	if key == "api_key" {
		return maskAPIKey(value)
	}
	return value
}
