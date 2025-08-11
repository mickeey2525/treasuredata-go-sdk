package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/alecthomas/kong"
	td "github.com/mickeey2525/treasuredata-go-sdk"
)

var (
	version = "dev"
	commit  = "unknown"
	date    = "unknown"
)

// isFlagExplicitlySet checks if a flag was explicitly set on the command line
// This is more robust than string matching as it handles edge cases like
// flag values containing the flag name
func isFlagExplicitlySet(flagName string) bool {
	for _, arg := range os.Args {
		// Check for --flag format
		if arg == flagName {
			return true
		}
		// Check for --flag=value format
		if strings.HasPrefix(arg, flagName+"=") {
			return true
		}
		// Check for short flags (if applicable)
		// Note: This implementation assumes long flags, extend as needed for short flags
		if strings.HasPrefix(arg, "-") && !strings.HasPrefix(arg, "--") {
			// This is a short flag or combined short flags
			// For now, we only handle long flags to avoid complexity
			continue
		}
	}
	return false
}

func main() {
	var cli CLI

	ctx := kong.Parse(&cli,
		kong.Name("tdcli"),
		kong.Description("Treasure Data CLI Tool"),
		kong.UsageOnError(),
		kong.ConfigureHelp(kong.HelpOptions{
			Compact: true,
		}),
		kong.Vars{
			"version": version,
		},
	)

	// Load configuration from files after parsing (so flags can override config)
	config, err := LoadConfig()
	if err != nil {
		// Continue with defaults if config loading fails
		config = DefaultConfig()
	}

	// Apply config values if not overridden by flags/env
	// Check if values were explicitly set via command line flags or environment
	regionExplicitlySet := isFlagExplicitlySet("--region") || os.Getenv("TD_REGION") != ""
	formatExplicitlySet := isFlagExplicitlySet("--format") || os.Getenv("TD_FORMAT") != ""
	outputExplicitlySet := isFlagExplicitlySet("--output") || os.Getenv("TD_OUTPUT") != ""
	sslExplicitlySet := isFlagExplicitlySet("--insecure-skip-verify") ||
		isFlagExplicitlySet("--cert-file") ||
		isFlagExplicitlySet("--key-file") ||
		isFlagExplicitlySet("--ca-file") ||
		os.Getenv("TD_INSECURE_SKIP_VERIFY") != "" ||
		os.Getenv("TD_CERT_FILE") != "" ||
		os.Getenv("TD_KEY_FILE") != "" ||
		os.Getenv("TD_CA_FILE") != ""

	// Get command for validation
	command := ctx.Command()

	if cli.APIKey == "" && config.APIKey != "" {
		cli.APIKey = config.APIKey
	}
	if !regionExplicitlySet && config.Region != "" {
		cli.Region = config.Region
	}
	if !formatExplicitlySet && config.Format != "" {
		cli.Format = config.Format
	}
	if !outputExplicitlySet && config.Output != "" {
		cli.Output = config.Output
	}
	// Apply SSL config values if not explicitly set via flags/env
	if !sslExplicitlySet {
		if config.InsecureSkipVerify && !cli.InsecureSkipVerify {
			cli.InsecureSkipVerify = config.InsecureSkipVerify
		}
		if cli.CertFile == "" && config.CertFile != "" {
			cli.CertFile = config.CertFile
		}
		if cli.KeyFile == "" && config.KeyFile != "" {
			cli.KeyFile = config.KeyFile
		}
		if cli.CAFile == "" && config.CAFile != "" {
			cli.CAFile = config.CAFile
		}
	}

	// Validate API key for non-version and non-config commands
	if command != "version" && !strings.HasPrefix(command, "config") {
		if cli.APIKey == "" {
			fmt.Println("Error: API key required.")
			fmt.Println("Set it via:")
			fmt.Println("  - Configuration file: tdcli config set api_key YOUR_KEY")
			fmt.Println("  - Environment variable: TD_API_KEY")
			fmt.Println("  - Command flag: --api-key YOUR_KEY")
			fmt.Println("Format: account_id/api_key")
			os.Exit(1)
		}

		// Validate API key format
		if !isValidAPIKey(cli.APIKey) {
			log.Fatal("Invalid API key format. Expected format: account_id/api_key")
		}
	}

	// Create client if API key is provided
	var client *td.Client
	if cli.APIKey != "" {
		var err error
		// Create client with region configuration
		clientOptions := []td.ClientOption{}
		if cli.Region != "" {
			clientOptions = append(clientOptions, td.WithRegion(cli.Region))
		}

		// Apply SSL options if any are configured
		if cli.InsecureSkipVerify || cli.CertFile != "" || cli.KeyFile != "" || cli.CAFile != "" {
			sslOptions := td.SSLOptions{
				InsecureSkipVerify: cli.InsecureSkipVerify,
				CertFile:           cli.CertFile,
				KeyFile:            cli.KeyFile,
				CAFile:             cli.CAFile,
			}
			clientOptions = append(clientOptions, td.WithSSLOptions(sslOptions))
		}

		client, err = td.NewClient(cli.APIKey, clientOptions...)
		if err != nil {
			log.Fatalf("Failed to create client: %v", err)
		}
	}

	// Create CLI context
	cliContext := &CLIContext{
		Context:     context.Background(),
		Client:      client,
		GlobalFlags: cli.ToFlags(),
	}

	// Execute the command
	err = ctx.Run(cliContext)
	if err != nil {
		handleError(err, "Command failed", cli.Verbose)
	}
}

func isValidAPIKey(apiKey string) bool {
	// TD API keys should be in format: account_id/api_key
	// Basic validation: contains exactly one slash and has content on both sides
	parts := strings.Split(apiKey, "/")
	return len(parts) == 2 && len(parts[0]) > 0 && len(parts[1]) > 0
}

func handleError(err error, message string, verbose bool) {
	if err != nil {
		if verbose {
			log.Fatalf("%s: %v", message, err)
		} else {
			fmt.Printf("Error: %s\n", err.Error())
			os.Exit(1)
		}
	}
}
