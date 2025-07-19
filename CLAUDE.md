# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is the **Treasure Data Go SDK**, a client library for interacting with Treasure Data's REST API. Treasure Data is a cloud-based data analytics platform providing big data processing capabilities.

## Commands

### Development Commands
```bash
# Build the project
go build ./...

# Run tests
go test ./...

# Run tests with verbose output
go test -v ./...

# Run tests with coverage
go test -cover ./...

# Format code
go fmt ./...

# Vet code for common errors
go vet ./...

# Download dependencies
go mod download

# Tidy up go.mod
go mod tidy

# Generate documentation
go doc -all
```

### Installation and Setup
```bash
# Install as a dependency
go get github.com/treasuredata/treasuredata-go-sdk

# Run example tests
go test -run Example
```

## Architecture

### Core Structure
- **Single-package design**: All code is in the root package `treasuredata`
- **Service-oriented architecture**: Each API domain has its own service struct
- **Client-centered**: All services are accessed through the main `Client` struct
- **Context-first**: All operations accept `context.Context` as first parameter

### Key Components

#### Client (`client.go`)
- Main entry point with `NewClient(apiKey string, options ...ClientOption)`
- Handles authentication, HTTP configuration, and region-specific endpoints
- Contains all service instances (Databases, Tables, Queries, Jobs, etc.)

#### Service Pattern
Each service follows the same pattern:
- Service struct with client reference
- Methods that accept context and parameters
- Consistent error handling with `ErrorResponse` type

#### Services
- **DatabasesService**: Database CRUD operations
- **TablesService**: Table management including swap, rename
- **QueriesService**: Query submission (Presto/Hive)
- **JobsService**: Job lifecycle management and monitoring
- **ResultsService**: Query result retrieval in multiple formats
- **UsersService**: User management and API key operations
- **PermissionsService**: Policy and permission management
- **BulkImportService**: Bulk data import workflow
- **CDPService**: Customer Data Platform operations including:
  - Segment creation and management
  - Audience building and management
  - Activation configuration for external destinations
  - Folder management with JSON API format support
- **WorkflowService**: Workflow automation and orchestration including:
  - Workflow lifecycle management (create, update, delete, list)
  - Workflow execution and monitoring (start, retry, kill attempts)
  - Task management and monitoring
  - Schedule configuration and management
  - Log retrieval for workflows and tasks

### Authentication & Configuration
- Uses TD1 API key authentication
- Supports multiple regions: US, EU, Japan, Asia-Pacific
- Configurable via `ClientOption` pattern:
  - `WithRegion(region string)`
  - `WithEndpoint(endpoint string)`
  - `WithHTTPClient(client *http.Client)`
  - `WithUserAgent(userAgent string)`

### Error Handling
- Custom `ErrorResponse` type with detailed API error information
- Preserves HTTP response details for debugging
- Type assertion pattern: `if tdErr, ok := err.(*td.ErrorResponse); ok`

## Development Guidelines

### Code Style
- Follow standard Go conventions
- Use `gofmt` for formatting
- All public APIs must have documentation comments
- Use consistent naming across services

### Testing
- Example tests in `examples_test.go` serve as documentation
- Use `context.Background()` for examples
- Include error handling in all examples

### API Integration
- All API methods accept `context.Context` as first parameter
- Use options structs for optional parameters
- Return appropriate Go types (no generic interface{} unless necessary)
- Handle HTTP status codes appropriately

### Dependencies
- Minimal dependencies: only `github.com/google/go-querystring` for URL encoding
- Standard library preferred for HTTP operations
- No external testing frameworks - use Go's built-in testing

## CLI (tdcli) Structure

### Overview
The `tdcli` command-line tool provides access to all Treasure Data API operations through a structured command hierarchy using the Kong CLI framework.

### Command Hierarchy

```
tdcli
├── version                           # Show version information
├── config                            # Configuration management
├── databases (db)                    # Database management
│   ├── list (ls)                    # List all databases
│   ├── get (show)                   # Get database details
│   ├── create                       # Create a new database
│   ├── delete (rm)                  # Delete a database
│   └── update                       # Update database properties
├── tables (table)                    # Table management
│   ├── list (ls)                    # List tables in database
│   ├── get (show)                   # Get table details
│   ├── create                       # Create a new table
│   ├── delete (rm)                  # Delete a table
│   ├── swap                         # Swap two tables
│   └── rename (mv)                  # Rename a table
├── queries (query, q)                # Query execution
│   ├── submit (run)                 # Submit a query for execution
│   ├── status                       # Check query execution status
│   ├── result (results)             # Get query results
│   ├── list (ls)                    # List recent queries
│   └── cancel                       # Cancel a running query
├── jobs (job)                        # Job management
│   ├── list (ls)                    # List jobs
│   ├── get (show)                   # Get job details
│   └── cancel (kill)                # Cancel a running job
├── users (user)                      # User management
│   ├── list (ls)                    # List users
│   └── get (show)                   # Get user details
├── perms (permissions, acl)          # Access control and permissions
│   ├── policies                     # Policy management
│   │   ├── list (ls)               # List all policies
│   │   ├── get (show)              # Get policy details
│   │   ├── create                  # Create a new policy
│   │   └── delete (rm)             # Delete a policy
│   ├── groups                       # Policy group management
│   │   ├── list (ls)               # List all policy groups
│   │   ├── get (show)              # Get policy group details
│   │   ├── create                  # Create a new policy group
│   │   └── delete (rm)             # Delete a policy group
│   └── users                        # Access control user management
│       ├── list (ls)               # List access control users
│       └── get (show)              # Get user access control details
├── results (result)                  # Query results management
│   └── get (show)                   # Get query results
├── import (bulk-import)              # Bulk data import
│   ├── list (ls)                    # List bulk import sessions
│   ├── get (show)                   # Get bulk import session details
│   ├── create                       # Create a new bulk import session
│   ├── delete (rm)                  # Delete a bulk import session
│   ├── upload                       # Upload a part to session
│   ├── commit                       # Commit a bulk import session
│   ├── perform                      # Perform bulk import job
│   ├── freeze                       # Freeze a bulk import session
│   ├── unfreeze                     # Unfreeze a bulk import session
│   └── parts                        # List parts in a bulk import session
├── cdp                               # Customer Data Platform (CDP) management
│   ├── segments (segment)           # CDP segment management
│   │   ├── create                  # Create a new segment
│   │   ├── list (ls)               # List segments
│   │   ├── get (show)              # Get segment details
│   │   ├── update                  # Update segment
│   │   ├── delete (rm)             # Delete segment
│   │   ├── folders                 # Get segments in folder
│   │   ├── query                   # Execute segment query
│   │   ├── new-query               # Create new segment query
│   │   ├── query-status            # Get segment query status
│   │   ├── kill-query              # Kill segment query
│   │   ├── customers               # Get segment customers
│   │   └── statistics (stats)      # Get segment statistics
│   ├── audiences (audience)         # CDP audience management
│   │   ├── create                  # Create a new audience
│   │   ├── list (ls)               # List audiences
│   │   ├── get (show)              # Get audience details
│   │   ├── delete (rm)             # Delete audience
│   │   ├── behaviors               # Get audience behaviors
│   │   ├── run                     # Run audience execution
│   │   ├── executions              # Get audience executions history
│   │   ├── statistics (stats)      # Get audience statistics
│   │   ├── sample-values (samples) # Get audience sample values
│   │   └── behavior-samples        # Get behavior sample values
│   ├── activations (activation)     # CDP activation management
│   │   ├── create                  # Create activation
│   │   ├── create-with-struct      # Create activation with struct
│   │   ├── list (ls)               # List activations
│   │   ├── get (show)              # Get activation details
│   │   ├── update                  # Update activation
│   │   ├── update-status           # Update activation status
│   │   ├── delete (rm)             # Delete activation
│   │   ├── execute                 # Execute activation
│   │   ├── executions              # Get activation executions (requires: audience-id, segment-id, activation-id)
│   │   ├── list-by-audience        # List activations by audience
│   │   ├── list-by-segment-folder  # List activations by segment folder
│   │   ├── run-segment             # Run activation for segment
│   │   ├── list-by-parent-segment  # List activations by parent segment
│   │   ├── workflow-projects       # Get workflow projects for parent segment
│   │   ├── workflows               # Get workflows for parent segment
│   │   └── matched-activations     # Get matched activations for parent segment
│   ├── folders (folder)             # CDP folder management
│   │   ├── list (ls)               # List folders in audience
│   │   ├── create                  # Create folder in audience
│   │   ├── get (show)              # Get folder details
│   │   ├── create-entity           # Create entity folder
│   │   ├── get-entity              # Get entity folder
│   │   ├── update-entity           # Update entity folder
│   │   ├── delete-entity           # Delete entity folder
│   │   └── get-entities            # Get entities by folder
│   └── tokens (token)               # CDP token management
│       ├── list (ls)               # List tokens
│       ├── get-entity (get, show)  # Get entity token details
│       ├── update-entity           # Update entity token
│       └── delete-entity (rm)      # Delete entity token
└── workflow (wf)                     # Workflow management
    ├── list (ls)                    # List workflows
    ├── get (show)                   # Get workflow details
    ├── create                       # Create a new workflow
    ├── update                       # Update workflow
    ├── delete (rm)                  # Delete workflow
    ├── start (run)                  # Start workflow execution
    ├── attempts (attempt)           # Workflow attempt management
    │   ├── list (ls)               # List workflow attempts
    │   ├── get (show)              # Get attempt details
    │   ├── kill                    # Kill running attempt
    │   └── retry                   # Retry failed attempt
    ├── schedule                     # Workflow schedule management
    │   ├── get (show)              # Get workflow schedule
    │   ├── enable                  # Enable workflow schedule
    │   ├── disable                 # Disable workflow schedule
    │   └── update                  # Update workflow schedule
    ├── tasks (task)                 # Workflow task management
    │   ├── list (ls)               # List workflow tasks
    │   └── get (show)              # Get task details
    ├── logs (log)                   # Workflow log management
    │   ├── attempt                 # Get attempt log
    │   └── task                    # Get task log
    └── projects (project, proj)     # Workflow project management
        ├── list (ls)               # List workflow projects
        ├── get (show)              # Get project details
        ├── create                  # Create a new project
        ├── push                    # Push project from directory (alias for create)
        ├── workflows (wf)          # List workflows in project
        └── secrets (secret)        # Project secrets management
            ├── list (ls)           # List project secrets
            ├── set                 # Set project secret
            └── delete (rm)         # Delete project secret
```

### Global Flags
- `--api-key STRING`: Treasure Data API key (format: account_id/api_key) ($TD_API_KEY)
- `--region STRING`: API region (us, eu, tokyo, ap02) [default: "us"]
- `--format STRING`: Output format (json, table, csv) [default: "table"]
- `--output STRING`: Output to file
- `-v, --verbose`: Verbose output

### CLI Implementation Structure

#### Command Registration (`cmd/tdcli/cli.go`)
- Uses Kong framework for command parsing and routing
- Each command is defined as a struct with Kong tags
- Commands implement a `Run(ctx *CLIContext) error` method
- Command aliases are defined with `kong:"cmd,aliases='...'"` tags

#### Handler Functions (`cmd/tdcli/*.go`)
- Each service has its own file with handler functions
- Handlers accept: `(ctx context.Context, client *td.Client, args []string, flags Flags)`
- Support multiple output formats: table (default), JSON, CSV
- Include comprehensive error handling with verbose mode support

#### CLIContext Structure
```go
type CLIContext struct {
    Context     context.Context
    Client      *td.Client
    GlobalFlags Flags
}
```

#### Flags Structure
```go
type Flags struct {
    APIKey      string
    Region      string
    Format      string
    Output      string
    Verbose     bool
    Database    string
    Status      string
    Priority    int
    Limit       int
    WithDetails bool
}
```