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
- **TablesService**: Table management including swap, rename, partial delete
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