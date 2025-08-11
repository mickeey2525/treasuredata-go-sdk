# Treasure Data Go SDK

A comprehensive Go client library for interacting with the Treasure Data REST API, including support for databases, queries, jobs, CDP (Customer Data Platform), workflows, and more.

## Table of Contents

- [Installation](#installation)
  - [Go SDK](#go-sdk)
  - [CLI Tool (tdcli)](#cli-tool-tdcli)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
  - [Client Options](#client-options)
  - [Available Regions](#available-regions)
- [Usage Examples](#usage-examples)
  - [Database Operations](#database-operations)
  - [Table Operations](#table-operations)
  - [Query Execution](#query-execution)
  - [Job Management](#job-management)
  - [Retrieving Query Results](#retrieving-query-results)
  - [User Management](#user-management)
  - [Permission Management](#permission-management)
  - [Bulk Import](#bulk-import)
  - [Customer Data Platform (CDP)](#customer-data-platform-cdp)
  - [Workflow Management](#workflow-management)
- [Error Handling](#error-handling)
- [Advanced Usage](#advanced-usage)
- [Contributing](#contributing)
- [License](#license)

## Installation

### Go SDK

```bash
go get github.com/treasuredata/treasuredata-go-sdk
```

### CLI Tool (tdcli)

You can also install the command-line interface tool for direct API access:

```bash
go install github.com/treasuredata/treasuredata-go-sdk/cmd/tdcli@latest
```

The CLI provides convenient access to all Treasure Data API operations:

```bash
# Configure your API key
export TD_API_KEY="your_account_id/your_api_key"

# Basic operations
tdcli databases list
tdcli tables list --database my_db
tdcli queries submit --database my_db --query "SELECT COUNT(*) FROM my_table"

# Job management
tdcli jobs list --status running
tdcli jobs get 12345
tdcli jobs cancel 12345

# User and permission management
tdcli users list
tdcli perms policies list

# Bulk import operations
tdcli import create session_name my_db my_table
tdcli import upload session_name part1 data.json

# CDP operations
tdcli cdp audiences list
tdcli cdp segments list --audience-id 123
tdcli cdp activations list --audience-id 123

# Workflow management
tdcli workflow list --project-id 123
tdcli workflow start --project-id 123 workflow_name
tdcli workflow attempts list --project-id 123 workflow_name

# Get help for any command
tdcli --help
tdcli databases --help
tdcli cdp --help
tdcli workflow --help
```

#### CLI Global Flags

- `--api-key STRING`: Treasure Data API key (format: account_id/api_key) ($TD_API_KEY)
- `--region STRING`: API region (us, eu, tokyo, ap02) [default: "us"]
- `--format STRING`: Output format (json, table, csv) [default: "table"]
- `--output STRING`: Output to file
- `-v, --verbose`: Verbose output

#### CLI Command Structure

The CLI follows a hierarchical command structure:

- **databases (db)**: Database management (list, create, get, delete, update)
- **tables (table)**: Table management (list, create, get, delete, swap, rename)
- **queries (query, q)**: Query execution (submit, status, result, list, cancel)
- **jobs (job)**: Job management (list, get, cancel)
- **users (user)**: User management (list, get)
- **perms (permissions, acl)**: Access control and permissions
- **import (bulk-import)**: Bulk data import operations
- **cdp**: Customer Data Platform operations
  - **segments**: Segment management
  - **audiences**: Audience management
  - **activations**: Activation management
  - **folders**: Folder management
  - **tokens**: Token management
- **workflow (wf)**: Workflow automation
  - **attempts**: Attempt management
  - **schedule**: Schedule management
  - **tasks**: Task management
  - **logs**: Log management
  - **projects**: Project management

For more CLI usage examples, see the [CLI documentation](cmd/tdcli/README.md).

## Quick Start

```go
package main

import (
    "context"
    "fmt"
    "log"
    
    td "github.com/treasuredata/treasuredata-go-sdk"
)

func main() {
    // Create a new client
    client, err := td.NewClient("YOUR_API_KEY")
    if err != nil {
        log.Fatal(err)
    }
    
    // Create a new client with region
    client, err = td.NewClient("YOUR_API_KEY", td.WithRegion("jp"))
    if err != nil {
        log.Fatal(err)
    }
    
    ctx := context.Background()
    
    // List databases
    databases, err := client.Databases.List(ctx)
    if err != nil {
        log.Fatal(err)
    }
    
    for _, db := range databases {
        fmt.Printf("Database: %s\n", db.Name)
    }
}
```

## Configuration

### Client Options

The client can be configured with various options:

```go
// Use a specific region
client, _ := td.NewClient("YOUR_API_KEY", td.WithRegion("jp"))

// Use a custom endpoint
client, _ := td.NewClient("YOUR_API_KEY", td.WithEndpoint("https://api.treasuredata.co.jp"))

// Use a custom HTTP client
httpClient := &http.Client{
    Timeout: 60 * time.Second,
}
client, _ := td.NewClient("YOUR_API_KEY", td.WithHTTPClient(httpClient))

// Set a custom user agent
client, _ := td.NewClient("YOUR_API_KEY", td.WithUserAgent("myapp/1.0"))
```

### Available Regions

- `us` - US region (api.treasuredata.com)
- `eu` - EU region (api.treasuredata.eu)
- `jp` / `tokyo` - Japan region (api.treasuredata.co.jp)
- `ap` - Asia Pacific region (api.ap-northeast-1.treasuredata.com)

## Usage Examples

### Database Operations

```go
// List all databases
databases, err := client.Databases.List(ctx)

// Get a specific database
db, err := client.Databases.Get(ctx, "my_database")

// Create a new database
db, err := client.Databases.Create(ctx, "new_database")

// Delete a database
err := client.Databases.Delete(ctx, "old_database")
```

### Table Operations

```go
// List tables in a database
tables, err := client.Tables.List(ctx, "my_database")

// Get a specific table
table, err := client.Tables.Get(ctx, "my_database", "my_table")

// Create a new table
resp, err := client.Tables.Create(ctx, "my_database", "new_table", "log")

// Delete a table
err := client.Tables.Delete(ctx, "my_database", "old_table")

// Swap table contents
err := client.Tables.Swap(ctx, "my_database", "table1", "table2")

// Rename a table
err := client.Tables.Rename(ctx, "my_database", "old_name", "new_name")

```

### Query Execution

```go
// Submit a Trino query
opts := &td.IssueQueryOptions{
    Query:      "SELECT COUNT(*) FROM my_table",
    Priority:   0,
    RetryLimit: 1,
}
resp, err := client.Queries.Issue(ctx, td.QueryTypeTrino, "my_database", opts)

// Submit a Hive query
resp, err := client.Queries.Issue(ctx, td.QueryTypeHive, "my_database", opts)

// Submit with idempotency key
opts.DomainKey = "unique-key-123"
resp, err := client.Queries.Issue(ctx, td.QueryTypeTrino, "my_database", opts)
```

### Job Management

```go
// List jobs
listOpts := &td.JobListOptions{
    From:   1,
    To:     100,
    Status: "success",
}
jobList, err := client.Jobs.List(ctx, listOpts)

// Get job details
job, err := client.Jobs.Get(ctx, "12345")

// Check job status
status, err := client.Jobs.Status(ctx, "12345")

// Check job status by domain key
status, err := client.Jobs.StatusByDomainKey(ctx, "unique-key-123")

// Kill a running job
err := client.Jobs.Kill(ctx, "12345")

// Export job results
exportOpts := &td.ResultExportOptions{
    Result: "td://my_database/result_table",
}
job, err := client.Jobs.ResultExport(ctx, "12345", exportOpts)
```

### Retrieving Query Results

```go
// Get results as raw data
opts := &td.GetResultOptions{
    Format: td.ResultFormatJSON,
}
reader, err := client.Results.GetResult(ctx, "12345", opts)
defer reader.Close()

// Get results as JSON
var results []map[string]interface{}
err := client.Results.GetResultJSON(ctx, "12345", &results)

// Get results as JSONL (streaming)
scanner, err := client.Results.GetResultJSONL(ctx, "12345")
defer scanner.Close()

for scanner.Scan() {
    var record map[string]interface{}
    if err := scanner.Decode(&record); err != nil {
        log.Printf("Error decoding: %v", err)
        continue
    }
    fmt.Printf("Record: %v\n", record)
}

if err := scanner.Err(); err != nil {
    log.Fatal(err)
}
```

### User Management

```go
// List all users
users, err := client.Users.List(ctx)

// Get a specific user
user, err := client.Users.Get(ctx, "user@example.com")

// Create a new user
createOpts := &td.CreateUserOptions{
    Email:    "newuser@example.com",
    Password: "secure_password",
    Name:     "New User",
}
user, err := client.Users.Create(ctx, createOpts)

// Delete a user
err := client.Users.Delete(ctx, "user@example.com")

// Manage API keys
keys, err := client.Users.ListAPIKeys(ctx, "user@example.com")
key, err := client.Users.AddAPIKey(ctx, "user@example.com")
err := client.Users.RemoveAPIKey(ctx, "user@example.com", "API_KEY")
```

### Permission Management

```go
// List policies
policies, err := client.Permissions.ListPolicies(ctx)

// Get a specific policy
policy, err := client.Permissions.GetPolicy(ctx, 123)

// Create a policy
createOpts := &td.CreatePolicyOptions{
    Name:        "ReadOnlyPolicy",
    Description: "Read-only access policy",
}
policy, err := client.Permissions.CreatePolicy(ctx, createOpts)

// Update a policy
updateOpts := &td.UpdatePolicyOptions{
    Description: "Updated description",
}
policy, err := client.Permissions.UpdatePolicy(ctx, 123, updateOpts)

// Delete a policy
err := client.Permissions.DeletePolicy(ctx, 123)

// Manage user policies
policies, err := client.Permissions.ListUserPolicies(ctx, 456)
policies, err := client.Permissions.UpdateUserPolicies(ctx, 456, []int{123, 124})
policy, err := client.Permissions.AttachUserToPolicy(ctx, 456, 123)
err := client.Permissions.DetachUserFromPolicy(ctx, 456, 123)
```

### Bulk Import

```go
// Create a bulk import session
err := client.BulkImport.Create(ctx, "import_session", "my_database", "my_table")

// Upload data
data := bytes.NewReader([]byte("your data here"))
err := client.BulkImport.UploadPart(ctx, "import_session", "part1", data)

// List parts
parts, err := client.BulkImport.ListParts(ctx, "import_session")

// Freeze the session
err := client.BulkImport.Freeze(ctx, "import_session")

// Commit the session
err := client.BulkImport.Commit(ctx, "import_session")

// Perform the import
job, err := client.BulkImport.Perform(ctx, "import_session")

// Check session status
session, err := client.BulkImport.Show(ctx, "import_session")

// Delete a session
err := client.BulkImport.Delete(ctx, "import_session")
```

### Customer Data Platform (CDP)

The SDK provides comprehensive CDP functionality including segments, audiences, activations, journeys, and more.

#### Segment Management

```go
// List segments
segments, err := client.CDP.ListSegments(ctx, "audience_id")

// Create a segment
createOpts := &td.CreateSegmentOptions{
    Name:        "High Value Customers",
    Description: "Customers with high purchase value",
    SQL:         "SELECT customer_id FROM customers WHERE total_spent > 1000",
}
segment, err := client.CDP.CreateSegment(ctx, "audience_id", createOpts)

// Get segment details
segment, err := client.CDP.GetSegment(ctx, "audience_id", "segment_id")

// Query segment data
queryOpts := &td.SegmentQueryOptions{
    Query: "SELECT * FROM segment_customers LIMIT 100",
}
job, err := client.CDP.QuerySegment(ctx, "audience_id", "segment_id", queryOpts)

// Get segment statistics
stats, err := client.CDP.GetSegmentStatistics(ctx, "audience_id", "segment_id")
```

#### Audience Management

```go
// List audiences
audiences, err := client.CDP.ListAudiences(ctx)

// Create an audience
createOpts := &td.CreateAudienceOptions{
    Name:        "Marketing Audience",
    Description: "Audience for marketing campaigns",
}
audience, err := client.CDP.CreateAudience(ctx, createOpts)

// Get audience details
audience, err := client.CDP.GetAudience(ctx, "audience_id")

// Get audience behaviors
behaviors, err := client.CDP.GetAudienceBehaviors(ctx, "audience_id")

// Run audience execution
execution, err := client.CDP.RunAudienceExecution(ctx, "audience_id")

// Get execution history
executions, err := client.CDP.GetAudienceExecutions(ctx, "audience_id")
```

#### Activation Management

```go
// List activations
activations, err := client.CDP.ListActivations(ctx, "audience_id")

// Create an activation
createOpts := &td.CreateActivationOptions{
    Name:           "Email Campaign",
    Description:    "Send email to high-value customers",
    DestinationType: "email",
    Configuration:  map[string]interface{}{"template_id": "123"},
}
activation, err := client.CDP.CreateActivation(ctx, "audience_id", "segment_id", createOpts)

// Execute an activation
err := client.CDP.ExecuteActivation(ctx, "audience_id", "segment_id", "activation_id")

// Get activation executions
executions, err := client.CDP.GetActivationExecutions(ctx, "audience_id", "segment_id", "activation_id")
```

#### Journey Management

```go
// List journeys
journeys, err := client.CDP.ListJourneys(ctx, "audience_id")

// Create a journey
createOpts := &td.CreateJourneyOptions{
    Name:        "Onboarding Journey",
    Description: "Customer onboarding flow",
    Steps:       []td.JourneyStep{...},
}
journey, err := client.CDP.CreateJourney(ctx, "audience_id", createOpts)

// Start/pause/resume journey
err := client.CDP.PauseJourney(ctx, "audience_id", "journey_id")
err := client.CDP.ResumeJourney(ctx, "audience_id", "journey_id")

// Get journey statistics
stats, err := client.CDP.GetJourneyStatistics(ctx, "audience_id", "journey_id")

// Get journey customers
customers, err := client.CDP.GetJourneyCustomers(ctx, "audience_id", "journey_id")
```

### Workflow Management

The SDK provides comprehensive workflow automation capabilities.

#### Workflow Operations

```go
// List workflows
workflows, err := client.Workflow.List(ctx, "project_id", nil)

// Create a workflow
createOpts := &td.CreateWorkflowOptions{
    Name:     "data-pipeline",
    TimeZone: "UTC",
    Schedule: &td.WorkflowSchedule{
        Cron: "0 1 * * *", // Daily at 1 AM
    },
}
workflow, err := client.Workflow.Create(ctx, "project_id", createOpts)

// Get workflow details
workflow, err := client.Workflow.Get(ctx, "project_id", "workflow_name")

// Start workflow execution
attempt, err := client.Workflow.Start(ctx, "project_id", "workflow_name", nil)

// Update workflow
updateOpts := &td.UpdateWorkflowOptions{
    Schedule: &td.WorkflowSchedule{
        Cron: "0 2 * * *", // Change to 2 AM
    },
}
workflow, err := client.Workflow.Update(ctx, "project_id", "workflow_name", updateOpts)
```

#### Workflow Attempts and Monitoring

```go
// List workflow attempts
attempts, err := client.Workflow.ListAttempts(ctx, "project_id", "workflow_name", nil)

// Get attempt details
attempt, err := client.Workflow.GetAttempt(ctx, "project_id", "workflow_name", "attempt_id")

// Kill running attempt
err := client.Workflow.KillAttempt(ctx, "project_id", "workflow_name", "attempt_id")

// Retry failed attempt
attempt, err := client.Workflow.RetryAttempt(ctx, "project_id", "workflow_name", "attempt_id", nil)

// Get workflow tasks
tasks, err := client.Workflow.ListTasks(ctx, "project_id", "workflow_name", "attempt_id")

// Get task details
task, err := client.Workflow.GetTask(ctx, "project_id", "workflow_name", "attempt_id", "task_name")
```

#### Workflow Schedules

```go
// Get workflow schedule
schedule, err := client.Workflow.GetSchedule(ctx, "project_id", "workflow_name")

// Enable workflow schedule
schedule, err := client.Workflow.EnableSchedule(ctx, "project_id", "workflow_name")

// Disable workflow schedule
schedule, err := client.Workflow.DisableSchedule(ctx, "project_id", "workflow_name")

// Update schedule
updateOpts := &td.UpdateScheduleOptions{
    Cron: "0 3 * * *", // Change to 3 AM
}
schedule, err := client.Workflow.UpdateSchedule(ctx, "project_id", "workflow_name", updateOpts)
```

#### Workflow Projects

```go
// List projects
projects, err := client.Workflow.ListProjects(ctx, nil)

// Create a project
createOpts := &td.CreateProjectOptions{
    Name: "my-data-pipeline",
}
project, err := client.Workflow.CreateProject(ctx, createOpts)

// Get project details
project, err := client.Workflow.GetProject(ctx, "project_id")

// Push project from directory
archive := &td.WorkflowArchive{
    Files: map[string][]byte{
        "workflow.dig": []byte("timezone: UTC\n+task1:\n  sh>: echo 'Hello World'"),
    },
}
revision, err := client.Workflow.PushProject(ctx, "project_id", archive)
```

#### Project Secrets Management

```go
// List project secrets
secrets, err := client.Workflow.ListSecrets(ctx, "project_id")

// Set a secret
err := client.Workflow.SetSecret(ctx, "project_id", "API_KEY", "secret_value")

// Delete a secret
err := client.Workflow.DeleteSecret(ctx, "project_id", "API_KEY")
```

## Features Overview

This SDK provides comprehensive coverage of the Treasure Data platform:

### Core Data Platform
- **Database Management**: Create, list, get, update, and delete databases
- **Table Operations**: Manage tables including CRUD operations, swapping, and renaming
- **Query Engine**: Execute Trino (Presto) and Hive queries with full job lifecycle management
- **Job Management**: Monitor, control, and export query results
- **Bulk Data Import**: High-performance data ingestion with session management

### User & Access Control
- **User Management**: Complete user lifecycle and API key management
- **Permission System**: Policy-based access control with groups and user assignments

### Customer Data Platform (CDP)
- **Audience Management**: Create and manage customer audiences
- **Segment Operations**: Build customer segments with SQL queries and analytics
- **Journey Orchestration**: Design and execute customer journey workflows
- **Activation Engine**: Connect audiences to external destinations (email, ads, etc.)
- **Folder Organization**: Organize segments and audiences in hierarchical folders
- **Token Management**: Secure API access with entity-specific tokens
- **Funnel Analytics**: Track conversion funnels and customer behavior
- **Predictive Segments**: AI-powered customer segmentation

### Workflow Automation
- **Workflow Management**: Create, update, and execute data processing workflows
- **Schedule Management**: Cron-based workflow scheduling with timezone support
- **Project Organization**: Organize workflows in projects with version control
- **Attempt Monitoring**: Track workflow executions, retry failed runs, and kill active runs
- **Task Management**: Monitor individual workflow tasks and their logs
- **Secrets Management**: Secure storage and management of workflow secrets
- **Archive Handling**: Upload and manage workflow project archives

### CLI Tool (tdcli)
- **Complete API Coverage**: All SDK features accessible via command line
- **Multiple Output Formats**: JSON, table, and CSV output options
- **Regional Support**: Connect to different Treasure Data regions
- **Batch Operations**: Efficient bulk operations for common tasks

## Error Handling

The SDK provides detailed error information:

```go
databases, err := client.Databases.List(ctx)
if err != nil {
    if tdErr, ok := err.(*td.ErrorResponse); ok {
        fmt.Printf("API Error: %s\n", tdErr.Message)
        fmt.Printf("Status Code: %d\n", tdErr.Response.StatusCode)
    } else {
        fmt.Printf("Error: %v\n", err)
    }
}
```

## Advanced Usage

### Custom HTTP Client

```go
import (
    "net/http"
    "time"
)

httpClient := &http.Client{
    Timeout: 60 * time.Second,
    Transport: &http.Transport{
        MaxIdleConns:        100,
        MaxIdleConnsPerHost: 10,
    },
}

client, err := td.NewClient("YOUR_API_KEY", td.WithHTTPClient(httpClient))
```

### Context with Timeout

```go
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

databases, err := client.Databases.List(ctx)
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This SDK is distributed under the Apache License, Version 2.0. See LICENSE for more information.
