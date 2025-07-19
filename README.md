# Treasure Data Go SDK

A Go client library for interacting with the Treasure Data REST API.

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

# List databases
tdcli databases list

# Submit a query
tdcli queries submit --database my_db --query "SELECT COUNT(*) FROM my_table"

# Get help for any command
tdcli --help
tdcli databases --help
```

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

// Partial delete
opts := &td.PartialDeleteOptions{
    From: 1609459200, // Unix timestamp
    To:   1609545600,
}
resp, err := client.Tables.PartialDelete(ctx, "my_database", "my_table", opts)
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
