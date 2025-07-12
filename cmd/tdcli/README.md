# tdcli - Treasure Data CLI Tool

A command-line interface for the Treasure Data Go SDK.

## Installation

```bash
cd cmd/tdcli
go build -o tdcli .
```

## Authentication

Set your Treasure Data API key using the environment variable:

```bash
export TD_API_KEY="account_id/api_key"
```

Or use the `--api-key` flag with commands.

## Usage

### Database Management
```bash
# List databases
tdcli db list

# Show database details  
tdcli db show my_database

# Create a database
tdcli db create new_database

# Delete a database
tdcli db delete old_database
```

### Table Management
```bash
# List tables in a database
tdcli table list my_database

# Show table details
tdcli table show my_database my_table

# Create a table
tdcli table create my_database new_table

# Delete a table
tdcli table delete my_database old_table

# Swap tables
tdcli table swap my_database table1 table2

# Rename a table
tdcli table rename my_database old_name new_name
```

### Query Execution
```bash
# Submit a query
tdcli query submit "SELECT COUNT(*) FROM my_table" --database my_db

# Check job status
tdcli query status 12345

# Get query results
tdcli query result 12345 --format csv

# List recent queries
tdcli query list

# Cancel a running query
tdcli query cancel 12345
```

### Job Management
```bash
# List jobs
tdcli job list

# Show job details
tdcli job show 12345

# Cancel a job
tdcli job cancel 12345
```

### Access Control and Permissions
```bash
# List policies
tdcli perms policies list

# Create a policy
tdcli perms policies create "My Policy"

# List policy groups
tdcli perms groups list

# List access control users
tdcli perms users list
```

### Results Management
```bash
# Get query results
tdcli result get 12345 --format json --limit 100
```

### Bulk Import Management
```bash
# List bulk import sessions
tdcli import list

# Create a bulk import session
tdcli import create my_session my_database my_table

# Upload data parts
tdcli import upload my_session part1 data.csv

# List parts in a session
tdcli import parts my_session

# Commit the session
tdcli import commit my_session

# Perform the bulk import
tdcli import perform my_session

# Show session details
tdcli import show my_session

# Freeze/unfreeze sessions
tdcli import freeze my_session
tdcli import unfreeze my_session

# Delete a session
tdcli import delete my_session
```

## Output Formats

Most commands support multiple output formats:
- `table` (default) - Human-readable table format
- `json` - JSON format
- `csv` - CSV format

Use the `--format` flag to specify the format.

## Help

Get help for any command:

```bash
tdcli help
tdcli db help
tdcli query help
tdcli perms policies help
```