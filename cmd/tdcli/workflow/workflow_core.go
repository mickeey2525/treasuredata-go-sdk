package workflow

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"text/tabwriter"

	td "github.com/mickeey2525/treasuredata-go-sdk"
)

// HandleWorkflowList lists workflows
func HandleWorkflowList(ctx context.Context, client *td.Client, flags Flags) error {
	opts := &td.WorkflowListOptions{
		Limit:  100,
		Offset: 0,
	}

	resp, err := client.Workflow.ListWorkflows(ctx, opts)
	if err != nil {
		return wrapError(err, "failed to list workflows", flags.Verbose)
	}

	switch flags.Format {
	case "json":
		return PrintJSON(resp)
	case "csv":
		fmt.Println("id,name,project,status,created_at,updated_at")
		for _, workflow := range resp.Workflows {
			createdAt := ""
			if workflow.CreatedAt != nil {
				createdAt = workflow.CreatedAt.Format("2006-01-02 15:04:05")
			}
			updatedAt := ""
			if workflow.UpdatedAt != nil {
				updatedAt = workflow.UpdatedAt.Format("2006-01-02 15:04:05")
			}
			fmt.Printf("%s,%s,%s,%s,%s,%s\n",
				workflow.ID, workflow.Name, workflow.Project.Name, workflow.Status,
				createdAt, updatedAt)
		}
	default:
		if len(resp.Workflows) == 0 {
			fmt.Println("No workflows found")
			return nil
		}

		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "ID\tNAME\tPROJECT\tSTATUS\tTIMEZONE")
		for _, workflow := range resp.Workflows {
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n",
				workflow.ID, workflow.Name, workflow.Project.Name, workflow.Status,
				workflow.Timezone)
		}
		w.Flush()
		fmt.Printf("\nTotal: %d workflows\n", len(resp.Workflows))
	}
	return nil
}

// HandleWorkflowGet retrieves a workflow
func HandleWorkflowGet(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	if len(args) < 1 {
		return usageError("Workflow ID required")
	}

	workflowID := args[0]

	workflow, err := client.Workflow.GetWorkflow(ctx, workflowID)
	if err != nil {
		return wrapError(err, "failed to get workflow", flags.Verbose)
	}

	switch flags.Format {
	case "json":
		return PrintJSON(workflow)
	case "csv":
		fmt.Println("id,name,project,status,revision,timezone,created_at,updated_at")
		createdAt := ""
		if workflow.CreatedAt != nil {
			createdAt = workflow.CreatedAt.Format("2006-01-02 15:04:05")
		}
		updatedAt := ""
		if workflow.UpdatedAt != nil {
			updatedAt = workflow.UpdatedAt.Format("2006-01-02 15:04:05")
		}
		fmt.Printf("%s,%s,%s,%s,%s,%s,%s,%s\n",
			workflow.ID, workflow.Name, workflow.Project.Name, workflow.Status,
			workflow.Revision, workflow.Timezone,
			createdAt, updatedAt)
	default:
		fmt.Printf("ID: %s\n", workflow.ID)
		fmt.Printf("Name: %s\n", workflow.Name)
		fmt.Printf("Project: %s (%s)\n", workflow.Project.Name, workflow.Project.ID)
		fmt.Printf("Status: %s\n", workflow.Status)
		fmt.Printf("Revision: %s\n", workflow.Revision)
		fmt.Printf("Timezone: %s\n", workflow.Timezone)
		if workflow.CreatedAt != nil {
			fmt.Printf("Created: %s\n", workflow.CreatedAt.Format("2006-01-02 15:04:05"))
		}
		if workflow.UpdatedAt != nil {
			fmt.Printf("Updated: %s\n", workflow.UpdatedAt.Format("2006-01-02 15:04:05"))
		}
		if workflow.LastAttempt != nil {
			fmt.Printf("Last Attempt: %d\n", *workflow.LastAttempt)
		}
		if workflow.NextSchedule != nil {
			fmt.Printf("Next Schedule: %s\n", workflow.NextSchedule.Format("2006-01-02 15:04:05"))
		}
		if len(workflow.Config) > 0 {
			fmt.Printf("\nConfig:\n%+v\n", workflow.Config)
		}
	}
	return nil
}

// HandleWorkflowInit scaffolds a workflow project on disk.
func HandleWorkflowInit(ctx context.Context, args []string, flags Flags) error {
	if len(args) < 1 {
		return usageError("Project name required")
	}
	projectName := args[0]

	if err := os.Mkdir(projectName, 0755); err != nil {
		if os.IsExist(err) {
			return fmt.Errorf("directory %q already exists", projectName)
		}
		return wrapError(err, "failed to create project directory", flags.Verbose)
	}

	queriesDir := filepath.Join(projectName, "queries")
	if err := os.Mkdir(queriesDir, 0755); err != nil {
		return wrapError(err, "failed to create queries directory", flags.Verbose)
	}

	workflowDigContent := `timezone: UTC

+setup:
  echo>: Setting up the project...

+query_and_export:
  +query:
    td>: queries/sample_query.sql
    database: sample_datasets
`
	workflowDigPath := filepath.Join(projectName, "workflow.dig")
	if err := os.WriteFile(workflowDigPath, []byte(workflowDigContent), 0644); err != nil {
		return wrapError(err, "failed to create workflow.dig file", flags.Verbose)
	}

	sampleQueryContent := `-- Sample query: select the count of records from a sample table
SELECT count(1) FROM www_access;
`
	sampleQueryPath := filepath.Join(queriesDir, "sample_query.sql")
	if err := os.WriteFile(sampleQueryPath, []byte(sampleQueryContent), 0644); err != nil {
		return wrapError(err, "failed to create sample_query.sql file", flags.Verbose)
	}

	fmt.Printf("✅ Sample workflow project '%s' created successfully.\n", projectName)
	fmt.Println("To push this project to Treasure Data, run:")
	fmt.Printf("  tdcli workflow projects push %s %s\n", projectName, projectName)
	return nil
}

// HandleWorkflowCreate creates a workflow
func HandleWorkflowCreate(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	if len(args) < 3 {
		return usageError("Name, project, and config required")
	}

	workflow, err := client.Workflow.CreateWorkflow(ctx, args[0], args[1], args[2])
	if err != nil {
		return wrapError(err, "failed to create workflow", flags.Verbose)
	}

	fmt.Printf("Workflow created successfully\n")
	fmt.Printf("ID: %s\n", workflow.ID)
	fmt.Printf("Name: %s\n", workflow.Name)
	return nil
}

// HandleWorkflowUpdate updates a workflow
func HandleWorkflowUpdate(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	if len(args) < 2 {
		return usageError("Workflow ID and updates (key=value) required")
	}

	workflowID := args[0]

	updates := make(map[string]string)
	for _, arg := range args[1:] {
		parts := strings.SplitN(arg, "=", 2)
		if len(parts) != 2 {
			return usageError(fmt.Sprintf("Invalid update format: %s (expected key=value)", arg))
		}
		updates[parts[0]] = parts[1]
	}

	workflow, err := client.Workflow.UpdateWorkflow(ctx, workflowID, updates)
	if err != nil {
		return wrapError(err, "failed to update workflow", flags.Verbose)
	}

	fmt.Printf("Workflow %s updated successfully\n", workflow.ID)
	return nil
}

// HandleWorkflowDelete deletes a workflow
func HandleWorkflowDelete(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	if len(args) < 1 {
		return usageError("Workflow ID required")
	}

	if err := client.Workflow.DeleteWorkflow(ctx, args[0]); err != nil {
		return wrapError(err, "failed to delete workflow", flags.Verbose)
	}

	fmt.Printf("Workflow %s deleted successfully\n", args[0])
	return nil
}

// HandleWorkflowStart starts a workflow
func HandleWorkflowStart(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	if len(args) < 1 {
		return usageError("Workflow ID required")
	}

	workflowID := args[0]

	var params map[string]interface{}
	if len(args) > 1 {
		if err := json.Unmarshal([]byte(args[1]), &params); err != nil {
			return usageError(fmt.Sprintf("Invalid parameters JSON: %v", err))
		}
	}

	attempt, err := client.Workflow.StartWorkflow(ctx, workflowID, params)
	if err != nil {
		return wrapError(err, "failed to start workflow", flags.Verbose)
	}

	fmt.Printf("Workflow started successfully\n")
	fmt.Printf("Attempt ID: %s\n", attempt.ID)
	fmt.Printf("Status: %s\n", attempt.Status)
	return nil
}
