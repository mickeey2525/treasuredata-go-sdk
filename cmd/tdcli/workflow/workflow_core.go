package workflow

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"text/tabwriter"

	td "github.com/mickeey2525/treasuredata-go-sdk"
)

// Workflow handlers
func HandleWorkflowList(ctx context.Context, client *td.Client, flags Flags) {
	opts := &td.WorkflowListOptions{
		Limit:  100,
		Offset: 0,
	}

	resp, err := client.Workflow.ListWorkflows(ctx, opts)
	if err != nil {
		HandleError(err, "Failed to list workflows", flags.Verbose)
	}

	switch flags.Format {
	case "json":
		PrintJSON(resp)
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
			return
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
}

func HandleWorkflowGet(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 1 {
		log.Fatal("Workflow ID required")
	}

	workflowID := args[0]

	workflow, err := client.Workflow.GetWorkflow(ctx, workflowID)
	if err != nil {
		HandleError(err, "Failed to get workflow", flags.Verbose)
	}

	switch flags.Format {
	case "json":
		PrintJSON(workflow)
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
}

func HandleWorkflowInit(ctx context.Context, args []string, flags Flags) {
	if len(args) < 1 {
		log.Fatal("Project name required")
	}
	projectName := args[0]

	// Create project directory
	if err := os.Mkdir(projectName, 0755); err != nil {
		if os.IsExist(err) {
			log.Fatalf("Directory '%s' already exists", projectName)
		}
		HandleError(err, "Failed to create project directory", flags.Verbose)
	}

	// Create queries subdirectory
	queriesDir := filepath.Join(projectName, "queries")
	if err := os.Mkdir(queriesDir, 0755); err != nil {
		HandleError(err, "Failed to create queries directory", flags.Verbose)
	}

	// Create workflow.dig file
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
		HandleError(err, "Failed to create workflow.dig file", flags.Verbose)
	}

	// Create sample_query.sql file
	sampleQueryContent := `-- Sample query: select the count of records from a sample table
SELECT count(1) FROM www_access;
`
	sampleQueryPath := filepath.Join(queriesDir, "sample_query.sql")
	if err := os.WriteFile(sampleQueryPath, []byte(sampleQueryContent), 0644); err != nil {
		HandleError(err, "Failed to create sample_query.sql file", flags.Verbose)
	}

	fmt.Printf("✅ Sample workflow project '%s' created successfully.\n", projectName)
	fmt.Println("To push this project to Treasure Data, run:")
	fmt.Printf("  tdcli workflow projects push %s %s\n", projectName, projectName)
}

func HandleWorkflowCreate(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 3 {
		log.Fatal("Name, project, and config required")
	}

	workflow, err := client.Workflow.CreateWorkflow(ctx, args[0], args[1], args[2])
	if err != nil {
		HandleError(err, "Failed to create workflow", flags.Verbose)
	}

	fmt.Printf("Workflow created successfully\n")
	fmt.Printf("ID: %s\n", workflow.ID)
	fmt.Printf("Name: %s\n", workflow.Name)
}

func HandleWorkflowUpdate(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 2 {
		log.Fatal("Workflow ID and updates (key=value) required")
	}

	workflowID := args[0]

	updates := make(map[string]string)
	// Parse key=value pairs
	for _, arg := range args[1:] {
		parts := strings.SplitN(arg, "=", 2)
		if len(parts) != 2 {
			log.Fatalf("Invalid update format: %s (expected key=value)", arg)
		}
		updates[parts[0]] = parts[1]
	}

	workflow, err := client.Workflow.UpdateWorkflow(ctx, workflowID, updates)
	if err != nil {
		HandleError(err, "Failed to update workflow", flags.Verbose)
	}

	fmt.Printf("Workflow %s updated successfully\n", workflow.ID)
}

func HandleWorkflowDelete(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 1 {
		log.Fatal("Workflow ID required")
	}

	workflowID := args[0]

	err := client.Workflow.DeleteWorkflow(ctx, workflowID)
	if err != nil {
		HandleError(err, "Failed to delete workflow", flags.Verbose)
	}

	fmt.Printf("Workflow %s deleted successfully\n", workflowID)
}

func HandleWorkflowStart(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 1 {
		log.Fatal("Workflow ID required")
	}

	workflowID := args[0]

	var params map[string]interface{}
	if len(args) > 1 {
		err := json.Unmarshal([]byte(args[1]), &params)
		if err != nil {
			log.Fatalf("Invalid parameters JSON: %v", err)
		}
	}

	attempt, err := client.Workflow.StartWorkflow(ctx, workflowID, params)
	if err != nil {
		HandleError(err, "Failed to start workflow", flags.Verbose)
	}

	fmt.Printf("Workflow started successfully\n")
	fmt.Printf("Attempt ID: %s\n", attempt.ID)
	fmt.Printf("Status: %s\n", attempt.Status)
}
