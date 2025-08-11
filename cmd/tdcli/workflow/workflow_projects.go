package workflow

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"text/tabwriter"

	td "github.com/mickeey2525/treasuredata-go-sdk"
)

// Workflow project handlers
func HandleWorkflowProjectList(ctx context.Context, client *td.Client, flags Flags) {
	resp, err := client.Workflow.ListProjects(ctx)
	if err != nil {
		HandleError(err, "Failed to list workflow projects", flags.Verbose)
	}

	switch flags.Format {
	case "json":
		PrintJSON(resp)
	case "csv":
		fmt.Println("id,name,revision,archive_type,created_at,updated_at")
		for _, project := range resp.Projects {
			fmt.Printf("%s,%s,%s,%s,%s,%s\n",
				project.ID, project.Name, project.Revision, project.ArchiveType,
				project.CreatedAt.Time.UTC().Format("2006-01-02 15:04:05"),
				project.UpdatedAt.Time.UTC().Format("2006-01-02 15:04:05"))
		}
	default:
		if len(resp.Projects) == 0 {
			fmt.Println("No projects found")
			return
		}

		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "ID\tNAME\tREVISION\tTYPE\tCREATED")
		for _, project := range resp.Projects {
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n",
				project.ID, project.Name, project.Revision, project.ArchiveType,
				project.CreatedAt.Time.UTC().Format("2006-01-02 15:04:05"))
		}
		w.Flush()
		fmt.Printf("\nTotal: %d projects\n", len(resp.Projects))
	}
}

func HandleWorkflowProjectGet(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 1 {
		log.Fatal("Project ID or name required")
	}

	projectIdentifier := args[0]
	var project *td.WorkflowProject
	var err error

	// Try to parse as project ID first (numeric)
	if _, parseErr := strconv.Atoi(projectIdentifier); parseErr == nil {
		// It's a numeric ID
		project, err = client.Workflow.GetProject(ctx, projectIdentifier)
	} else {
		// It's not numeric, try to get by name
		project, err = client.Workflow.GetProjectByName(ctx, projectIdentifier)
	}

	if err != nil {
		HandleError(err, "Failed to get workflow project", flags.Verbose)
	}

	switch flags.Format {
	case "json":
		PrintJSON(project)
	case "csv":
		fmt.Println("id,name,revision,archive_type,archive_md5,created_at,updated_at,deleted_at")
		deletedAt := ""
		if project.DeletedAt != nil {
			deletedAt = project.DeletedAt.Time.UTC().Format("2006-01-02 15:04:05")
		}
		fmt.Printf("%s,%s,%s,%s,%s,%s,%s,%s\n",
			project.ID, project.Name, project.Revision, project.ArchiveType,
			project.ArchiveMD5,
			project.CreatedAt.Time.UTC().Format("2006-01-02 15:04:05"),
			project.UpdatedAt.Time.UTC().Format("2006-01-02 15:04:05"),
			deletedAt)
	default:
		fmt.Printf("ID: %s\n", project.ID)
		fmt.Printf("Name: %s\n", project.Name)
		fmt.Printf("Revision: %s\n", project.Revision)
		fmt.Printf("Archive Type: %s\n", project.ArchiveType)
		fmt.Printf("Archive MD5: %s\n", project.ArchiveMD5)
		fmt.Printf("Created: %s\n", project.CreatedAt.Time.UTC().Format("2006-01-02 15:04:05"))
		fmt.Printf("Updated: %s\n", project.UpdatedAt.Time.UTC().Format("2006-01-02 15:04:05"))
		if project.DeletedAt != nil {
			fmt.Printf("Deleted: %s\n", project.DeletedAt.Time.UTC().Format("2006-01-02 15:04:05"))
		}
	}
}

func HandleWorkflowProjectCreate(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 2 {
		log.Fatal("Project name and path (directory or archive file) required")
	}

	path := args[1]

	// Check if the path is a directory or file
	fileInfo, err := os.Stat(path)
	if err != nil {
		log.Fatalf("Failed to access path %s: %v", path, err)
	}

	var project *td.WorkflowProject

	if fileInfo.IsDir() {
		// Create project from directory
		fmt.Printf("Creating project from directory: %s\n", path)
		project, err = client.Workflow.CreateProjectFromDirectory(ctx, args[0], path)
	} else {
		// Create project from archive file
		fmt.Printf("Creating project from archive file: %s\n", path)
		archiveData, readErr := os.ReadFile(path)
		if readErr != nil {
			log.Fatalf("Failed to read archive file: %v", readErr)
		}
		project, err = client.Workflow.CreateProject(ctx, args[0], archiveData)
	}

	if err != nil {
		HandleError(err, "Failed to create workflow project", flags.Verbose)
	}

	fmt.Printf("Project created successfully\n")
	fmt.Printf("ID: %s\n", project.ID)
	fmt.Printf("Name: %s\n", project.Name)
	fmt.Printf("Revision: %s\n", project.Revision)
}

func HandleWorkflowProjectWorkflows(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 1 {
		log.Fatal("Project ID required")
	}

	projectID := args[0]

	resp, err := client.Workflow.ListProjectWorkflows(ctx, projectID)
	if err != nil {
		HandleError(err, "Failed to list project workflows", flags.Verbose)
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
			fmt.Println("No workflows found in this project")
			return
		}

		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "ID\tNAME\tSTATUS\tTIMEZONE")
		for _, workflow := range resp.Workflows {
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\n",
				workflow.ID, workflow.Name, workflow.Status,
				workflow.Timezone)
		}
		w.Flush()
		fmt.Printf("\nTotal: %d workflows\n", len(resp.Workflows))
	}
}

func HandleWorkflowProjectSecretsList(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 1 {
		log.Fatal("Project ID required")
	}

	projectID := args[0]

	resp, err := client.Workflow.GetProjectSecrets(ctx, projectID)
	if err != nil {
		HandleError(err, "Failed to list project secrets", flags.Verbose)
	}

	switch flags.Format {
	case "json":
		PrintJSON(resp)
	case "csv":
		fmt.Println("key,value")
		for key, value := range resp.Secrets {
			fmt.Printf("%s,%s\n", key, value)
		}
	default:
		if len(resp.Secrets) == 0 {
			fmt.Println("No secrets found in this project")
			return
		}

		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "KEY\tVALUE")
		for key, value := range resp.Secrets {
			fmt.Fprintf(w, "%s\t%s\n", key, value)
		}
		w.Flush()
		fmt.Printf("\nTotal: %d secrets\n", len(resp.Secrets))
	}
}

func HandleWorkflowProjectSecretsSet(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 3 {
		log.Fatal("Project ID, secret key, and secret value required")
	}

	projectID := args[0]

	err := client.Workflow.SetProjectSecret(ctx, projectID, args[1], args[2])
	if err != nil {
		HandleError(err, "Failed to set project secret", flags.Verbose)
	}

	fmt.Printf("Secret '%s' set successfully for project %s\n", args[1], projectID)
}

func HandleWorkflowProjectSecretsDelete(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 2 {
		log.Fatal("Project ID and secret key required")
	}

	projectID := args[0]

	err := client.Workflow.DeleteProjectSecret(ctx, projectID, args[1])
	if err != nil {
		HandleError(err, "Failed to delete project secret", flags.Verbose)
	}

	fmt.Printf("Secret '%s' deleted successfully from project %s\n", args[1], projectID)
}

// Wrapper functions for test compatibility
func handleWorkflowProjectList(ctx context.Context, client *td.Client, flags Flags) {
	HandleWorkflowProjectList(ctx, client, flags)
}

func handleWorkflowProjectGet(ctx context.Context, client *td.Client, args []string, flags Flags) {
	HandleWorkflowProjectGet(ctx, client, args, flags)
}

func handleWorkflowProjectCreate(ctx context.Context, client *td.Client, args []string, flags Flags) {
	HandleWorkflowProjectCreate(ctx, client, args, flags)
}

func handleWorkflowProjectWorkflows(ctx context.Context, client *td.Client, args []string, flags Flags) {
	HandleWorkflowProjectWorkflows(ctx, client, args, flags)
}

func handleWorkflowProjectSecretsList(ctx context.Context, client *td.Client, args []string, flags Flags) {
	HandleWorkflowProjectSecretsList(ctx, client, args, flags)
}

func handleWorkflowProjectSecretsSet(ctx context.Context, client *td.Client, args []string, flags Flags) {
	HandleWorkflowProjectSecretsSet(ctx, client, args, flags)
}

func handleWorkflowProjectSecretsDelete(ctx context.Context, client *td.Client, args []string, flags Flags) {
	HandleWorkflowProjectSecretsDelete(ctx, client, args, flags)
}

func HandleWorkflowProjectDownload(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 1 {
		log.Fatal("Project ID or name required")
	}

	projectIdentifier := args[0]
	var outputDir string

	// Determine output directory
	if len(args) >= 2 {
		outputDir = args[1]
	} else {
		// Default to project name if available, otherwise use identifier
		outputDir = projectIdentifier
	}

	// Revision support (we'll extend this with proper flag support later)
	var revision string

	if flags.Verbose {
		fmt.Printf("Downloading project: %s\n", projectIdentifier)
		if revision != "" {
			fmt.Printf("Revision: %s\n", revision)
		}
		fmt.Printf("Output directory: %s\n", outputDir)
	}

	var err error
	var projectInfo *td.WorkflowProject

	// Try to parse as project ID first (numeric)
	_, parseErr := strconv.Atoi(projectIdentifier)
	if parseErr == nil {
		// It's a numeric ID, use it directly
		if flags.Verbose {
			fmt.Printf("Using project ID: %s\n", projectIdentifier)
		}

		// Get project info for display
		projectInfo, err = client.Workflow.GetProject(ctx, projectIdentifier)
		if err != nil {
			HandleError(err, "Failed to get project details", flags.Verbose)
		}

		// Download by ID
		if revision != "" {
			err = client.Workflow.DownloadProjectToDirectoryWithRevision(ctx, projectIdentifier, revision, outputDir)
		} else {
			err = client.Workflow.DownloadProjectToDirectory(ctx, projectIdentifier, outputDir)
		}
	} else {
		// It's not numeric, try to find by name
		if flags.Verbose {
			fmt.Printf("Searching for project by name: %s\n", projectIdentifier)
		}

		// Get project by name using direct API call
		projectInfo, err = client.Workflow.GetProjectByName(ctx, projectIdentifier)
		if err != nil {
			HandleError(err, "Failed to get project by name", flags.Verbose)
		}

		if flags.Verbose {
			fmt.Printf("Found project: %s (ID: %s)\n", projectInfo.Name, projectInfo.ID)
		}

		// Use the project name for the default output directory if not specified
		if len(args) < 2 {
			outputDir = projectInfo.Name
		}

		// Download by name
		if revision != "" {
			err = client.Workflow.DownloadProjectByNameToDirectoryWithRevision(ctx, projectIdentifier, revision, outputDir)
		} else {
			err = client.Workflow.DownloadProjectByNameToDirectory(ctx, projectIdentifier, outputDir)
		}
	}

	if err != nil {
		HandleError(err, "Failed to download project", flags.Verbose)
	}

	fmt.Printf("Project downloaded successfully\n")
	if projectInfo != nil {
		fmt.Printf("Project: %s (ID: %s)\n", projectInfo.Name, projectInfo.ID)
		fmt.Printf("Revision: %s\n", projectInfo.Revision)
		fmt.Printf("Archive Type: %s\n", projectInfo.ArchiveType)
	}
	fmt.Printf("Output directory: %s\n", outputDir)

	// Show directory contents if verbose
	if flags.Verbose {
		fmt.Printf("\nExtracted files:\n")
		err := filepath.Walk(outputDir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return nil // Skip errors and continue
			}
			relPath, _ := filepath.Rel(outputDir, path)
			if relPath == "." {
				return nil
			}
			if info.IsDir() {
				fmt.Printf("  %s/\n", relPath)
			} else {
				fmt.Printf("  %s\n", relPath)
			}
			return nil
		})
		if err != nil {
			fmt.Printf("Warning: Failed to list extracted files: %v\n", err)
		}
	}
}
