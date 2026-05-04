package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"

	td "github.com/mickeey2525/treasuredata-go-sdk"
)

func handleLLMActionList(ctx context.Context, client *td.Client, flags Flags) {
	resp, err := client.LLM.ListActions(ctx)
	if err != nil {
		handleError(err, "Failed to list LLM actions", flags.Verbose)
		return
	}

	if flags.Format == "json" {
		printJSON(resp)
		return
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "ID\tTYPE\tINTEGRATION ID\tPROMPT ID\tUI TAGS")
	for _, action := range resp.Data {
		tagStr := strings.Join(action.UITags, ", ")
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n", action.ID, action.Type, action.IntegrationID, action.PromptID, tagStr)
	}
	w.Flush()
}

func handleLLMActionGet(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 1 {
		fmt.Println("Usage: tdcli llm actions get <action-id>")
		return
	}

	actionID := args[0]
	resp, err := client.LLM.GetAction(ctx, actionID)
	if err != nil {
		handleError(err, "Failed to get LLM action", flags.Verbose)
		return
	}

	if flags.Format == "json" {
		printJSON(resp)
		return
	}

	action := resp.Data
	fmt.Printf("ID:          %s\n", action.ID)
	fmt.Printf("Type:        %s\n", action.Type)
	fmt.Printf("Integration: %s\n", action.IntegrationID)
	fmt.Printf("Prompt:      %s\n", action.PromptID)
	fmt.Printf("Webhook URL: %s\n", action.WebhookTextURL)
	if len(action.UITags) > 0 {
		fmt.Printf("UI Tags:     %s\n", strings.Join(action.UITags, ", "))
	}
}

func handleLLMActionExecute(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 2 {
		fmt.Println("Usage: tdcli llm actions execute <action-id> <json-input>")
		return
	}

	actionID := args[0]
	inputStr := args[1]

	var input map[string]interface{}
	if err := json.Unmarshal([]byte(inputStr), &input); err != nil {
		handleError(err, "Failed to parse input JSON", flags.Verbose)
		return
	}

	resp, err := client.LLM.ExecuteAction(ctx, actionID, input)
	if err != nil {
		handleError(err, "Failed to execute LLM action", flags.Verbose)
		return
	}

	printJSON(resp)
}

func handleLLMIntegrationList(ctx context.Context, client *td.Client, flags Flags) {
	resp, err := client.LLM.ListIntegrations(ctx)
	if err != nil {
		handleError(err, "Failed to list LLM integrations", flags.Verbose)
		return
	}

	if flags.Format == "json" {
		printJSON(resp)
		return
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "ID\tNAME\tTYPE\tSTATUS")
	for _, integration := range resp.Data {
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\n", integration.ID, integration.Name, integration.Type, integration.Status)
	}
	w.Flush()
}

func handleLLMPromptList(ctx context.Context, client *td.Client, flags Flags) {
	resp, err := client.LLM.ListPrompts(ctx)
	if err != nil {
		handleError(err, "Failed to list LLM prompts", flags.Verbose)
		return
	}

	if flags.Format == "json" {
		printJSON(resp)
		return
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "ID\tNAME\tSTATUS\tVARIABLES")
	for _, prompt := range resp.Data {
		varsStr := strings.Join(prompt.Variables, ", ")
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\n", prompt.ID, prompt.Name, prompt.Status, varsStr)
	}
	w.Flush()
}

func handleLLMProjectList(ctx context.Context, client *td.Client, flags Flags) {
	resp, err := client.LLM.ListProjects(ctx)
	if err != nil {
		handleError(err, "Failed to list LLM projects", flags.Verbose)
		return
	}

	if flags.Format == "json" {
		printJSON(resp)
		return
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "ID\tNAME\tSTATUS")
	for _, project := range resp.Data {
		fmt.Fprintf(w, "%s\t%s\t%s\n", project.ID, project.Name, project.Status)
	}
	w.Flush()
}

func handleLLMProjectGet(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) < 1 {
		fmt.Println("Usage: tdcli llm projects get <project-id>")
		return
	}

	projectID := args[0]
	project, err := client.LLM.GetProject(ctx, projectID)
	if err != nil {
		handleError(err, "Failed to get LLM project", flags.Verbose)
		return
	}

	if flags.Format == "json" {
		printJSON(project)
		return
	}

	fmt.Printf("ID:          %s\n", project.ID)
	fmt.Printf("Name:        %s\n", project.Name)
	fmt.Printf("Status:      %s\n", project.Status)
	if project.Description != "" {
		fmt.Printf("Description: %s\n", project.Description)
	}
}
