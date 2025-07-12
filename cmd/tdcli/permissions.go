package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"text/tabwriter"

	td "github.com/mickeey2525/treasuredata-go-sdk"
)

func handlePermissionCommands(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) == 0 || args[0] == "help" {
		printPermissionUsage()
		return
	}

	subcommand := args[0]
	subArgs := args[1:]

	switch subcommand {
	case "policies":
		handlePolicyCommands(ctx, client, subArgs, flags)
	case "groups":
		handlePolicyGroupCommands(ctx, client, subArgs, flags)
	case "users":
		handleAccessControlUserCommands(ctx, client, subArgs, flags)
	default:
		fmt.Printf("Unknown permission subcommand: %s\n", subcommand)
		printPermissionUsage()
		os.Exit(1)
	}
}

func printPermissionUsage() {
	fmt.Printf(`Access Control and Permissions Commands

USAGE:
    tdcli permissions <subcommand> [options]
    tdcli perms <subcommand> [options]
    tdcli acl <subcommand> [options]

SUBCOMMANDS:
    policies               Policy management
    groups                 Policy group management
    users                  Access control user management

OPTIONS:
    --format FORMAT        Output format (json, table, csv)
    --verbose, -v          Verbose output

EXAMPLES:
    tdcli perms policies list
    tdcli perms policies create "My Policy"
    tdcli perms groups list
    tdcli perms users list

For detailed help on each subcommand:
    tdcli perms policies help
    tdcli perms groups help
    tdcli perms users help

`)
}

func handlePolicyCommands(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) == 0 || args[0] == "help" {
		printPolicyUsage()
		return
	}

	subcommand := args[0]
	subArgs := args[1:]

	switch subcommand {
	case "list", "ls":
		handlePolicyList(ctx, client, flags)
	case "get", "show":
		handlePolicyGet(ctx, client, subArgs, flags)
	case "create":
		handlePolicyCreate(ctx, client, subArgs, flags)
	case "delete", "rm":
		handlePolicyDelete(ctx, client, subArgs, flags)
	default:
		fmt.Printf("Unknown policy subcommand: %s\n", subcommand)
		printPolicyUsage()
		os.Exit(1)
	}
}

func printPolicyUsage() {
	fmt.Printf(`Policy Management Commands

USAGE:
    tdcli perms policies <subcommand> [options]

SUBCOMMANDS:
    list, ls               List all policies
    get, show <id>         Get policy details
    create <name>          Create a new policy
    delete, rm <id>        Delete a policy

EXAMPLES:
    tdcli perms policies list
    tdcli perms policies show 123
    tdcli perms policies create "Analytics Policy"
    tdcli perms policies delete 123

`)
}

func handlePolicyList(ctx context.Context, client *td.Client, flags Flags) {
	policies, err := client.Permissions.ListPolicies(ctx, nil)
	handleError(err, "Failed to list policies", flags.Verbose)

	switch flags.Format {
	case "json":
		printJSON(policies)
	case "csv":
		printPoliciesCSV(policies)
	default:
		printPoliciesTable(policies)
	}
}

func handlePolicyGet(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) == 0 {
		fmt.Println("Error: Policy ID required")
		fmt.Println("Usage: tdcli perms policies get <policy_id>")
		os.Exit(1)
	}

	policyIDStr := args[0]
	policyID, err := strconv.Atoi(policyIDStr)
	if err != nil {
		fmt.Printf("Error: Invalid policy ID: %s\n", policyIDStr)
		os.Exit(1)
	}

	policy, err := client.Permissions.GetPolicy(ctx, policyID)
	handleError(err, "Failed to get policy", flags.Verbose)

	switch flags.Format {
	case "json":
		printJSON(policy)
	default:
		printPolicyDetails(*policy)
	}
}

func handlePolicyCreate(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) == 0 {
		fmt.Println("Error: Policy name required")
		fmt.Println("Usage: tdcli perms policies create <name> [description]")
		os.Exit(1)
	}

	name := args[0]
	description := ""
	if len(args) > 1 {
		description = args[1]
	}

	policy, err := client.Permissions.CreatePolicy(ctx, name, description)
	handleError(err, "Failed to create policy", flags.Verbose)

	fmt.Printf("Created policy: %s (ID: %d)\n", policy.Name, policy.ID)
	if flags.Verbose {
		printPolicyDetails(*policy)
	}
}

func handlePolicyDelete(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) == 0 {
		fmt.Println("Error: Policy ID required")
		fmt.Println("Usage: tdcli perms policies delete <policy_id>")
		os.Exit(1)
	}

	policyIDStr := args[0]
	policyID, err := strconv.Atoi(policyIDStr)
	if err != nil {
		fmt.Printf("Error: Invalid policy ID: %s\n", policyIDStr)
		os.Exit(1)
	}

	// Confirm deletion
	fmt.Printf("Are you sure you want to delete policy %d? (y/N): ", policyID)
	var response string
	fmt.Scanln(&response)

	if response != "y" && response != "Y" && response != "yes" && response != "Yes" {
		fmt.Println("Deletion cancelled")
		return
	}

	deletedPolicy, err := client.Permissions.DeletePolicy(ctx, policyID)
	handleError(err, "Failed to delete policy", flags.Verbose)

	fmt.Printf("Deleted policy: %s (ID: %d)\n", deletedPolicy.Name, deletedPolicy.ID)
}

func handlePolicyGroupCommands(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) == 0 || args[0] == "help" {
		printPolicyGroupUsage()
		return
	}

	subcommand := args[0]
	subArgs := args[1:]

	switch subcommand {
	case "list", "ls":
		handlePolicyGroupList(ctx, client, flags)
	case "get", "show":
		handlePolicyGroupGet(ctx, client, subArgs, flags)
	case "create":
		handlePolicyGroupCreate(ctx, client, subArgs, flags)
	case "delete", "rm":
		handlePolicyGroupDelete(ctx, client, subArgs, flags)
	default:
		fmt.Printf("Unknown policy group subcommand: %s\n", subcommand)
		printPolicyGroupUsage()
		os.Exit(1)
	}
}

func printPolicyGroupUsage() {
	fmt.Printf(`Policy Group Management Commands

USAGE:
    tdcli perms groups <subcommand> [options]

SUBCOMMANDS:
    list, ls               List all policy groups
    get, show <id>         Get policy group details
    create <name>          Create a new policy group
    delete, rm <id>        Delete a policy group

EXAMPLES:
    tdcli perms groups list
    tdcli perms groups show 123
    tdcli perms groups create "Analytics Group"
    tdcli perms groups delete 123

`)
}

func handlePolicyGroupList(ctx context.Context, client *td.Client, flags Flags) {
	groups, err := client.Permissions.ListPolicyGroups(ctx)
	handleError(err, "Failed to list policy groups", flags.Verbose)

	switch flags.Format {
	case "json":
		printJSON(groups)
	case "csv":
		printPolicyGroupsCSV(groups)
	default:
		printPolicyGroupsTable(groups)
	}
}

func handlePolicyGroupGet(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) == 0 {
		fmt.Println("Error: Policy group ID required")
		fmt.Println("Usage: tdcli perms groups get <group_id>")
		os.Exit(1)
	}

	groupID := args[0]
	group, err := client.Permissions.GetPolicyGroup(ctx, groupID)
	handleError(err, "Failed to get policy group", flags.Verbose)

	switch flags.Format {
	case "json":
		printJSON(group)
	default:
		printPolicyGroupDetails(*group)
	}
}

func handlePolicyGroupCreate(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) == 0 {
		fmt.Println("Error: Policy group name required")
		fmt.Println("Usage: tdcli perms groups create <name>")
		os.Exit(1)
	}

	name := args[0]
	group, err := client.Permissions.CreatePolicyGroup(ctx, name)
	handleError(err, "Failed to create policy group", flags.Verbose)

	fmt.Printf("Created policy group: %s (ID: %d)\n", group.Name, group.ID)
	if flags.Verbose {
		printPolicyGroupDetails(*group)
	}
}

func handlePolicyGroupDelete(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) == 0 {
		fmt.Println("Error: Policy group ID required")
		fmt.Println("Usage: tdcli perms groups delete <group_id>")
		os.Exit(1)
	}

	groupID := args[0]

	// Confirm deletion
	fmt.Printf("Are you sure you want to delete policy group %s? (y/N): ", groupID)
	var response string
	fmt.Scanln(&response)

	if response != "y" && response != "Y" && response != "yes" && response != "Yes" {
		fmt.Println("Deletion cancelled")
		return
	}

	err := client.Permissions.DeletePolicyGroup(ctx, groupID)
	handleError(err, "Failed to delete policy group", flags.Verbose)

	fmt.Printf("Deleted policy group: %s\n", groupID)
}

func handleAccessControlUserCommands(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) == 0 || args[0] == "help" {
		printAccessControlUserUsage()
		return
	}

	subcommand := args[0]
	subArgs := args[1:]

	switch subcommand {
	case "list", "ls":
		handleAccessControlUserList(ctx, client, flags)
	case "get", "show":
		handleAccessControlUserGet(ctx, client, subArgs, flags)
	default:
		fmt.Printf("Unknown access control user subcommand: %s\n", subcommand)
		printAccessControlUserUsage()
		os.Exit(1)
	}
}

func printAccessControlUserUsage() {
	fmt.Printf(`Access Control User Management Commands

USAGE:
    tdcli perms users <subcommand> [options]

SUBCOMMANDS:
    list, ls               List access control users
    get, show <user_id>    Get user access control details

OPTIONS:
    --with-details         Include user email and name details (default: true)
    --format FORMAT        Output format (json, table, csv)
    --verbose, -v          Verbose output

EXAMPLES:
    tdcli perms users list
    tdcli perms users list --no-with-details
    tdcli perms users list --format json
    tdcli perms users show 12345

`)
}

func handleAccessControlUserList(ctx context.Context, client *td.Client, flags Flags) {
	users, err := client.Permissions.ListAccessControlUsers(ctx)
	handleError(err, "Failed to list access control users", flags.Verbose)

	var userDetailsMap map[int]td.User
	if flags.WithDetails {
		// Fetch all users to get email and name information
		allUsers, err := client.Users.List(ctx)
		if err != nil && flags.Verbose {
			fmt.Printf("Warning: Failed to fetch user details: %v\n", err)
		}

		// Create a map for quick lookup of user details by ID
		userDetailsMap = make(map[int]td.User)
		for _, user := range allUsers {
			userDetailsMap[user.ID] = user
		}
	}

	switch flags.Format {
	case "json":
		if flags.WithDetails && userDetailsMap != nil {
			printAccessControlUsersJSON(users, userDetailsMap)
		} else {
			printJSON(users)
		}
	case "csv":
		printAccessControlUsersCSV(users, userDetailsMap)
	default:
		printAccessControlUsersTable(users, userDetailsMap)
	}
}

func handleAccessControlUserGet(ctx context.Context, client *td.Client, args []string, flags Flags) {
	if len(args) == 0 {
		fmt.Println("Error: User ID required")
		fmt.Println("Usage: tdcli perms users get <user_id>")
		os.Exit(1)
	}

	userIDStr := args[0]
	userID, err := strconv.Atoi(userIDStr)
	if err != nil {
		fmt.Printf("Error: Invalid user ID: %s\n", userIDStr)
		os.Exit(1)
	}

	user, err := client.Permissions.GetAccessControlUser(ctx, userID)
	handleError(err, "Failed to get access control user", flags.Verbose)

	switch flags.Format {
	case "json":
		printJSON(user)
	default:
		printAccessControlUserDetails(*user)
	}
}

// Print functions
func printPoliciesTable(policies []td.AccessControlPolicy) {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "ID\tNAME\tDESCRIPTION\tUSERS")

	for _, policy := range policies {
		fmt.Fprintf(w, "%d\t%s\t%s\t%d\n",
			policy.ID,
			policy.Name,
			policy.Description,
			policy.UserCount,
		)
	}
	w.Flush()
}

func printPolicyDetails(policy td.AccessControlPolicy) {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "PROPERTY\tVALUE")
	fmt.Fprintf(w, "ID\t%d\n", policy.ID)
	fmt.Fprintf(w, "Name\t%s\n", policy.Name)
	fmt.Fprintf(w, "Description\t%s\n", policy.Description)
	fmt.Fprintf(w, "Account ID\t%d\n", policy.AccountID)
	fmt.Fprintf(w, "User Count\t%d\n", policy.UserCount)
	w.Flush()
}

func printPoliciesCSV(policies []td.AccessControlPolicy) {
	fmt.Println("id,name,description,account_id,user_count")
	for _, policy := range policies {
		fmt.Printf("%d,%s,%s,%d,%d\n",
			policy.ID,
			policy.Name,
			policy.Description,
			policy.AccountID,
			policy.UserCount,
		)
	}
}

func printPolicyGroupsTable(groups []td.AccessControlPolicyGroup) {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "ID\tNAME\tDESCRIPTION\tPOLICIES")

	for _, group := range groups {
		desc := ""
		if group.Description != nil {
			desc = *group.Description
		}
		fmt.Fprintf(w, "%d\t%s\t%s\t%d\n",
			group.ID,
			group.Name,
			desc,
			group.PolicyCount,
		)
	}
	w.Flush()
}

func printPolicyGroupDetails(group td.AccessControlPolicyGroup) {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "PROPERTY\tVALUE")
	fmt.Fprintf(w, "ID\t%d\n", group.ID)
	fmt.Fprintf(w, "Name\t%s\n", group.Name)
	if group.Description != nil {
		fmt.Fprintf(w, "Description\t%s\n", *group.Description)
	}
	fmt.Fprintf(w, "Account ID\t%d\n", group.AccountID)
	fmt.Fprintf(w, "Policy Count\t%d\n", group.PolicyCount)
	w.Flush()
}

func printPolicyGroupsCSV(groups []td.AccessControlPolicyGroup) {
	fmt.Println("id,name,description,account_id,policy_count")
	for _, group := range groups {
		desc := ""
		if group.Description != nil {
			desc = *group.Description
		}
		fmt.Printf("%d,%s,%s,%d,%d\n",
			group.ID,
			group.Name,
			desc,
			group.AccountID,
			group.PolicyCount,
		)
	}
}

func printAccessControlUsersTable(users []td.AccessControlUser, userDetailsMap map[int]td.User) {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)

	if userDetailsMap != nil {
		fmt.Fprintln(w, "USER_ID\tEMAIL\tNAME\tACCOUNT_ID\tPOLICIES")
		for _, user := range users {
			email := ""
			name := ""
			if details, ok := userDetailsMap[user.UserID]; ok {
				email = details.Email
				name = details.Name
			}
			fmt.Fprintf(w, "%d\t%s\t%s\t%d\t%d\n",
				user.UserID,
				email,
				name,
				user.AccountID,
				len(user.Policies),
			)
		}
	} else {
		fmt.Fprintln(w, "USER_ID\tACCOUNT_ID\tPOLICIES")
		for _, user := range users {
			fmt.Fprintf(w, "%d\t%d\t%d\n",
				user.UserID,
				user.AccountID,
				len(user.Policies),
			)
		}
	}
	w.Flush()
}

func printAccessControlUserDetails(user td.AccessControlUser) {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "PROPERTY\tVALUE")
	fmt.Fprintf(w, "User ID\t%d\n", user.UserID)
	fmt.Fprintf(w, "Account ID\t%d\n", user.AccountID)
	fmt.Fprintf(w, "Policies\t%d\n", len(user.Policies))
	w.Flush()

	if len(user.Policies) > 0 {
		fmt.Printf("\nPolicies:\n")
		printPoliciesTable(user.Policies)
	}
}

func printAccessControlUsersCSV(users []td.AccessControlUser, userDetailsMap map[int]td.User) {
	if userDetailsMap != nil {
		fmt.Println("user_id,email,name,account_id,policy_count")
		for _, user := range users {
			email := ""
			name := ""
			if details, ok := userDetailsMap[user.UserID]; ok {
				email = details.Email
				name = details.Name
			}
			fmt.Printf("%d,%s,%s,%d,%d\n",
				user.UserID,
				email,
				name,
				user.AccountID,
				len(user.Policies),
			)
		}
	} else {
		fmt.Println("user_id,account_id,policy_count")
		for _, user := range users {
			fmt.Printf("%d,%d,%d\n",
				user.UserID,
				user.AccountID,
				len(user.Policies),
			)
		}
	}
}

type AccessControlUserWithDetails struct {
	UserID      int                         `json:"user_id"`
	AccountID   int                         `json:"account_id"`
	Email       string                      `json:"email,omitempty"`
	Name        string                      `json:"name,omitempty"`
	Permissions td.AccessControlPermissions `json:"permissions"`
	Policies    []td.AccessControlPolicy    `json:"policies,omitempty"`
}

func printAccessControlUsersJSON(users []td.AccessControlUser, userDetailsMap map[int]td.User) {
	var usersWithDetails []AccessControlUserWithDetails

	for _, user := range users {
		userWithDetails := AccessControlUserWithDetails{
			UserID:      user.UserID,
			AccountID:   user.AccountID,
			Permissions: user.Permissions,
			Policies:    user.Policies,
		}

		if details, ok := userDetailsMap[user.UserID]; ok {
			userWithDetails.Email = details.Email
			userWithDetails.Name = details.Name
		}

		usersWithDetails = append(usersWithDetails, userWithDetails)
	}

	printJSON(usersWithDetails)
}
