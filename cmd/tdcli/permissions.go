package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"text/tabwriter"

	td "github.com/mickeey2525/treasuredata-go-sdk"
)

func handlePolicyList(ctx context.Context, client *td.Client, flags Flags) error {
	policies, err := client.Permissions.ListPolicies(ctx, nil)
	if err != nil {
		return wrapErr(err, "failed to list policies", flags.Verbose)
	}

	switch flags.Format {
	case "json":
		printJSON(policies)
	case "csv":
		printPoliciesCSV(policies)
	default:
		printPoliciesTable(policies)
	}
	return nil
}

func handlePolicyGet(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	if len(args) == 0 {
		return errors.New("policy ID required\nUsage: tdcli perms policies get <policy_id>")
	}

	policyID, err := strconv.Atoi(args[0])
	if err != nil {
		return fmt.Errorf("invalid policy ID: %s", args[0])
	}

	policy, err := client.Permissions.GetPolicy(ctx, policyID)
	if err != nil {
		return wrapErr(err, "failed to get policy", flags.Verbose)
	}

	switch flags.Format {
	case "json":
		printJSON(policy)
	default:
		printPolicyDetails(*policy)
	}
	return nil
}

func handlePolicyCreate(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	if len(args) == 0 {
		return errors.New("policy name required\nUsage: tdcli perms policies create <name> [description]")
	}

	name := args[0]
	description := ""
	if len(args) > 1 {
		description = args[1]
	}

	policy, err := client.Permissions.CreatePolicy(ctx, name, description)
	if err != nil {
		return wrapErr(err, "failed to create policy", flags.Verbose)
	}

	fmt.Printf("Created policy: %s (ID: %d)\n", policy.Name, policy.ID)
	if flags.Verbose {
		printPolicyDetails(*policy)
	}
	return nil
}

func handlePolicyDelete(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	if len(args) == 0 {
		return errors.New("policy ID required\nUsage: tdcli perms policies delete <policy_id>")
	}

	policyID, err := strconv.Atoi(args[0])
	if err != nil {
		return fmt.Errorf("invalid policy ID: %s", args[0])
	}

	fmt.Printf("Are you sure you want to delete policy %d? (y/N): ", policyID)
	var response string
	fmt.Scanln(&response)

	if response != "y" && response != "Y" && response != "yes" && response != "Yes" {
		fmt.Println("Deletion cancelled")
		return nil
	}

	deletedPolicy, err := client.Permissions.DeletePolicy(ctx, policyID)
	if err != nil {
		return wrapErr(err, "failed to delete policy", flags.Verbose)
	}

	fmt.Printf("Deleted policy: %s (ID: %d)\n", deletedPolicy.Name, deletedPolicy.ID)
	return nil
}

func handlePolicyGroupList(ctx context.Context, client *td.Client, flags Flags) error {
	groups, err := client.Permissions.ListPolicyGroups(ctx)
	if err != nil {
		return wrapErr(err, "failed to list policy groups", flags.Verbose)
	}

	switch flags.Format {
	case "json":
		printJSON(groups)
	case "csv":
		printPolicyGroupsCSV(groups)
	default:
		printPolicyGroupsTable(groups)
	}
	return nil
}

func handlePolicyGroupGet(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	if len(args) == 0 {
		return errors.New("policy group ID required\nUsage: tdcli perms groups get <group_id>")
	}

	group, err := client.Permissions.GetPolicyGroup(ctx, args[0])
	if err != nil {
		return wrapErr(err, "failed to get policy group", flags.Verbose)
	}

	switch flags.Format {
	case "json":
		printJSON(group)
	default:
		printPolicyGroupDetails(*group)
	}
	return nil
}

func handlePolicyGroupCreate(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	if len(args) == 0 {
		return errors.New("policy group name required\nUsage: tdcli perms groups create <name>")
	}

	group, err := client.Permissions.CreatePolicyGroup(ctx, args[0])
	if err != nil {
		return wrapErr(err, "failed to create policy group", flags.Verbose)
	}

	fmt.Printf("Created policy group: %s (ID: %d)\n", group.Name, group.ID)
	if flags.Verbose {
		printPolicyGroupDetails(*group)
	}
	return nil
}

func handlePolicyGroupDelete(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	if len(args) == 0 {
		return errors.New("policy group ID required\nUsage: tdcli perms groups delete <group_id>")
	}

	groupID := args[0]

	fmt.Printf("Are you sure you want to delete policy group %s? (y/N): ", groupID)
	var response string
	fmt.Scanln(&response)

	if response != "y" && response != "Y" && response != "yes" && response != "Yes" {
		fmt.Println("Deletion cancelled")
		return nil
	}

	if err := client.Permissions.DeletePolicyGroup(ctx, groupID); err != nil {
		return wrapErr(err, "failed to delete policy group", flags.Verbose)
	}

	fmt.Printf("Deleted policy group: %s\n", groupID)
	return nil
}

func handleAccessControlUserList(ctx context.Context, client *td.Client, flags Flags) error {
	users, err := client.Permissions.ListAccessControlUsers(ctx)
	if err != nil {
		return wrapErr(err, "failed to list access control users", flags.Verbose)
	}

	var userDetailsMap map[int]td.User
	if flags.WithDetails {
		userDetailsMap = make(map[int]td.User)

		allUsers, err := client.Users.List(ctx)
		if err != nil && flags.Verbose {
			fmt.Printf("Warning: Failed to fetch user details: %v\n", err)
		} else {
			neededUserIDs := make(map[int]bool)
			for _, user := range users {
				neededUserIDs[user.UserID] = true
			}

			for _, user := range allUsers {
				if neededUserIDs[user.ID] {
					userDetailsMap[user.ID] = user
				}
			}
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
	return nil
}

func handleAccessControlUserGet(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	if len(args) == 0 {
		return errors.New("user ID required\nUsage: tdcli perms users get <user_id>")
	}

	userID, err := strconv.Atoi(args[0])
	if err != nil {
		return fmt.Errorf("invalid user ID: %s", args[0])
	}

	user, err := client.Permissions.GetAccessControlUser(ctx, userID)
	if err != nil {
		return wrapErr(err, "failed to get access control user", flags.Verbose)
	}

	switch flags.Format {
	case "json":
		printJSON(user)
	default:
		printAccessControlUserDetails(*user)
	}
	return nil
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
