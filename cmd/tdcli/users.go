package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"text/tabwriter"

	td "github.com/mickeey2525/treasuredata-go-sdk"
)

func handleUserList(ctx context.Context, client *td.Client, flags Flags) error {
	users, err := client.Users.List(ctx)
	if err != nil {
		return wrapErr(err, "failed to list users", flags.Verbose)
	}

	csvFormatter := func(data interface{}) string {
		users := data.([]td.User)
		var csvBuilder strings.Builder
		for _, user := range users {
			csvBuilder.WriteString(fmt.Sprintf("%d,%s,%s,%d,%s,%t,%t,%t\n",
				user.ID,
				user.Name,
				user.Email,
				user.AccountID,
				user.CreatedAt.Format("2006-01-02 15:04:05"),
				user.Administrator,
				user.EmailVerified,
				user.Restricted,
			))
		}
		return csvBuilder.String()
	}

	tableFormatter := func(data interface{}) string {
		users := data.([]td.User)
		if len(users) == 0 {
			return "No users found\n"
		}
		var tableBuilder strings.Builder
		w := tabwriter.NewWriter(&tableBuilder, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "ID\tNAME\tEMAIL\tADMIN\tVERIFIED\tCREATED")
		for _, user := range users {
			fmt.Fprintf(w, "%d\t%s\t%s\t%t\t%t\t%s\n",
				user.ID,
				user.Name,
				user.Email,
				user.Administrator,
				user.EmailVerified,
				user.CreatedAt.Format("2006-01-02 15:04:05"),
			)
		}
		w.Flush()
		tableBuilder.WriteString(fmt.Sprintf("\nTotal: %d users\n", len(users)))
		return tableBuilder.String()
	}

	if err := formatAndWriteOutput(users, flags.Format, flags.Output, "id,name,email,account_id,created_at,administrator,email_verified,restricted", csvFormatter, tableFormatter); err != nil {
		return wrapErr(err, "failed to write output", flags.Verbose)
	}
	return nil
}

func handleUserGet(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	if len(args) == 0 {
		return errors.New("user email required\nUsage: tdcli user get <email>")
	}

	email := args[0]
	user, err := client.Users.Get(ctx, email)
	if err != nil {
		return wrapErr(err, "failed to get user", flags.Verbose)
	}

	csvFormatter := func(data interface{}) string {
		user := data.(*td.User)
		return fmt.Sprintf("%d,%s,%s,%d,%s,%t,%t,%t\n",
			user.ID,
			user.Name,
			user.Email,
			user.AccountID,
			user.CreatedAt.Format("2006-01-02 15:04:05"),
			user.Administrator,
			user.EmailVerified,
			user.Restricted,
		)
	}

	tableFormatter := func(data interface{}) string {
		user := data.(*td.User)
		var tableBuilder strings.Builder
		w := tabwriter.NewWriter(&tableBuilder, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "PROPERTY\tVALUE")
		fmt.Fprintf(w, "ID\t%d\n", user.ID)
		fmt.Fprintf(w, "Name\t%s\n", user.Name)
		fmt.Fprintf(w, "Email\t%s\n", user.Email)
		fmt.Fprintf(w, "Account ID\t%d\n", user.AccountID)
		fmt.Fprintf(w, "Created\t%s\n", user.CreatedAt.Format("2006-01-02 15:04:05"))
		fmt.Fprintf(w, "Updated\t%s\n", user.UpdatedAt.Format("2006-01-02 15:04:05"))
		fmt.Fprintf(w, "Administrator\t%t\n", user.Administrator)
		fmt.Fprintf(w, "Email Verified\t%t\n", user.EmailVerified)
		fmt.Fprintf(w, "Restricted\t%t\n", user.Restricted)
		w.Flush()
		return tableBuilder.String()
	}

	if err := formatAndWriteOutput(user, flags.Format, flags.Output, "id,name,email,account_id,created_at,administrator,email_verified,restricted", csvFormatter, tableFormatter); err != nil {
		return wrapErr(err, "failed to write output", flags.Verbose)
	}
	return nil
}
