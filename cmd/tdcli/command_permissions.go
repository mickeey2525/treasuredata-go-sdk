package main

import "fmt"

type PermsCmd struct {
	Policies PermsPoliciesCmd `kong:"cmd,help='Policy management'"`
	Groups   PermsGroupsCmd   `kong:"cmd,help='Policy group management'"`
	Users    PermsUsersCmd    `kong:"cmd,help='Access control user management'"`
}

type PermsPoliciesCmd struct {
	List   PermsPoliciesListCmd   `kong:"cmd,aliases='ls',help='List all policies'"`
	Get    PermsPoliciesGetCmd    `kong:"cmd,aliases='show',help='Get policy details'"`
	Create PermsPoliciesCreateCmd `kong:"cmd,help='Create a new policy'"`
	Delete PermsPoliciesDeleteCmd `kong:"cmd,aliases='rm',help='Delete a policy'"`
}

type PermsPoliciesListCmd struct{}

func (p *PermsPoliciesListCmd) Run(ctx *CLIContext) error {
	return runHandlerWithErrorCapture(func() {
		handlePolicyList(ctx.Context, ctx.Client, ctx.GlobalFlags)
	})
}

type PermsPoliciesGetCmd struct {
	PolicyID int `kong:"arg,help='Policy ID'"`
}

func (p *PermsPoliciesGetCmd) Run(ctx *CLIContext) error {
	return runHandlerWithErrorCapture(func() {
		handlePolicyGet(ctx.Context, ctx.Client, []string{fmt.Sprintf("%d", p.PolicyID)}, ctx.GlobalFlags)
	})
}

type PermsPoliciesCreateCmd struct {
	Name        string `kong:"arg,help='Policy name'"`
	Description string `kong:"help='Policy description'"`
}

func (p *PermsPoliciesCreateCmd) Run(ctx *CLIContext) error {
	return runHandlerWithErrorCapture(func() {
		args := []string{p.Name}
		if p.Description != "" {
			args = append(args, p.Description)
		}
		handlePolicyCreate(ctx.Context, ctx.Client, args, ctx.GlobalFlags)
	})
}

type PermsPoliciesDeleteCmd struct {
	PolicyID int `kong:"arg,help='Policy ID'"`
}

func (p *PermsPoliciesDeleteCmd) Run(ctx *CLIContext) error {
	return runHandlerWithErrorCapture(func() {
		handlePolicyDelete(ctx.Context, ctx.Client, []string{fmt.Sprintf("%d", p.PolicyID)}, ctx.GlobalFlags)
	})
}

type PermsGroupsCmd struct {
	List   PermsGroupsListCmd   `kong:"cmd,aliases='ls',help='List all policy groups'"`
	Get    PermsGroupsGetCmd    `kong:"cmd,aliases='show',help='Get policy group details'"`
	Create PermsGroupsCreateCmd `kong:"cmd,help='Create a new policy group'"`
	Delete PermsGroupsDeleteCmd `kong:"cmd,aliases='rm',help='Delete a policy group'"`
}

type PermsGroupsListCmd struct{}

func (p *PermsGroupsListCmd) Run(ctx *CLIContext) error {
	return runHandlerWithErrorCapture(func() {
		handlePolicyGroupList(ctx.Context, ctx.Client, ctx.GlobalFlags)
	})
}

type PermsGroupsGetCmd struct {
	GroupID string `kong:"arg,help='Policy group ID'"`
}

func (p *PermsGroupsGetCmd) Run(ctx *CLIContext) error {
	return runHandlerWithErrorCapture(func() {
		handlePolicyGroupGet(ctx.Context, ctx.Client, []string{p.GroupID}, ctx.GlobalFlags)
	})
}

type PermsGroupsCreateCmd struct {
	Name string `kong:"arg,help='Policy group name'"`
}

func (p *PermsGroupsCreateCmd) Run(ctx *CLIContext) error {
	return runHandlerWithErrorCapture(func() {
		handlePolicyGroupCreate(ctx.Context, ctx.Client, []string{p.Name}, ctx.GlobalFlags)
	})
}

type PermsGroupsDeleteCmd struct {
	GroupID string `kong:"arg,help='Policy group ID'"`
}

func (p *PermsGroupsDeleteCmd) Run(ctx *CLIContext) error {
	return runHandlerWithErrorCapture(func() {
		handlePolicyGroupDelete(ctx.Context, ctx.Client, []string{p.GroupID}, ctx.GlobalFlags)
	})
}

type PermsUsersCmd struct {
	List PermsUsersListCmd `kong:"cmd,aliases='ls',help='List access control users'"`
	Get  PermsUsersGetCmd  `kong:"cmd,aliases='show',help='Get user access control details'"`
}

type PermsUsersListCmd struct {
	WithDetails bool `kong:"help='Include user email and name details',default=true"`
}

func (p *PermsUsersListCmd) Run(ctx *CLIContext) error {
	return runHandlerWithErrorCapture(func() {
		ctx.GlobalFlags.WithDetails = p.WithDetails
		handleAccessControlUserList(ctx.Context, ctx.Client, ctx.GlobalFlags)
	})
}

type PermsUsersGetCmd struct {
	UserID int `kong:"arg,help='User ID'"`
}

func (p *PermsUsersGetCmd) Run(ctx *CLIContext) error {
	return runHandlerWithErrorCapture(func() {
		handleAccessControlUserGet(ctx.Context, ctx.Client, []string{fmt.Sprintf("%d", p.UserID)}, ctx.GlobalFlags)
	})
}
