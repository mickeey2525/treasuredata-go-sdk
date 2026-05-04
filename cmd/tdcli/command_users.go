package main

type UsersCmd struct {
	List UsersListCmd `kong:"cmd,aliases='ls',help='List users'"`
	Get  UsersGetCmd  `kong:"cmd,aliases='show',help='Get user details'"`
}

type UsersListCmd struct{}

func (u *UsersListCmd) Run(ctx *CLIContext) error {
	return runInstrumented(ctx, "users.list", []string{}, func() {
		handleUserList(ctx.Context, ctx.Client, ctx.GlobalFlags)
	})
}

type UsersGetCmd struct {
	UserID string `kong:"arg,help='User ID'"`
}

func (u *UsersGetCmd) Run(ctx *CLIContext) error {
	return runInstrumented(ctx, "users.get", []string{u.UserID}, func() {
		handleUserGet(ctx.Context, ctx.Client, []string{u.UserID}, ctx.GlobalFlags)
	})
}
