package main

import (
	"context"
	"log"

	td "github.com/mickeey2525/treasuredata-go-sdk"
	cdphandlers "github.com/mickeey2525/treasuredata-go-sdk/cmd/tdcli/cdp"
)

// CDP segment handlers
func handleCDPSegmentCreate(ctx context.Context, client *td.Client, args []string, flags Flags) {
	cdpFlags := cdphandlers.Flags{
		APIKey:      flags.APIKey,
		Region:      flags.Region,
		Format:      flags.Format,
		Output:      flags.Output,
		Verbose:     flags.Verbose,
		Database:    flags.Database,
		Status:      flags.Status,
		Priority:    flags.Priority,
		Limit:       flags.Limit,
		WithDetails: flags.WithDetails,
	}
	cdphandlers.HandleSegmentCreate(ctx, client, args, cdpFlags)
}

func handleCDPSegmentList(ctx context.Context, client *td.Client, args []string, flags Flags) {
	cdpFlags := cdphandlers.Flags{
		APIKey:      flags.APIKey,
		Region:      flags.Region,
		Format:      flags.Format,
		Output:      flags.Output,
		Verbose:     flags.Verbose,
		Database:    flags.Database,
		Status:      flags.Status,
		Priority:    flags.Priority,
		Limit:       flags.Limit,
		WithDetails: flags.WithDetails,
	}
	cdphandlers.HandleSegmentList(ctx, client, args, cdpFlags)
}

func handleCDPSegmentGet(ctx context.Context, client *td.Client, args []string, flags Flags) {
	cdpFlags := cdphandlers.Flags{
		APIKey:      flags.APIKey,
		Region:      flags.Region,
		Format:      flags.Format,
		Output:      flags.Output,
		Verbose:     flags.Verbose,
		Database:    flags.Database,
		Status:      flags.Status,
		Priority:    flags.Priority,
		Limit:       flags.Limit,
		WithDetails: flags.WithDetails,
	}
	cdphandlers.HandleSegmentGet(ctx, client, args, cdpFlags)
}

func handleCDPSegmentUpdate(ctx context.Context, client *td.Client, args []string, flags Flags) {
	cdpFlags := cdphandlers.Flags{
		APIKey:      flags.APIKey,
		Region:      flags.Region,
		Format:      flags.Format,
		Output:      flags.Output,
		Verbose:     flags.Verbose,
		Database:    flags.Database,
		Status:      flags.Status,
		Priority:    flags.Priority,
		Limit:       flags.Limit,
		WithDetails: flags.WithDetails,
	}
	cdphandlers.HandleSegmentUpdate(ctx, client, args, cdpFlags)
}

func handleCDPSegmentDelete(ctx context.Context, client *td.Client, args []string, flags Flags) {
	cdpFlags := cdphandlers.Flags{
		APIKey:      flags.APIKey,
		Region:      flags.Region,
		Format:      flags.Format,
		Output:      flags.Output,
		Verbose:     flags.Verbose,
		Database:    flags.Database,
		Status:      flags.Status,
		Priority:    flags.Priority,
		Limit:       flags.Limit,
		WithDetails: flags.WithDetails,
	}
	cdphandlers.HandleSegmentDelete(ctx, client, args, cdpFlags)
}

// CDP audience handlers
func handleCDPAudienceCreate(ctx context.Context, client *td.Client, args []string, flags Flags) {
	cdpFlags := cdphandlers.Flags{
		APIKey:      flags.APIKey,
		Region:      flags.Region,
		Format:      flags.Format,
		Output:      flags.Output,
		Verbose:     flags.Verbose,
		Database:    flags.Database,
		Status:      flags.Status,
		Priority:    flags.Priority,
		Limit:       flags.Limit,
		WithDetails: flags.WithDetails,
	}
	cdphandlers.HandleAudienceCreate(ctx, client, args, cdpFlags)
}

func handleCDPAudienceList(ctx context.Context, client *td.Client, flags Flags) {
	cdpFlags := cdphandlers.Flags{
		APIKey:      flags.APIKey,
		Region:      flags.Region,
		Format:      flags.Format,
		Output:      flags.Output,
		Verbose:     flags.Verbose,
		Database:    flags.Database,
		Status:      flags.Status,
		Priority:    flags.Priority,
		Limit:       flags.Limit,
		WithDetails: flags.WithDetails,
	}
	cdphandlers.HandleAudienceList(ctx, client, cdpFlags)
}

func handleCDPAudienceGet(ctx context.Context, client *td.Client, args []string, flags Flags) {
	cdpFlags := cdphandlers.Flags{
		APIKey:      flags.APIKey,
		Region:      flags.Region,
		Format:      flags.Format,
		Output:      flags.Output,
		Verbose:     flags.Verbose,
		Database:    flags.Database,
		Status:      flags.Status,
		Priority:    flags.Priority,
		Limit:       flags.Limit,
		WithDetails: flags.WithDetails,
	}
	cdphandlers.HandleAudienceGet(ctx, client, args, cdpFlags)
}

func handleCDPAudienceDelete(ctx context.Context, client *td.Client, args []string, flags Flags) {
	cdpFlags := cdphandlers.Flags{
		APIKey:      flags.APIKey,
		Region:      flags.Region,
		Format:      flags.Format,
		Output:      flags.Output,
		Verbose:     flags.Verbose,
		Database:    flags.Database,
		Status:      flags.Status,
		Priority:    flags.Priority,
		Limit:       flags.Limit,
		WithDetails: flags.WithDetails,
	}
	cdphandlers.HandleAudienceDelete(ctx, client, args, cdpFlags)
}

func handleCDPAudienceUpdate(ctx context.Context, client *td.Client, args []string, flags Flags) {
	cdpFlags := cdphandlers.Flags{
		APIKey:      flags.APIKey,
		Region:      flags.Region,
		Format:      flags.Format,
		Output:      flags.Output,
		Verbose:     flags.Verbose,
		Database:    flags.Database,
		Status:      flags.Status,
		Priority:    flags.Priority,
		Limit:       flags.Limit,
		WithDetails: flags.WithDetails,
	}
	cdphandlers.HandleAudienceUpdate(ctx, client, args, cdpFlags)
}

func handleCDPAudienceAttributes(ctx context.Context, client *td.Client, args []string, flags Flags) {
	cdpFlags := cdphandlers.Flags{
		APIKey:      flags.APIKey,
		Region:      flags.Region,
		Format:      flags.Format,
		Output:      flags.Output,
		Verbose:     flags.Verbose,
		Database:    flags.Database,
		Status:      flags.Status,
		Priority:    flags.Priority,
		Limit:       flags.Limit,
		WithDetails: flags.WithDetails,
	}
	cdphandlers.HandleAudienceAttributes(ctx, client, args, cdpFlags)
}

// CDP audience behavior handlers
func handleCDPAudienceBehaviors(ctx context.Context, client *td.Client, args []string, flags Flags) {
	cdpFlags := cdphandlers.Flags{
		APIKey:      flags.APIKey,
		Region:      flags.Region,
		Format:      flags.Format,
		Output:      flags.Output,
		Verbose:     flags.Verbose,
		Database:    flags.Database,
		Status:      flags.Status,
		Priority:    flags.Priority,
		Limit:       flags.Limit,
		WithDetails: flags.WithDetails,
	}
	cdphandlers.HandleAudienceBehaviors(ctx, client, args, cdpFlags)
}

func handleCDPAudienceRun(ctx context.Context, client *td.Client, args []string, flags Flags) {
	cdpFlags := cdphandlers.Flags{
		APIKey:      flags.APIKey,
		Region:      flags.Region,
		Format:      flags.Format,
		Output:      flags.Output,
		Verbose:     flags.Verbose,
		Database:    flags.Database,
		Status:      flags.Status,
		Priority:    flags.Priority,
		Limit:       flags.Limit,
		WithDetails: flags.WithDetails,
	}
	cdphandlers.HandleAudienceRun(ctx, client, args, cdpFlags)
}

func handleCDPAudienceExecutions(ctx context.Context, client *td.Client, args []string, flags Flags) {
	cdpFlags := cdphandlers.Flags{
		APIKey:      flags.APIKey,
		Region:      flags.Region,
		Format:      flags.Format,
		Output:      flags.Output,
		Verbose:     flags.Verbose,
		Database:    flags.Database,
		Status:      flags.Status,
		Priority:    flags.Priority,
		Limit:       flags.Limit,
		WithDetails: flags.WithDetails,
	}
	cdphandlers.HandleAudienceExecutions(ctx, client, args, cdpFlags)
}

func handleCDPAudienceStatistics(ctx context.Context, client *td.Client, args []string, flags Flags) {
	cdpFlags := cdphandlers.Flags{
		APIKey:      flags.APIKey,
		Region:      flags.Region,
		Format:      flags.Format,
		Output:      flags.Output,
		Verbose:     flags.Verbose,
		Database:    flags.Database,
		Status:      flags.Status,
		Priority:    flags.Priority,
		Limit:       flags.Limit,
		WithDetails: flags.WithDetails,
	}
	cdphandlers.HandleAudienceStatistics(ctx, client, args, cdpFlags)
}

func handleCDPAudienceSampleValues(ctx context.Context, client *td.Client, args []string, flags Flags) {
	cdpFlags := cdphandlers.Flags{
		APIKey:      flags.APIKey,
		Region:      flags.Region,
		Format:      flags.Format,
		Output:      flags.Output,
		Verbose:     flags.Verbose,
		Database:    flags.Database,
		Status:      flags.Status,
		Priority:    flags.Priority,
		Limit:       flags.Limit,
		WithDetails: flags.WithDetails,
	}
	cdphandlers.HandleAudienceSampleValues(ctx, client, args, cdpFlags)
}

func handleCDPAudienceBehaviorSamples(ctx context.Context, client *td.Client, args []string, flags Flags) {
	cdpFlags := cdphandlers.Flags{
		APIKey:      flags.APIKey,
		Region:      flags.Region,
		Format:      flags.Format,
		Output:      flags.Output,
		Verbose:     flags.Verbose,
		Database:    flags.Database,
		Status:      flags.Status,
		Priority:    flags.Priority,
		Limit:       flags.Limit,
		WithDetails: flags.WithDetails,
	}
	cdphandlers.HandleAudienceBehaviorSamples(ctx, client, args, cdpFlags)
}

// CDP segment folder handlers
func handleCDPSegmentFolders(ctx context.Context, client *td.Client, args []string, flags Flags) {
	cdpFlags := cdphandlers.Flags{
		APIKey:      flags.APIKey,
		Region:      flags.Region,
		Format:      flags.Format,
		Output:      flags.Output,
		Verbose:     flags.Verbose,
		Database:    flags.Database,
		Status:      flags.Status,
		Priority:    flags.Priority,
		Limit:       flags.Limit,
		WithDetails: flags.WithDetails,
	}
	cdphandlers.HandleSegmentFolders(ctx, client, args, cdpFlags)
}

// Additional segment handlers for query operations
func handleCDPSegmentQuery(ctx context.Context, client *td.Client, args []string, flags Flags) {
	cdpFlags := cdphandlers.Flags{
		APIKey:      flags.APIKey,
		Region:      flags.Region,
		Format:      flags.Format,
		Output:      flags.Output,
		Verbose:     flags.Verbose,
		Database:    flags.Database,
		Status:      flags.Status,
		Priority:    flags.Priority,
		Limit:       flags.Limit,
		WithDetails: flags.WithDetails,
	}
	cdphandlers.HandleSegmentQuery(ctx, client, args, cdpFlags)
}

func handleCDPSegmentNewQuery(ctx context.Context, client *td.Client, args []string, flags Flags) {
	cdpFlags := cdphandlers.Flags{
		APIKey:      flags.APIKey,
		Region:      flags.Region,
		Format:      flags.Format,
		Output:      flags.Output,
		Verbose:     flags.Verbose,
		Database:    flags.Database,
		Status:      flags.Status,
		Priority:    flags.Priority,
		Limit:       flags.Limit,
		WithDetails: flags.WithDetails,
	}
	cdphandlers.HandleSegmentNewQuery(ctx, client, args, cdpFlags)
}

func handleCDPSegmentQueryStatus(ctx context.Context, client *td.Client, args []string, flags Flags) {
	cdpFlags := cdphandlers.Flags{
		APIKey:      flags.APIKey,
		Region:      flags.Region,
		Format:      flags.Format,
		Output:      flags.Output,
		Verbose:     flags.Verbose,
		Database:    flags.Database,
		Status:      flags.Status,
		Priority:    flags.Priority,
		Limit:       flags.Limit,
		WithDetails: flags.WithDetails,
	}
	cdphandlers.HandleSegmentQueryStatus(ctx, client, args, cdpFlags)
}

func handleCDPSegmentKillQuery(ctx context.Context, client *td.Client, args []string, flags Flags) {
	cdpFlags := cdphandlers.Flags{
		APIKey:      flags.APIKey,
		Region:      flags.Region,
		Format:      flags.Format,
		Output:      flags.Output,
		Verbose:     flags.Verbose,
		Database:    flags.Database,
		Status:      flags.Status,
		Priority:    flags.Priority,
		Limit:       flags.Limit,
		WithDetails: flags.WithDetails,
	}
	cdphandlers.HandleSegmentKillQuery(ctx, client, args, cdpFlags)
}

func handleCDPSegmentCustomers(ctx context.Context, client *td.Client, args []string, flags Flags) {
	cdpFlags := cdphandlers.Flags{
		APIKey:      flags.APIKey,
		Region:      flags.Region,
		Format:      flags.Format,
		Output:      flags.Output,
		Verbose:     flags.Verbose,
		Database:    flags.Database,
		Status:      flags.Status,
		Priority:    flags.Priority,
		Limit:       flags.Limit,
		WithDetails: flags.WithDetails,
	}
	cdphandlers.HandleSegmentCustomers(ctx, client, args, cdpFlags)
}

func handleCDPSegmentStatistics(ctx context.Context, client *td.Client, args []string, flags Flags) {
	cdpFlags := cdphandlers.Flags{
		APIKey:      flags.APIKey,
		Region:      flags.Region,
		Format:      flags.Format,
		Output:      flags.Output,
		Verbose:     flags.Verbose,
		Database:    flags.Database,
		Status:      flags.Status,
		Priority:    flags.Priority,
		Limit:       flags.Limit,
		WithDetails: flags.WithDetails,
	}
	cdphandlers.HandleSegmentStatistics(ctx, client, args, cdpFlags)
}

// CDP activation handlers
func handleCDPActivationCreate(ctx context.Context, client *td.Client, args []string, flags Flags) {
	cdpFlags := cdphandlers.Flags{
		APIKey:      flags.APIKey,
		Region:      flags.Region,
		Format:      flags.Format,
		Output:      flags.Output,
		Verbose:     flags.Verbose,
		Database:    flags.Database,
		Status:      flags.Status,
		Priority:    flags.Priority,
		Limit:       flags.Limit,
		WithDetails: flags.WithDetails,
	}
	cdphandlers.HandleActivationCreate(ctx, client, args, cdpFlags)
}

func handleCDPActivationCreateWithStruct(ctx context.Context, client *td.Client, args []string, flags Flags) {
	cdpFlags := cdphandlers.Flags{
		APIKey:      flags.APIKey,
		Region:      flags.Region,
		Format:      flags.Format,
		Output:      flags.Output,
		Verbose:     flags.Verbose,
		Database:    flags.Database,
		Status:      flags.Status,
		Priority:    flags.Priority,
		Limit:       flags.Limit,
		WithDetails: flags.WithDetails,
	}
	cdphandlers.HandleActivationCreateWithStruct(ctx, client, args, cdpFlags)
}

func handleCDPActivationListWithForce(ctx context.Context, client *td.Client, flags Flags, force bool) {
	cdpFlags := cdphandlers.Flags{
		APIKey:      flags.APIKey,
		Region:      flags.Region,
		Format:      flags.Format,
		Output:      flags.Output,
		Verbose:     flags.Verbose,
		Database:    flags.Database,
		Status:      flags.Status,
		Priority:    flags.Priority,
		Limit:       flags.Limit,
		WithDetails: flags.WithDetails,
	}
	cdphandlers.HandleActivationListWithForce(ctx, client, cdpFlags, force)
}

func handleCDPActivationGet(ctx context.Context, client *td.Client, args []string, flags Flags) {
	cdpFlags := cdphandlers.Flags{
		APIKey:      flags.APIKey,
		Region:      flags.Region,
		Format:      flags.Format,
		Output:      flags.Output,
		Verbose:     flags.Verbose,
		Database:    flags.Database,
		Status:      flags.Status,
		Priority:    flags.Priority,
		Limit:       flags.Limit,
		WithDetails: flags.WithDetails,
	}
	cdphandlers.HandleActivationGet(ctx, client, args, cdpFlags)
}

func handleCDPActivationUpdate(ctx context.Context, client *td.Client, args []string, flags Flags) {
	cdpFlags := cdphandlers.Flags{
		APIKey:      flags.APIKey,
		Region:      flags.Region,
		Format:      flags.Format,
		Output:      flags.Output,
		Verbose:     flags.Verbose,
		Database:    flags.Database,
		Status:      flags.Status,
		Priority:    flags.Priority,
		Limit:       flags.Limit,
		WithDetails: flags.WithDetails,
	}
	cdphandlers.HandleActivationUpdate(ctx, client, args, cdpFlags)
}

func handleCDPActivationUpdateStatus(ctx context.Context, client *td.Client, args []string, flags Flags) {
	cdpFlags := cdphandlers.Flags{
		APIKey:      flags.APIKey,
		Region:      flags.Region,
		Format:      flags.Format,
		Output:      flags.Output,
		Verbose:     flags.Verbose,
		Database:    flags.Database,
		Status:      flags.Status,
		Priority:    flags.Priority,
		Limit:       flags.Limit,
		WithDetails: flags.WithDetails,
	}
	cdphandlers.HandleActivationUpdateStatus(ctx, client, args, cdpFlags)
}

func handleCDPActivationDelete(ctx context.Context, client *td.Client, args []string, flags Flags) {
	cdpFlags := cdphandlers.Flags{
		APIKey:      flags.APIKey,
		Region:      flags.Region,
		Format:      flags.Format,
		Output:      flags.Output,
		Verbose:     flags.Verbose,
		Database:    flags.Database,
		Status:      flags.Status,
		Priority:    flags.Priority,
		Limit:       flags.Limit,
		WithDetails: flags.WithDetails,
	}
	cdphandlers.HandleActivationDelete(ctx, client, args, cdpFlags)
}

func handleCDPExecuteActivation(ctx context.Context, client *td.Client, args []string, flags Flags) {
	cdpFlags := cdphandlers.Flags{
		APIKey:      flags.APIKey,
		Region:      flags.Region,
		Format:      flags.Format,
		Output:      flags.Output,
		Verbose:     flags.Verbose,
		Database:    flags.Database,
		Status:      flags.Status,
		Priority:    flags.Priority,
		Limit:       flags.Limit,
		WithDetails: flags.WithDetails,
	}
	cdphandlers.HandleActivationExecute(ctx, client, args, cdpFlags)
}

func handleCDPGetActivationExecutions(ctx context.Context, client *td.Client, args []string, flags Flags) {
	cdpFlags := cdphandlers.Flags{
		APIKey:      flags.APIKey,
		Region:      flags.Region,
		Format:      flags.Format,
		Output:      flags.Output,
		Verbose:     flags.Verbose,
		Database:    flags.Database,
		Status:      flags.Status,
		Priority:    flags.Priority,
		Limit:       flags.Limit,
		WithDetails: flags.WithDetails,
	}
	cdphandlers.HandleActivationGetExecutions(ctx, client, args, cdpFlags)
}

func handleCDPListActivationsByAudience(ctx context.Context, client *td.Client, args []string, flags Flags) {
	cdpFlags := cdphandlers.Flags{
		APIKey:      flags.APIKey,
		Region:      flags.Region,
		Format:      flags.Format,
		Output:      flags.Output,
		Verbose:     flags.Verbose,
		Database:    flags.Database,
		Status:      flags.Status,
		Priority:    flags.Priority,
		Limit:       flags.Limit,
		WithDetails: flags.WithDetails,
	}
	cdphandlers.HandleActivationListByAudience(ctx, client, args, cdpFlags)
}

func handleCDPListActivationsBySegmentFolder(ctx context.Context, client *td.Client, args []string, flags Flags) {
	cdpFlags := cdphandlers.Flags{
		APIKey:      flags.APIKey,
		Region:      flags.Region,
		Format:      flags.Format,
		Output:      flags.Output,
		Verbose:     flags.Verbose,
		Database:    flags.Database,
		Status:      flags.Status,
		Priority:    flags.Priority,
		Limit:       flags.Limit,
		WithDetails: flags.WithDetails,
	}
	cdphandlers.HandleActivationListBySegmentFolder(ctx, client, args, cdpFlags)
}

func handleCDPRunSegmentActivation(ctx context.Context, client *td.Client, args []string, flags Flags) {
	cdpFlags := cdphandlers.Flags{
		APIKey:      flags.APIKey,
		Region:      flags.Region,
		Format:      flags.Format,
		Output:      flags.Output,
		Verbose:     flags.Verbose,
		Database:    flags.Database,
		Status:      flags.Status,
		Priority:    flags.Priority,
		Limit:       flags.Limit,
		WithDetails: flags.WithDetails,
	}
	cdphandlers.HandleActivationRunForSegment(ctx, client, args, cdpFlags)
}

func handleCDPListActivationsByParentSegment(ctx context.Context, client *td.Client, args []string, flags Flags) {
	cdpFlags := cdphandlers.Flags{
		APIKey:      flags.APIKey,
		Region:      flags.Region,
		Format:      flags.Format,
		Output:      flags.Output,
		Verbose:     flags.Verbose,
		Database:    flags.Database,
		Status:      flags.Status,
		Priority:    flags.Priority,
		Limit:       flags.Limit,
		WithDetails: flags.WithDetails,
	}
	cdphandlers.HandleActivationListByParentSegment(ctx, client, args, cdpFlags)
}

func handleCDPGetWorkflowProjectsForParentSegment(ctx context.Context, client *td.Client, args []string, flags Flags) {
	cdpFlags := cdphandlers.Flags{
		APIKey:      flags.APIKey,
		Region:      flags.Region,
		Format:      flags.Format,
		Output:      flags.Output,
		Verbose:     flags.Verbose,
		Database:    flags.Database,
		Status:      flags.Status,
		Priority:    flags.Priority,
		Limit:       flags.Limit,
		WithDetails: flags.WithDetails,
	}
	cdphandlers.HandleGetWorkflowProjectsForParentSegment(ctx, client, args, cdpFlags)
}

func handleCDPGetWorkflowsForParentSegment(ctx context.Context, client *td.Client, args []string, flags Flags) {
	cdpFlags := cdphandlers.Flags{
		APIKey:      flags.APIKey,
		Region:      flags.Region,
		Format:      flags.Format,
		Output:      flags.Output,
		Verbose:     flags.Verbose,
		Database:    flags.Database,
		Status:      flags.Status,
		Priority:    flags.Priority,
		Limit:       flags.Limit,
		WithDetails: flags.WithDetails,
	}
	cdphandlers.HandleGetWorkflowsForParentSegment(ctx, client, args, cdpFlags)
}

func handleCDPGetMatchedActivations(ctx context.Context, client *td.Client, args []string, flags Flags) {
	log.Fatal("Get matched activations is not yet implemented")
}

func handleCDPRunActivationForSegment(ctx context.Context, client *td.Client, args []string, flags Flags) {
	log.Fatal("Run activation for segment is not yet implemented")
}

func handleCDPGetMatchedActivationsForParentSegment(ctx context.Context, client *td.Client, args []string, flags Flags) {
	cdpFlags := cdphandlers.Flags{
		APIKey:      flags.APIKey,
		Region:      flags.Region,
		Format:      flags.Format,
		Output:      flags.Output,
		Verbose:     flags.Verbose,
		Database:    flags.Database,
		Status:      flags.Status,
		Priority:    flags.Priority,
		Limit:       flags.Limit,
		WithDetails: flags.WithDetails,
	}
	cdphandlers.HandleGetMatchedActivationsForParentSegment(ctx, client, args, cdpFlags)
}

func handleCDPListFolders(ctx context.Context, client *td.Client, args []string, flags Flags) {
	cdpFlags := cdphandlers.Flags{
		APIKey:      flags.APIKey,
		Region:      flags.Region,
		Format:      flags.Format,
		Output:      flags.Output,
		Verbose:     flags.Verbose,
		Database:    flags.Database,
		Status:      flags.Status,
		Priority:    flags.Priority,
		Limit:       flags.Limit,
		WithDetails: flags.WithDetails,
	}
	cdphandlers.HandleListFolders(ctx, client, args, cdpFlags)
}

func handleCDPCreateAudienceFolder(ctx context.Context, client *td.Client, args []string, flags Flags) {
	cdpFlags := cdphandlers.Flags{
		APIKey:      flags.APIKey,
		Region:      flags.Region,
		Format:      flags.Format,
		Output:      flags.Output,
		Verbose:     flags.Verbose,
		Database:    flags.Database,
		Status:      flags.Status,
		Priority:    flags.Priority,
		Limit:       flags.Limit,
		WithDetails: flags.WithDetails,
	}
	cdphandlers.HandleCreateAudienceFolder(ctx, client, args, cdpFlags)
}

func handleCDPGetAudienceFolder(ctx context.Context, client *td.Client, args []string, flags Flags) {
	cdpFlags := cdphandlers.Flags{
		APIKey:      flags.APIKey,
		Region:      flags.Region,
		Format:      flags.Format,
		Output:      flags.Output,
		Verbose:     flags.Verbose,
		Database:    flags.Database,
		Status:      flags.Status,
		Priority:    flags.Priority,
		Limit:       flags.Limit,
		WithDetails: flags.WithDetails,
	}
	cdphandlers.HandleGetAudienceFolder(ctx, client, args, cdpFlags)
}

func handleCDPCreateEntityFolder(ctx context.Context, client *td.Client, args []string, flags Flags) {
	cdpFlags := cdphandlers.Flags{
		APIKey:      flags.APIKey,
		Region:      flags.Region,
		Format:      flags.Format,
		Output:      flags.Output,
		Verbose:     flags.Verbose,
		Database:    flags.Database,
		Status:      flags.Status,
		Priority:    flags.Priority,
		Limit:       flags.Limit,
		WithDetails: flags.WithDetails,
	}
	cdphandlers.HandleCreateEntityFolder(ctx, client, args, cdpFlags)
}

func handleCDPGetEntityFolder(ctx context.Context, client *td.Client, args []string, flags Flags) {
	cdpFlags := cdphandlers.Flags{
		APIKey:      flags.APIKey,
		Region:      flags.Region,
		Format:      flags.Format,
		Output:      flags.Output,
		Verbose:     flags.Verbose,
		Database:    flags.Database,
		Status:      flags.Status,
		Priority:    flags.Priority,
		Limit:       flags.Limit,
		WithDetails: flags.WithDetails,
	}
	cdphandlers.HandleGetEntityFolder(ctx, client, args, cdpFlags)
}

func handleCDPUpdateEntityFolder(ctx context.Context, client *td.Client, args []string, flags Flags) {
	cdpFlags := cdphandlers.Flags{
		APIKey:      flags.APIKey,
		Region:      flags.Region,
		Format:      flags.Format,
		Output:      flags.Output,
		Verbose:     flags.Verbose,
		Database:    flags.Database,
		Status:      flags.Status,
		Priority:    flags.Priority,
		Limit:       flags.Limit,
		WithDetails: flags.WithDetails,
	}
	cdphandlers.HandleUpdateEntityFolder(ctx, client, args, cdpFlags)
}

func handleCDPDeleteEntityFolder(ctx context.Context, client *td.Client, args []string, flags Flags) {
	cdpFlags := cdphandlers.Flags{
		APIKey:      flags.APIKey,
		Region:      flags.Region,
		Format:      flags.Format,
		Output:      flags.Output,
		Verbose:     flags.Verbose,
		Database:    flags.Database,
		Status:      flags.Status,
		Priority:    flags.Priority,
		Limit:       flags.Limit,
		WithDetails: flags.WithDetails,
	}
	cdphandlers.HandleDeleteEntityFolder(ctx, client, args, cdpFlags)
}

func handleCDPGetEntitiesByFolder(ctx context.Context, client *td.Client, args []string, flags Flags) {
	cdpFlags := cdphandlers.Flags{
		APIKey:      flags.APIKey,
		Region:      flags.Region,
		Format:      flags.Format,
		Output:      flags.Output,
		Verbose:     flags.Verbose,
		Database:    flags.Database,
		Status:      flags.Status,
		Priority:    flags.Priority,
		Limit:       flags.Limit,
		WithDetails: flags.WithDetails,
	}
	cdphandlers.HandleGetEntitiesByFolder(ctx, client, args, cdpFlags)
}

func handleCDPListTokens(ctx context.Context, client *td.Client, cmd interface{}, flags Flags) {
	cdpFlags := cdphandlers.Flags{
		APIKey:      flags.APIKey,
		Region:      flags.Region,
		Format:      flags.Format,
		Output:      flags.Output,
		Verbose:     flags.Verbose,
		Database:    flags.Database,
		Status:      flags.Status,
		Priority:    flags.Priority,
		Limit:       flags.Limit,
		WithDetails: flags.WithDetails,
	}
	cdphandlers.HandleListTokens(ctx, client, cmd, cdpFlags)
}

func handleCDPGetEntityToken(ctx context.Context, client *td.Client, args []string, flags Flags) {
	cdpFlags := cdphandlers.Flags{
		APIKey:      flags.APIKey,
		Region:      flags.Region,
		Format:      flags.Format,
		Output:      flags.Output,
		Verbose:     flags.Verbose,
		Database:    flags.Database,
		Status:      flags.Status,
		Priority:    flags.Priority,
		Limit:       flags.Limit,
		WithDetails: flags.WithDetails,
	}
	cdphandlers.HandleGetEntityToken(ctx, client, args, cdpFlags)
}

func handleCDPUpdateEntityToken(ctx context.Context, client *td.Client, args []string, flags Flags) {
	cdpFlags := cdphandlers.Flags{
		APIKey:      flags.APIKey,
		Region:      flags.Region,
		Format:      flags.Format,
		Output:      flags.Output,
		Verbose:     flags.Verbose,
		Database:    flags.Database,
		Status:      flags.Status,
		Priority:    flags.Priority,
		Limit:       flags.Limit,
		WithDetails: flags.WithDetails,
	}
	cdphandlers.HandleUpdateEntityToken(ctx, client, args, cdpFlags)
}

func handleCDPDeleteEntityToken(ctx context.Context, client *td.Client, args []string, flags Flags) {
	cdpFlags := cdphandlers.Flags{
		APIKey:      flags.APIKey,
		Region:      flags.Region,
		Format:      flags.Format,
		Output:      flags.Output,
		Verbose:     flags.Verbose,
		Database:    flags.Database,
		Status:      flags.Status,
		Priority:    flags.Priority,
		Limit:       flags.Limit,
		WithDetails: flags.WithDetails,
	}
	cdphandlers.HandleDeleteEntityToken(ctx, client, args, cdpFlags)
}

// Journey handlers
func handleCDPJourneyList(ctx context.Context, client *td.Client, args []string, flags Flags) {
	cdpFlags := cdphandlers.Flags{
		APIKey:      flags.APIKey,
		Region:      flags.Region,
		Format:      flags.Format,
		Output:      flags.Output,
		Verbose:     flags.Verbose,
		Database:    flags.Database,
		Status:      flags.Status,
		Priority:    flags.Priority,
		Limit:       flags.Limit,
		WithDetails: flags.WithDetails,
	}
	cdphandlers.HandleJourneyList(ctx, client, args, cdpFlags)
}

func handleCDPJourneyCreate(ctx context.Context, client *td.Client, args []string, flags Flags) {
	cdpFlags := cdphandlers.Flags{
		APIKey:      flags.APIKey,
		Region:      flags.Region,
		Format:      flags.Format,
		Output:      flags.Output,
		Verbose:     flags.Verbose,
		Database:    flags.Database,
		Status:      flags.Status,
		Priority:    flags.Priority,
		Limit:       flags.Limit,
		WithDetails: flags.WithDetails,
	}
	cdphandlers.HandleJourneyCreate(ctx, client, args, cdpFlags)
}

func handleCDPJourneyGet(ctx context.Context, client *td.Client, args []string, flags Flags) {
	cdpFlags := cdphandlers.Flags{
		APIKey:      flags.APIKey,
		Region:      flags.Region,
		Format:      flags.Format,
		Output:      flags.Output,
		Verbose:     flags.Verbose,
		Database:    flags.Database,
		Status:      flags.Status,
		Priority:    flags.Priority,
		Limit:       flags.Limit,
		WithDetails: flags.WithDetails,
	}
	cdphandlers.HandleJourneyGet(ctx, client, args, cdpFlags)
}

func handleCDPJourneyUpdate(ctx context.Context, client *td.Client, args []string, flags Flags) {
	cdpFlags := cdphandlers.Flags{
		APIKey:      flags.APIKey,
		Region:      flags.Region,
		Format:      flags.Format,
		Output:      flags.Output,
		Verbose:     flags.Verbose,
		Database:    flags.Database,
		Status:      flags.Status,
		Priority:    flags.Priority,
		Limit:       flags.Limit,
		WithDetails: flags.WithDetails,
	}
	cdphandlers.HandleJourneyUpdate(ctx, client, args, cdpFlags)
}

func handleCDPJourneyDelete(ctx context.Context, client *td.Client, args []string, flags Flags) {
	cdpFlags := cdphandlers.Flags{
		APIKey:      flags.APIKey,
		Region:      flags.Region,
		Format:      flags.Format,
		Output:      flags.Output,
		Verbose:     flags.Verbose,
		Database:    flags.Database,
		Status:      flags.Status,
		Priority:    flags.Priority,
		Limit:       flags.Limit,
		WithDetails: flags.WithDetails,
	}
	cdphandlers.HandleJourneyDelete(ctx, client, args, cdpFlags)
}

func handleCDPJourneyDetail(ctx context.Context, client *td.Client, args []string, flags Flags) {
	cdpFlags := cdphandlers.Flags{
		APIKey:      flags.APIKey,
		Region:      flags.Region,
		Format:      flags.Format,
		Output:      flags.Output,
		Verbose:     flags.Verbose,
		Database:    flags.Database,
		Status:      flags.Status,
		Priority:    flags.Priority,
		Limit:       flags.Limit,
		WithDetails: flags.WithDetails,
	}
	cdphandlers.HandleJourneyDetail(ctx, client, args, cdpFlags)
}

func handleCDPJourneyDuplicate(ctx context.Context, client *td.Client, args []string, flags Flags) {
	cdpFlags := cdphandlers.Flags{
		APIKey:      flags.APIKey,
		Region:      flags.Region,
		Format:      flags.Format,
		Output:      flags.Output,
		Verbose:     flags.Verbose,
		Database:    flags.Database,
		Status:      flags.Status,
		Priority:    flags.Priority,
		Limit:       flags.Limit,
		WithDetails: flags.WithDetails,
	}
	cdphandlers.HandleJourneyDuplicate(ctx, client, args, cdpFlags)
}

func handleCDPJourneyPause(ctx context.Context, client *td.Client, args []string, flags Flags) {
	cdpFlags := cdphandlers.Flags{
		APIKey:      flags.APIKey,
		Region:      flags.Region,
		Format:      flags.Format,
		Output:      flags.Output,
		Verbose:     flags.Verbose,
		Database:    flags.Database,
		Status:      flags.Status,
		Priority:    flags.Priority,
		Limit:       flags.Limit,
		WithDetails: flags.WithDetails,
	}
	cdphandlers.HandleJourneyPause(ctx, client, args, cdpFlags)
}

func handleCDPJourneyResume(ctx context.Context, client *td.Client, args []string, flags Flags) {
	cdpFlags := cdphandlers.Flags{
		APIKey:      flags.APIKey,
		Region:      flags.Region,
		Format:      flags.Format,
		Output:      flags.Output,
		Verbose:     flags.Verbose,
		Database:    flags.Database,
		Status:      flags.Status,
		Priority:    flags.Priority,
		Limit:       flags.Limit,
		WithDetails: flags.WithDetails,
	}
	cdphandlers.HandleJourneyResume(ctx, client, args, cdpFlags)
}

func handleCDPJourneyStatistics(ctx context.Context, client *td.Client, args []string, flags Flags) {
	cdpFlags := cdphandlers.Flags{
		APIKey:      flags.APIKey,
		Region:      flags.Region,
		Format:      flags.Format,
		Output:      flags.Output,
		Verbose:     flags.Verbose,
		Database:    flags.Database,
		Status:      flags.Status,
		Priority:    flags.Priority,
		Limit:       flags.Limit,
		WithDetails: flags.WithDetails,
	}
	cdphandlers.HandleJourneyStatistics(ctx, client, args, cdpFlags)
}

func handleCDPJourneyCustomers(ctx context.Context, client *td.Client, args []string, flags Flags) {
	cdpFlags := cdphandlers.Flags{
		APIKey:      flags.APIKey,
		Region:      flags.Region,
		Format:      flags.Format,
		Output:      flags.Output,
		Verbose:     flags.Verbose,
		Database:    flags.Database,
		Status:      flags.Status,
		Priority:    flags.Priority,
		Limit:       flags.Limit,
		WithDetails: flags.WithDetails,
	}
	cdphandlers.HandleJourneyCustomers(ctx, client, args, cdpFlags)
}

func handleCDPJourneyStageCustomers(ctx context.Context, client *td.Client, args []string, flags Flags) {
	cdpFlags := cdphandlers.Flags{
		APIKey:      flags.APIKey,
		Region:      flags.Region,
		Format:      flags.Format,
		Output:      flags.Output,
		Verbose:     flags.Verbose,
		Database:    flags.Database,
		Status:      flags.Status,
		Priority:    flags.Priority,
		Limit:       flags.Limit,
		WithDetails: flags.WithDetails,
	}
	cdphandlers.HandleJourneyStageCustomers(ctx, client, args, cdpFlags)
}

func handleCDPJourneyConversionSankey(ctx context.Context, client *td.Client, args []string, flags Flags) {
	cdpFlags := cdphandlers.Flags{
		APIKey:      flags.APIKey,
		Region:      flags.Region,
		Format:      flags.Format,
		Output:      flags.Output,
		Verbose:     flags.Verbose,
		Database:    flags.Database,
		Status:      flags.Status,
		Priority:    flags.Priority,
		Limit:       flags.Limit,
		WithDetails: flags.WithDetails,
	}
	cdphandlers.HandleJourneyConversionSankey(ctx, client, args, cdpFlags)
}

func handleCDPJourneyActivationSankey(ctx context.Context, client *td.Client, args []string, flags Flags) {
	cdpFlags := cdphandlers.Flags{
		APIKey:      flags.APIKey,
		Region:      flags.Region,
		Format:      flags.Format,
		Output:      flags.Output,
		Verbose:     flags.Verbose,
		Database:    flags.Database,
		Status:      flags.Status,
		Priority:    flags.Priority,
		Limit:       flags.Limit,
		WithDetails: flags.WithDetails,
	}
	cdphandlers.HandleJourneyActivationSankey(ctx, client, args, cdpFlags)
}

func handleCDPJourneySegmentRules(ctx context.Context, client *td.Client, args []string, flags Flags) {
	cdpFlags := cdphandlers.Flags{
		APIKey:      flags.APIKey,
		Region:      flags.Region,
		Format:      flags.Format,
		Output:      flags.Output,
		Verbose:     flags.Verbose,
		Database:    flags.Database,
		Status:      flags.Status,
		Priority:    flags.Priority,
		Limit:       flags.Limit,
		WithDetails: flags.WithDetails,
	}
	cdphandlers.HandleJourneySegmentRules(ctx, client, args, cdpFlags)
}

func handleCDPJourneyBehaviors(ctx context.Context, client *td.Client, args []string, flags Flags) {
	cdpFlags := cdphandlers.Flags{
		APIKey:      flags.APIKey,
		Region:      flags.Region,
		Format:      flags.Format,
		Output:      flags.Output,
		Verbose:     flags.Verbose,
		Database:    flags.Database,
		Status:      flags.Status,
		Priority:    flags.Priority,
		Limit:       flags.Limit,
		WithDetails: flags.WithDetails,
	}
	cdphandlers.HandleJourneyBehaviors(ctx, client, args, cdpFlags)
}

func handleCDPJourneyTemplates(ctx context.Context, client *td.Client, args []string, flags Flags) {
	cdpFlags := cdphandlers.Flags{
		APIKey:      flags.APIKey,
		Region:      flags.Region,
		Format:      flags.Format,
		Output:      flags.Output,
		Verbose:     flags.Verbose,
		Database:    flags.Database,
		Status:      flags.Status,
		Priority:    flags.Priority,
		Limit:       flags.Limit,
		WithDetails: flags.WithDetails,
	}
	cdphandlers.HandleJourneyTemplates(ctx, client, args, cdpFlags)
}

// Journey Activation handlers
func handleCDPJourneyActivationList(ctx context.Context, client *td.Client, args []string, flags Flags) {
	cdpFlags := cdphandlers.Flags{
		APIKey:      flags.APIKey,
		Region:      flags.Region,
		Format:      flags.Format,
		Output:      flags.Output,
		Verbose:     flags.Verbose,
		Database:    flags.Database,
		Status:      flags.Status,
		Priority:    flags.Priority,
		Limit:       flags.Limit,
		WithDetails: flags.WithDetails,
	}
	cdphandlers.HandleJourneyActivationList(ctx, client, args, cdpFlags)
}

func handleCDPJourneyActivationCreate(ctx context.Context, client *td.Client, args []string, flags Flags) {
	cdpFlags := cdphandlers.Flags{
		APIKey:      flags.APIKey,
		Region:      flags.Region,
		Format:      flags.Format,
		Output:      flags.Output,
		Verbose:     flags.Verbose,
		Database:    flags.Database,
		Status:      flags.Status,
		Priority:    flags.Priority,
		Limit:       flags.Limit,
		WithDetails: flags.WithDetails,
	}
	cdphandlers.HandleJourneyActivationCreate(ctx, client, args, cdpFlags)
}

func handleCDPJourneyActivationGet(ctx context.Context, client *td.Client, args []string, flags Flags) {
	cdpFlags := cdphandlers.Flags{
		APIKey:      flags.APIKey,
		Region:      flags.Region,
		Format:      flags.Format,
		Output:      flags.Output,
		Verbose:     flags.Verbose,
		Database:    flags.Database,
		Status:      flags.Status,
		Priority:    flags.Priority,
		Limit:       flags.Limit,
		WithDetails: flags.WithDetails,
	}
	cdphandlers.HandleJourneyActivationGet(ctx, client, args, cdpFlags)
}

func handleCDPJourneyActivationUpdate(ctx context.Context, client *td.Client, args []string, flags Flags) {
	cdpFlags := cdphandlers.Flags{
		APIKey:      flags.APIKey,
		Region:      flags.Region,
		Format:      flags.Format,
		Output:      flags.Output,
		Verbose:     flags.Verbose,
		Database:    flags.Database,
		Status:      flags.Status,
		Priority:    flags.Priority,
		Limit:       flags.Limit,
		WithDetails: flags.WithDetails,
	}
	cdphandlers.HandleJourneyActivationUpdate(ctx, client, args, cdpFlags)
}

// Activation Template handlers
func handleCDPActivationTemplateList(ctx context.Context, client *td.Client, args []string, flags Flags) {
	cdpFlags := cdphandlers.Flags{
		APIKey:      flags.APIKey,
		Region:      flags.Region,
		Format:      flags.Format,
		Output:      flags.Output,
		Verbose:     flags.Verbose,
		Database:    flags.Database,
		Status:      flags.Status,
		Priority:    flags.Priority,
		Limit:       flags.Limit,
		WithDetails: flags.WithDetails,
	}
	cdphandlers.HandleActivationTemplateList(ctx, client, args, cdpFlags)
}

func handleCDPActivationTemplateCreate(ctx context.Context, client *td.Client, args []string, flags Flags) {
	cdpFlags := cdphandlers.Flags{
		APIKey:      flags.APIKey,
		Region:      flags.Region,
		Format:      flags.Format,
		Output:      flags.Output,
		Verbose:     flags.Verbose,
		Database:    flags.Database,
		Status:      flags.Status,
		Priority:    flags.Priority,
		Limit:       flags.Limit,
		WithDetails: flags.WithDetails,
	}
	cdphandlers.HandleActivationTemplateCreate(ctx, client, args, cdpFlags)
}

func handleCDPActivationTemplateGet(ctx context.Context, client *td.Client, args []string, flags Flags) {
	cdpFlags := cdphandlers.Flags{
		APIKey:      flags.APIKey,
		Region:      flags.Region,
		Format:      flags.Format,
		Output:      flags.Output,
		Verbose:     flags.Verbose,
		Database:    flags.Database,
		Status:      flags.Status,
		Priority:    flags.Priority,
		Limit:       flags.Limit,
		WithDetails: flags.WithDetails,
	}
	cdphandlers.HandleActivationTemplateGet(ctx, client, args, cdpFlags)
}

func handleCDPActivationTemplateUpdate(ctx context.Context, client *td.Client, args []string, flags Flags) {
	cdpFlags := cdphandlers.Flags{
		APIKey:      flags.APIKey,
		Region:      flags.Region,
		Format:      flags.Format,
		Output:      flags.Output,
		Verbose:     flags.Verbose,
		Database:    flags.Database,
		Status:      flags.Status,
		Priority:    flags.Priority,
		Limit:       flags.Limit,
		WithDetails: flags.WithDetails,
	}
	cdphandlers.HandleActivationTemplateUpdate(ctx, client, args, cdpFlags)
}

func handleCDPActivationTemplateDelete(ctx context.Context, client *td.Client, args []string, flags Flags) {
	cdpFlags := cdphandlers.Flags{
		APIKey:      flags.APIKey,
		Region:      flags.Region,
		Format:      flags.Format,
		Output:      flags.Output,
		Verbose:     flags.Verbose,
		Database:    flags.Database,
		Status:      flags.Status,
		Priority:    flags.Priority,
		Limit:       flags.Limit,
		WithDetails: flags.WithDetails,
	}
	cdphandlers.HandleActivationTemplateDelete(ctx, client, args, cdpFlags)
}
