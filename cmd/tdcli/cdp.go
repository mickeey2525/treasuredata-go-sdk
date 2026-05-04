package main

import (
	"context"
	"errors"

	td "github.com/mickeey2525/treasuredata-go-sdk"
	cdphandlers "github.com/mickeey2525/treasuredata-go-sdk/cmd/tdcli/cdp"
)

// Helper function to build CDP flags from main flags
func buildCDPFlags(flags Flags) cdphandlers.Flags {
	return cdphandlers.Flags{
		APIKey:             flags.APIKey,
		Region:             flags.Region,
		Format:             flags.Format,
		Output:             flags.Output,
		Verbose:            flags.Verbose,
		Database:           flags.Database,
		Status:             flags.Status,
		Priority:           flags.Priority,
		Limit:              flags.Limit,
		WithDetails:        flags.WithDetails,
		Engine:             flags.Engine,
		InsecureSkipVerify: flags.InsecureSkipVerify,
		CertFile:           flags.CertFile,
		KeyFile:            flags.KeyFile,
		CAFile:             flags.CAFile,
	}
}

// CDP segment handlers
func handleCDPSegmentCreate(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	return cdphandlers.HandleSegmentCreate(ctx, client, args, buildCDPFlags(flags))
}

func handleCDPSegmentList(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	return cdphandlers.HandleSegmentList(ctx, client, args, buildCDPFlags(flags))
}

func handleCDPSegmentGet(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	return cdphandlers.HandleSegmentGet(ctx, client, args, buildCDPFlags(flags))
}

func handleCDPSegmentUpdate(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	return cdphandlers.HandleSegmentUpdate(ctx, client, args, buildCDPFlags(flags))
}

func handleCDPSegmentDelete(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	return cdphandlers.HandleSegmentDelete(ctx, client, args, buildCDPFlags(flags))
}

// CDP audience handlers
func handleCDPAudienceCreate(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	return cdphandlers.HandleAudienceCreate(ctx, client, args, buildCDPFlags(flags))
}

func handleCDPAudienceList(ctx context.Context, client *td.Client, flags Flags) error {
	return cdphandlers.HandleAudienceList(ctx, client, buildCDPFlags(flags))
}

func handleCDPAudienceGet(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	return cdphandlers.HandleAudienceGet(ctx, client, args, buildCDPFlags(flags))
}

func handleCDPAudienceDelete(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	return cdphandlers.HandleAudienceDelete(ctx, client, args, buildCDPFlags(flags))
}

func handleCDPAudienceUpdate(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	return cdphandlers.HandleAudienceUpdate(ctx, client, args, buildCDPFlags(flags))
}

func handleCDPAudienceAttributes(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	return cdphandlers.HandleAudienceAttributes(ctx, client, args, buildCDPFlags(flags))
}

// CDP audience behavior handlers
func handleCDPAudienceBehaviors(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	return cdphandlers.HandleAudienceBehaviors(ctx, client, args, buildCDPFlags(flags))
}

func handleCDPAudienceRun(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	return cdphandlers.HandleAudienceRun(ctx, client, args, buildCDPFlags(flags))
}

func handleCDPAudienceExecutions(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	return cdphandlers.HandleAudienceExecutions(ctx, client, args, buildCDPFlags(flags))
}

func handleCDPAudienceStatistics(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	return cdphandlers.HandleAudienceStatistics(ctx, client, args, buildCDPFlags(flags))
}

func handleCDPAudienceSampleValues(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	return cdphandlers.HandleAudienceSampleValues(ctx, client, args, buildCDPFlags(flags))
}

func handleCDPAudienceBehaviorSamples(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	return cdphandlers.HandleAudienceBehaviorSamples(ctx, client, args, buildCDPFlags(flags))
}

// CDP segment folder handlers
func handleCDPSegmentFolders(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	return cdphandlers.HandleSegmentFolders(ctx, client, args, buildCDPFlags(flags))
}

// Additional segment handlers for query operations
func handleCDPSegmentQuery(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	return cdphandlers.HandleSegmentQuery(ctx, client, args, buildCDPFlags(flags))
}

func handleCDPSegmentNewQuery(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	return cdphandlers.HandleSegmentNewQuery(ctx, client, args, buildCDPFlags(flags))
}

func handleCDPSegmentQueryStatus(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	return cdphandlers.HandleSegmentQueryStatus(ctx, client, args, buildCDPFlags(flags))
}

func handleCDPSegmentKillQuery(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	return cdphandlers.HandleSegmentKillQuery(ctx, client, args, buildCDPFlags(flags))
}

func handleCDPSegmentCustomers(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	return cdphandlers.HandleSegmentCustomers(ctx, client, args, buildCDPFlags(flags))
}

func handleCDPSegmentStatistics(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	return cdphandlers.HandleSegmentStatistics(ctx, client, args, buildCDPFlags(flags))
}

// CDP activation handlers
func handleCDPActivationCreate(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	return cdphandlers.HandleActivationCreate(ctx, client, args, buildCDPFlags(flags))
}

func handleCDPActivationCreateWithStruct(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	return cdphandlers.HandleActivationCreateWithStruct(ctx, client, args, buildCDPFlags(flags))
}

func handleCDPActivationListWithForce(ctx context.Context, client *td.Client, flags Flags, force bool) error {
	return cdphandlers.HandleActivationListWithForce(ctx, client, buildCDPFlags(flags), force)
}

func handleCDPActivationGet(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	return cdphandlers.HandleActivationGet(ctx, client, args, buildCDPFlags(flags))
}

func handleCDPActivationUpdate(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	return cdphandlers.HandleActivationUpdate(ctx, client, args, buildCDPFlags(flags))
}

func handleCDPActivationUpdateStatus(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	return cdphandlers.HandleActivationUpdateStatus(ctx, client, args, buildCDPFlags(flags))
}

func handleCDPActivationDelete(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	return cdphandlers.HandleActivationDelete(ctx, client, args, buildCDPFlags(flags))
}

func handleCDPExecuteActivation(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	return cdphandlers.HandleActivationExecute(ctx, client, args, buildCDPFlags(flags))
}

func handleCDPGetActivationExecutions(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	return cdphandlers.HandleActivationGetExecutions(ctx, client, args, buildCDPFlags(flags))
}

func handleCDPListActivationsByAudience(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	return cdphandlers.HandleActivationListByAudience(ctx, client, args, buildCDPFlags(flags))
}

func handleCDPListActivationsBySegmentFolder(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	return cdphandlers.HandleActivationListBySegmentFolder(ctx, client, args, buildCDPFlags(flags))
}

func handleCDPRunSegmentActivation(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	return cdphandlers.HandleActivationRunForSegment(ctx, client, args, buildCDPFlags(flags))
}

func handleCDPListActivationsByParentSegment(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	return cdphandlers.HandleActivationListByParentSegment(ctx, client, args, buildCDPFlags(flags))
}

func handleCDPGetWorkflowProjectsForParentSegment(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	return cdphandlers.HandleGetWorkflowProjectsForParentSegment(ctx, client, args, buildCDPFlags(flags))
}

func handleCDPGetWorkflowsForParentSegment(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	return cdphandlers.HandleGetWorkflowsForParentSegment(ctx, client, args, buildCDPFlags(flags))
}

func handleCDPGetMatchedActivations(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	return errors.New("get matched activations is not yet implemented")
}

func handleCDPRunActivationForSegment(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	return errors.New("run activation for segment is not yet implemented")
}

func handleCDPGetMatchedActivationsForParentSegment(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	return cdphandlers.HandleGetMatchedActivationsForParentSegment(ctx, client, args, buildCDPFlags(flags))
}

func handleCDPListFolders(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	return cdphandlers.HandleListFolders(ctx, client, args, buildCDPFlags(flags))
}

func handleCDPCreateAudienceFolder(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	return cdphandlers.HandleCreateAudienceFolder(ctx, client, args, buildCDPFlags(flags))
}

func handleCDPGetAudienceFolder(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	return cdphandlers.HandleGetAudienceFolder(ctx, client, args, buildCDPFlags(flags))
}

func handleCDPCreateEntityFolder(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	return cdphandlers.HandleCreateEntityFolder(ctx, client, args, buildCDPFlags(flags))
}

func handleCDPGetEntityFolder(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	return cdphandlers.HandleGetEntityFolder(ctx, client, args, buildCDPFlags(flags))
}

func handleCDPUpdateEntityFolder(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	return cdphandlers.HandleUpdateEntityFolder(ctx, client, args, buildCDPFlags(flags))
}

func handleCDPDeleteEntityFolder(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	return cdphandlers.HandleDeleteEntityFolder(ctx, client, args, buildCDPFlags(flags))
}

func handleCDPGetEntitiesByFolder(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	return cdphandlers.HandleGetEntitiesByFolder(ctx, client, args, buildCDPFlags(flags))
}

func handleCDPListTokens(ctx context.Context, client *td.Client, cmd interface{}, flags Flags) error {
	return cdphandlers.HandleListTokens(ctx, client, cmd, buildCDPFlags(flags))
}

func handleCDPGetEntityToken(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	return cdphandlers.HandleGetEntityToken(ctx, client, args, buildCDPFlags(flags))
}

func handleCDPUpdateEntityToken(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	return cdphandlers.HandleUpdateEntityToken(ctx, client, args, buildCDPFlags(flags))
}

func handleCDPDeleteEntityToken(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	return cdphandlers.HandleDeleteEntityToken(ctx, client, args, buildCDPFlags(flags))
}

// Journey handlers
func handleCDPJourneyList(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	return cdphandlers.HandleJourneyList(ctx, client, args, buildCDPFlags(flags))
}

func handleCDPJourneyCreate(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	return cdphandlers.HandleJourneyCreate(ctx, client, args, buildCDPFlags(flags))
}

func handleCDPJourneyGet(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	return cdphandlers.HandleJourneyGet(ctx, client, args, buildCDPFlags(flags))
}

func handleCDPJourneyUpdate(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	return cdphandlers.HandleJourneyUpdate(ctx, client, args, buildCDPFlags(flags))
}

func handleCDPJourneyDelete(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	return cdphandlers.HandleJourneyDelete(ctx, client, args, buildCDPFlags(flags))
}

func handleCDPJourneyDetail(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	return cdphandlers.HandleJourneyDetail(ctx, client, args, buildCDPFlags(flags))
}

func handleCDPJourneyDuplicate(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	return cdphandlers.HandleJourneyDuplicate(ctx, client, args, buildCDPFlags(flags))
}

func handleCDPJourneyPause(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	return cdphandlers.HandleJourneyPause(ctx, client, args, buildCDPFlags(flags))
}

func handleCDPJourneyResume(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	return cdphandlers.HandleJourneyResume(ctx, client, args, buildCDPFlags(flags))
}

func handleCDPJourneyStatistics(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	return cdphandlers.HandleJourneyStatistics(ctx, client, args, buildCDPFlags(flags))
}

func handleCDPJourneyCustomers(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	return cdphandlers.HandleJourneyCustomers(ctx, client, args, buildCDPFlags(flags))
}

func handleCDPJourneyStageCustomers(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	return cdphandlers.HandleJourneyStageCustomers(ctx, client, args, buildCDPFlags(flags))
}

func handleCDPJourneyConversionSankey(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	return cdphandlers.HandleJourneyConversionSankey(ctx, client, args, buildCDPFlags(flags))
}

func handleCDPJourneyActivationSankey(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	return cdphandlers.HandleJourneyActivationSankey(ctx, client, args, buildCDPFlags(flags))
}

func handleCDPJourneySegmentRules(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	return cdphandlers.HandleJourneySegmentRules(ctx, client, args, buildCDPFlags(flags))
}

func handleCDPJourneyBehaviors(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	return cdphandlers.HandleJourneyBehaviors(ctx, client, args, buildCDPFlags(flags))
}

func handleCDPJourneyTemplates(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	return cdphandlers.HandleJourneyTemplates(ctx, client, args, buildCDPFlags(flags))
}

// Journey Activation handlers
func handleCDPJourneyActivationList(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	return cdphandlers.HandleJourneyActivationList(ctx, client, args, buildCDPFlags(flags))
}

func handleCDPJourneyActivationCreate(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	return cdphandlers.HandleJourneyActivationCreate(ctx, client, args, buildCDPFlags(flags))
}

func handleCDPJourneyActivationGet(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	return cdphandlers.HandleJourneyActivationGet(ctx, client, args, buildCDPFlags(flags))
}

func handleCDPJourneyActivationUpdate(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	return cdphandlers.HandleJourneyActivationUpdate(ctx, client, args, buildCDPFlags(flags))
}

// Activation Template handlers
func handleCDPActivationTemplateList(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	return cdphandlers.HandleActivationTemplateList(ctx, client, args, buildCDPFlags(flags))
}

func handleCDPActivationTemplateCreate(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	return cdphandlers.HandleActivationTemplateCreate(ctx, client, args, buildCDPFlags(flags))
}

func handleCDPActivationTemplateGet(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	return cdphandlers.HandleActivationTemplateGet(ctx, client, args, buildCDPFlags(flags))
}

func handleCDPActivationTemplateUpdate(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	return cdphandlers.HandleActivationTemplateUpdate(ctx, client, args, buildCDPFlags(flags))
}

func handleCDPActivationTemplateDelete(ctx context.Context, client *td.Client, args []string, flags Flags) error {
	return cdphandlers.HandleActivationTemplateDelete(ctx, client, args, buildCDPFlags(flags))
}
