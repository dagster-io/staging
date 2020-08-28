// @generated
/* tslint:disable */
/* eslint-disable */
// This file was automatically generated and should not be edited.

import { RepositorySelector, PipelineRunStatus, StepEventStatus } from "./globalTypes";

// ====================================================
// GraphQL query operation: TriggersRootQuery
// ====================================================

export interface TriggersRootQuery_triggerDefinitionsOrError_RepositoryNotFoundError {
  __typename: "RepositoryNotFoundError";
}

export interface TriggersRootQuery_triggerDefinitionsOrError_TriggerDefinitions_results_runs_tags {
  __typename: "PipelineTag";
  key: string;
  value: string;
}

export interface TriggersRootQuery_triggerDefinitionsOrError_TriggerDefinitions_results_runs_stepStats_materializations {
  __typename: "Materialization";
}

export interface TriggersRootQuery_triggerDefinitionsOrError_TriggerDefinitions_results_runs_stepStats_expectationResults {
  __typename: "ExpectationResult";
  success: boolean;
}

export interface TriggersRootQuery_triggerDefinitionsOrError_TriggerDefinitions_results_runs_stepStats {
  __typename: "PipelineRunStepStats";
  stepKey: string;
  startTime: number | null;
  endTime: number | null;
  status: StepEventStatus | null;
  materializations: TriggersRootQuery_triggerDefinitionsOrError_TriggerDefinitions_results_runs_stepStats_materializations[];
  expectationResults: TriggersRootQuery_triggerDefinitionsOrError_TriggerDefinitions_results_runs_stepStats_expectationResults[];
}

export interface TriggersRootQuery_triggerDefinitionsOrError_TriggerDefinitions_results_runs_stats_PipelineRunStatsSnapshot {
  __typename: "PipelineRunStatsSnapshot";
  startTime: number | null;
  endTime: number | null;
  materializations: number;
}

export interface TriggersRootQuery_triggerDefinitionsOrError_TriggerDefinitions_results_runs_stats_PythonError_cause {
  __typename: "PythonError";
  message: string;
  stack: string[];
}

export interface TriggersRootQuery_triggerDefinitionsOrError_TriggerDefinitions_results_runs_stats_PythonError {
  __typename: "PythonError";
  message: string;
  stack: string[];
  cause: TriggersRootQuery_triggerDefinitionsOrError_TriggerDefinitions_results_runs_stats_PythonError_cause | null;
}

export type TriggersRootQuery_triggerDefinitionsOrError_TriggerDefinitions_results_runs_stats = TriggersRootQuery_triggerDefinitionsOrError_TriggerDefinitions_results_runs_stats_PipelineRunStatsSnapshot | TriggersRootQuery_triggerDefinitionsOrError_TriggerDefinitions_results_runs_stats_PythonError;

export interface TriggersRootQuery_triggerDefinitionsOrError_TriggerDefinitions_results_runs {
  __typename: "PipelineRun";
  runId: string;
  tags: TriggersRootQuery_triggerDefinitionsOrError_TriggerDefinitions_results_runs_tags[];
  pipelineName: string;
  status: PipelineRunStatus;
  stepStats: TriggersRootQuery_triggerDefinitionsOrError_TriggerDefinitions_results_runs_stepStats[];
  stepKeysToExecute: string[] | null;
  canTerminate: boolean;
  mode: string;
  rootRunId: string | null;
  parentRunId: string | null;
  pipelineSnapshotId: string | null;
  solidSelection: string[] | null;
  stats: TriggersRootQuery_triggerDefinitionsOrError_TriggerDefinitions_results_runs_stats;
}

export interface TriggersRootQuery_triggerDefinitionsOrError_TriggerDefinitions_results {
  __typename: "TriggerDefinition";
  name: string;
  pipelineName: string;
  solidSelection: (string | null)[] | null;
  mode: string;
  runs: TriggersRootQuery_triggerDefinitionsOrError_TriggerDefinitions_results_runs[];
}

export interface TriggersRootQuery_triggerDefinitionsOrError_TriggerDefinitions {
  __typename: "TriggerDefinitions";
  results: TriggersRootQuery_triggerDefinitionsOrError_TriggerDefinitions_results[];
}

export interface TriggersRootQuery_triggerDefinitionsOrError_PythonError_cause {
  __typename: "PythonError";
  message: string;
  stack: string[];
}

export interface TriggersRootQuery_triggerDefinitionsOrError_PythonError {
  __typename: "PythonError";
  message: string;
  stack: string[];
  cause: TriggersRootQuery_triggerDefinitionsOrError_PythonError_cause | null;
}

export type TriggersRootQuery_triggerDefinitionsOrError = TriggersRootQuery_triggerDefinitionsOrError_RepositoryNotFoundError | TriggersRootQuery_triggerDefinitionsOrError_TriggerDefinitions | TriggersRootQuery_triggerDefinitionsOrError_PythonError;

export interface TriggersRootQuery {
  triggerDefinitionsOrError: TriggersRootQuery_triggerDefinitionsOrError;
}

export interface TriggersRootQueryVariables {
  repositorySelector: RepositorySelector;
}
