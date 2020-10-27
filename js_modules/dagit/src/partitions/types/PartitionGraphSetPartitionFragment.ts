// @generated
/* tslint:disable */
/* eslint-disable */
// This file was automatically generated and should not be edited.

import { StepEventStatus } from "./../../types/globalTypes";

// ====================================================
// GraphQL fragment: PartitionGraphSetPartitionFragment
// ====================================================

export interface PartitionGraphSetPartitionFragment_runs_stats_PipelineRunStatsSnapshot {
  __typename: "PipelineRunStatsSnapshot";
  startTime: number | null;
  endTime: number | null;
  materializations: number;
}

export interface PartitionGraphSetPartitionFragment_runs_stats_PythonError_cause {
  __typename: "PythonError";
  message: string;
  stack: string[];
}

export interface PartitionGraphSetPartitionFragment_runs_stats_PythonError {
  __typename: "PythonError";
  message: string;
  stack: string[];
  cause: PartitionGraphSetPartitionFragment_runs_stats_PythonError_cause | null;
}

export type PartitionGraphSetPartitionFragment_runs_stats = PartitionGraphSetPartitionFragment_runs_stats_PipelineRunStatsSnapshot | PartitionGraphSetPartitionFragment_runs_stats_PythonError;

export interface PartitionGraphSetPartitionFragment_runs_stepStats_materializations {
  __typename: "Materialization";
}

export interface PartitionGraphSetPartitionFragment_runs_stepStats_expectationResults {
  __typename: "ExpectationResult";
  success: boolean;
}

export interface PartitionGraphSetPartitionFragment_runs_stepStats {
  __typename: "PipelineRunStepStats";
  stepKey: string;
  startTime: number | null;
  endTime: number | null;
  status: StepEventStatus | null;
  materializations: PartitionGraphSetPartitionFragment_runs_stepStats_materializations[];
  expectationResults: PartitionGraphSetPartitionFragment_runs_stepStats_expectationResults[];
}

export interface PartitionGraphSetPartitionFragment_runs {
  __typename: "PipelineRun";
  runId: string;
  stats: PartitionGraphSetPartitionFragment_runs_stats;
  stepStats: PartitionGraphSetPartitionFragment_runs_stepStats[];
}

export interface PartitionGraphSetPartitionFragment {
  __typename: "Partition";
  name: string;
  runs: PartitionGraphSetPartitionFragment_runs[];
}
