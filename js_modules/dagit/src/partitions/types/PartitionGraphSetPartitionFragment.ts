// @generated
/* tslint:disable */
/* eslint-disable */
// This file was automatically generated and should not be edited.

import { PipelineRunStatus } from "./../../types/globalTypes";

// ====================================================
// GraphQL fragment: PartitionGraphSetPartitionFragment
// ====================================================

export interface PartitionGraphSetPartitionFragment_runs_tags {
  __typename: "PipelineTag";
  key: string;
  value: string;
}

export interface PartitionGraphSetPartitionFragment_runs_stats_PythonError {
  __typename: "PythonError";
}

export interface PartitionGraphSetPartitionFragment_runs_stats_PipelineRunStatsSnapshot {
  __typename: "PipelineRunStatsSnapshot";
  startTime: number | null;
  endTime: number | null;
  materializations: number;
}

export type PartitionGraphSetPartitionFragment_runs_stats = PartitionGraphSetPartitionFragment_runs_stats_PythonError | PartitionGraphSetPartitionFragment_runs_stats_PipelineRunStatsSnapshot;

export interface PartitionGraphSetPartitionFragment_runs_stepStats_materializations {
  __typename: "Materialization";
}

export interface PartitionGraphSetPartitionFragment_runs_stepStats_expectationResults {
  __typename: "ExpectationResult";
  success: boolean;
}

export interface PartitionGraphSetPartitionFragment_runs_stepStats {
  __typename: "PipelineRunStepStats";
  startTime: number | null;
  endTime: number | null;
  stepKey: string;
  materializations: PartitionGraphSetPartitionFragment_runs_stepStats_materializations[];
  expectationResults: PartitionGraphSetPartitionFragment_runs_stepStats_expectationResults[];
}

export interface PartitionGraphSetPartitionFragment_runs {
  __typename: "PipelineRun";
  runId: string;
  status: PipelineRunStatus;
  tags: PartitionGraphSetPartitionFragment_runs_tags[];
  stats: PartitionGraphSetPartitionFragment_runs_stats;
  stepStats: PartitionGraphSetPartitionFragment_runs_stepStats[];
}

export interface PartitionGraphSetPartitionFragment {
  __typename: "Partition";
  name: string;
  runs: PartitionGraphSetPartitionFragment_runs[];
}
