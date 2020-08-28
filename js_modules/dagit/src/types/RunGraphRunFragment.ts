// @generated
/* tslint:disable */
/* eslint-disable */
// This file was automatically generated and should not be edited.

// ====================================================
// GraphQL fragment: RunGraphRunFragment
// ====================================================

export interface RunGraphRunFragment_stats_PipelineRunStatsSnapshot {
  __typename: "PipelineRunStatsSnapshot";
  startTime: number | null;
  endTime: number | null;
}

export interface RunGraphRunFragment_stats_PythonError_cause {
  __typename: "PythonError";
  message: string;
  stack: string[];
}

export interface RunGraphRunFragment_stats_PythonError {
  __typename: "PythonError";
  message: string;
  stack: string[];
  cause: RunGraphRunFragment_stats_PythonError_cause | null;
}

export type RunGraphRunFragment_stats = RunGraphRunFragment_stats_PipelineRunStatsSnapshot | RunGraphRunFragment_stats_PythonError;

export interface RunGraphRunFragment_stepStats {
  __typename: "PipelineRunStepStats";
  stepKey: string;
  startTime: number | null;
  endTime: number | null;
}

export interface RunGraphRunFragment {
  __typename: "PipelineRun";
  runId: string;
  stats: RunGraphRunFragment_stats;
  stepStats: RunGraphRunFragment_stepStats[];
}
