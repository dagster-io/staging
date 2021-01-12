// @generated
/* tslint:disable */
/* eslint-disable */
// @generated
// This file was automatically generated and should not be edited.

import { PipelineRunStatus, JobType, JobStatus, JobTickStatus } from "./../../types/globalTypes";

// ====================================================
// GraphQL fragment: UpcomingTicksFragment
// ====================================================

export interface UpcomingTicksFragment_RepositoryConnection_nodes_location {
  __typename: "RepositoryLocation";
  id: string;
  name: string;
}

export interface UpcomingTicksFragment_RepositoryConnection_nodes_schedules_partitionSet_partitionStatusesOrError_PythonError {
  __typename: "PythonError";
}

export interface UpcomingTicksFragment_RepositoryConnection_nodes_schedules_partitionSet_partitionStatusesOrError_PartitionStatuses_results {
  __typename: "PartitionStatus";
  id: string;
  partitionName: string;
  runStatus: PipelineRunStatus | null;
}

export interface UpcomingTicksFragment_RepositoryConnection_nodes_schedules_partitionSet_partitionStatusesOrError_PartitionStatuses {
  __typename: "PartitionStatuses";
  results: UpcomingTicksFragment_RepositoryConnection_nodes_schedules_partitionSet_partitionStatusesOrError_PartitionStatuses_results[];
}

export type UpcomingTicksFragment_RepositoryConnection_nodes_schedules_partitionSet_partitionStatusesOrError = UpcomingTicksFragment_RepositoryConnection_nodes_schedules_partitionSet_partitionStatusesOrError_PythonError | UpcomingTicksFragment_RepositoryConnection_nodes_schedules_partitionSet_partitionStatusesOrError_PartitionStatuses;

export interface UpcomingTicksFragment_RepositoryConnection_nodes_schedules_partitionSet {
  __typename: "PartitionSet";
  id: string;
  name: string;
  partitionStatusesOrError: UpcomingTicksFragment_RepositoryConnection_nodes_schedules_partitionSet_partitionStatusesOrError;
}

export interface UpcomingTicksFragment_RepositoryConnection_nodes_schedules_scheduleState_repositoryOrigin_repositoryLocationMetadata {
  __typename: "RepositoryMetadata";
  key: string;
  value: string;
}

export interface UpcomingTicksFragment_RepositoryConnection_nodes_schedules_scheduleState_repositoryOrigin {
  __typename: "RepositoryOrigin";
  repositoryLocationName: string;
  repositoryName: string;
  repositoryLocationMetadata: UpcomingTicksFragment_RepositoryConnection_nodes_schedules_scheduleState_repositoryOrigin_repositoryLocationMetadata[];
}

export interface UpcomingTicksFragment_RepositoryConnection_nodes_schedules_scheduleState_jobSpecificData_SensorJobData {
  __typename: "SensorJobData";
  lastRunKey: string | null;
}

export interface UpcomingTicksFragment_RepositoryConnection_nodes_schedules_scheduleState_jobSpecificData_ScheduleJobData {
  __typename: "ScheduleJobData";
  cronSchedule: string;
}

export type UpcomingTicksFragment_RepositoryConnection_nodes_schedules_scheduleState_jobSpecificData = UpcomingTicksFragment_RepositoryConnection_nodes_schedules_scheduleState_jobSpecificData_SensorJobData | UpcomingTicksFragment_RepositoryConnection_nodes_schedules_scheduleState_jobSpecificData_ScheduleJobData;

export interface UpcomingTicksFragment_RepositoryConnection_nodes_schedules_scheduleState_runs {
  __typename: "PipelineRun";
  id: string;
  runId: string;
  status: PipelineRunStatus;
}

export interface UpcomingTicksFragment_RepositoryConnection_nodes_schedules_scheduleState_ticks_error_cause {
  __typename: "PythonError";
  message: string;
  stack: string[];
}

export interface UpcomingTicksFragment_RepositoryConnection_nodes_schedules_scheduleState_ticks_error {
  __typename: "PythonError";
  message: string;
  stack: string[];
  cause: UpcomingTicksFragment_RepositoryConnection_nodes_schedules_scheduleState_ticks_error_cause | null;
}

export interface UpcomingTicksFragment_RepositoryConnection_nodes_schedules_scheduleState_ticks {
  __typename: "JobTick";
  id: string;
  status: JobTickStatus;
  timestamp: number;
  skipReason: string | null;
  runIds: string[];
  error: UpcomingTicksFragment_RepositoryConnection_nodes_schedules_scheduleState_ticks_error | null;
}

export interface UpcomingTicksFragment_RepositoryConnection_nodes_schedules_scheduleState {
  __typename: "JobState";
  id: string;
  name: string;
  jobType: JobType;
  status: JobStatus;
  repositoryOrigin: UpcomingTicksFragment_RepositoryConnection_nodes_schedules_scheduleState_repositoryOrigin;
  jobSpecificData: UpcomingTicksFragment_RepositoryConnection_nodes_schedules_scheduleState_jobSpecificData | null;
  runs: UpcomingTicksFragment_RepositoryConnection_nodes_schedules_scheduleState_runs[];
  ticks: UpcomingTicksFragment_RepositoryConnection_nodes_schedules_scheduleState_ticks[];
  runningCount: number;
}

export interface UpcomingTicksFragment_RepositoryConnection_nodes_schedules_futureTicks_results {
  __typename: "FutureJobTick";
  timestamp: number;
}

export interface UpcomingTicksFragment_RepositoryConnection_nodes_schedules_futureTicks {
  __typename: "FutureJobTicks";
  results: UpcomingTicksFragment_RepositoryConnection_nodes_schedules_futureTicks_results[];
}

export interface UpcomingTicksFragment_RepositoryConnection_nodes_schedules {
  __typename: "Schedule";
  id: string;
  name: string;
  cronSchedule: string;
  executionTimezone: string | null;
  pipelineName: string;
  solidSelection: (string | null)[] | null;
  mode: string;
  partitionSet: UpcomingTicksFragment_RepositoryConnection_nodes_schedules_partitionSet | null;
  scheduleState: UpcomingTicksFragment_RepositoryConnection_nodes_schedules_scheduleState;
  futureTicks: UpcomingTicksFragment_RepositoryConnection_nodes_schedules_futureTicks;
}

export interface UpcomingTicksFragment_RepositoryConnection_nodes_origin_repositoryLocationMetadata {
  __typename: "RepositoryMetadata";
  key: string;
  value: string;
}

export interface UpcomingTicksFragment_RepositoryConnection_nodes_origin {
  __typename: "RepositoryOrigin";
  repositoryLocationName: string;
  repositoryName: string;
  repositoryLocationMetadata: UpcomingTicksFragment_RepositoryConnection_nodes_origin_repositoryLocationMetadata[];
}

export interface UpcomingTicksFragment_RepositoryConnection_nodes {
  __typename: "Repository";
  id: string;
  name: string;
  location: UpcomingTicksFragment_RepositoryConnection_nodes_location;
  schedules: UpcomingTicksFragment_RepositoryConnection_nodes_schedules[];
  origin: UpcomingTicksFragment_RepositoryConnection_nodes_origin;
}

export interface UpcomingTicksFragment_RepositoryConnection {
  __typename: "RepositoryConnection";
  nodes: UpcomingTicksFragment_RepositoryConnection_nodes[];
}

export interface UpcomingTicksFragment_PythonError_cause {
  __typename: "PythonError";
  message: string;
  stack: string[];
}

export interface UpcomingTicksFragment_PythonError {
  __typename: "PythonError";
  message: string;
  stack: string[];
  cause: UpcomingTicksFragment_PythonError_cause | null;
}

export type UpcomingTicksFragment = UpcomingTicksFragment_RepositoryConnection | UpcomingTicksFragment_PythonError;
