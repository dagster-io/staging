// @generated
/* tslint:disable */
/* eslint-disable */
// @generated
// This file was automatically generated and should not be edited.

import { PartitionRunStatus, JobType, JobStatus, PipelineRunStatus, JobTickStatus } from "./../../types/globalTypes";

// ====================================================
// GraphQL query operation: SchedulerRootQuery
// ====================================================

export interface SchedulerRootQuery_repositoriesOrError_RepositoryConnection_nodes_origin_repositoryLocationMetadata {
  __typename: "RepositoryMetadata";
  key: string;
  value: string;
}

export interface SchedulerRootQuery_repositoriesOrError_RepositoryConnection_nodes_origin {
  __typename: "RepositoryOrigin";
  repositoryLocationName: string;
  repositoryName: string;
  repositoryLocationMetadata: SchedulerRootQuery_repositoriesOrError_RepositoryConnection_nodes_origin_repositoryLocationMetadata[];
}

export interface SchedulerRootQuery_repositoriesOrError_RepositoryConnection_nodes_location {
  __typename: "RepositoryLocation";
  id: string;
  name: string;
}

export interface SchedulerRootQuery_repositoriesOrError_RepositoryConnection_nodes_schedules_partitionSet_partitionsOrError_PythonError {
  __typename: "PythonError";
}

export interface SchedulerRootQuery_repositoriesOrError_RepositoryConnection_nodes_schedules_partitionSet_partitionsOrError_Partitions_results {
  __typename: "Partition";
  name: string;
  status: PartitionRunStatus;
}

export interface SchedulerRootQuery_repositoriesOrError_RepositoryConnection_nodes_schedules_partitionSet_partitionsOrError_Partitions {
  __typename: "Partitions";
  results: SchedulerRootQuery_repositoriesOrError_RepositoryConnection_nodes_schedules_partitionSet_partitionsOrError_Partitions_results[];
}

export type SchedulerRootQuery_repositoriesOrError_RepositoryConnection_nodes_schedules_partitionSet_partitionsOrError = SchedulerRootQuery_repositoriesOrError_RepositoryConnection_nodes_schedules_partitionSet_partitionsOrError_PythonError | SchedulerRootQuery_repositoriesOrError_RepositoryConnection_nodes_schedules_partitionSet_partitionsOrError_Partitions;

export interface SchedulerRootQuery_repositoriesOrError_RepositoryConnection_nodes_schedules_partitionSet {
  __typename: "PartitionSet";
  name: string;
  partitionsOrError: SchedulerRootQuery_repositoriesOrError_RepositoryConnection_nodes_schedules_partitionSet_partitionsOrError;
}

export interface SchedulerRootQuery_repositoriesOrError_RepositoryConnection_nodes_schedules_scheduleState_repositoryOrigin_repositoryLocationMetadata {
  __typename: "RepositoryMetadata";
  key: string;
  value: string;
}

export interface SchedulerRootQuery_repositoriesOrError_RepositoryConnection_nodes_schedules_scheduleState_repositoryOrigin {
  __typename: "RepositoryOrigin";
  repositoryLocationName: string;
  repositoryName: string;
  repositoryLocationMetadata: SchedulerRootQuery_repositoriesOrError_RepositoryConnection_nodes_schedules_scheduleState_repositoryOrigin_repositoryLocationMetadata[];
}

export interface SchedulerRootQuery_repositoriesOrError_RepositoryConnection_nodes_schedules_scheduleState_jobSpecificData_SensorJobData {
  __typename: "SensorJobData";
  lastRunKey: string | null;
}

export interface SchedulerRootQuery_repositoriesOrError_RepositoryConnection_nodes_schedules_scheduleState_jobSpecificData_ScheduleJobData {
  __typename: "ScheduleJobData";
  cronSchedule: string;
}

export type SchedulerRootQuery_repositoriesOrError_RepositoryConnection_nodes_schedules_scheduleState_jobSpecificData = SchedulerRootQuery_repositoriesOrError_RepositoryConnection_nodes_schedules_scheduleState_jobSpecificData_SensorJobData | SchedulerRootQuery_repositoriesOrError_RepositoryConnection_nodes_schedules_scheduleState_jobSpecificData_ScheduleJobData;

export interface SchedulerRootQuery_repositoriesOrError_RepositoryConnection_nodes_schedules_scheduleState_runs {
  __typename: "PipelineRun";
  id: string;
  runId: string;
  status: PipelineRunStatus;
}

export interface SchedulerRootQuery_repositoriesOrError_RepositoryConnection_nodes_schedules_scheduleState_ticks_error_cause {
  __typename: "PythonError";
  message: string;
  stack: string[];
}

export interface SchedulerRootQuery_repositoriesOrError_RepositoryConnection_nodes_schedules_scheduleState_ticks_error {
  __typename: "PythonError";
  message: string;
  stack: string[];
  cause: SchedulerRootQuery_repositoriesOrError_RepositoryConnection_nodes_schedules_scheduleState_ticks_error_cause | null;
}

export interface SchedulerRootQuery_repositoriesOrError_RepositoryConnection_nodes_schedules_scheduleState_ticks {
  __typename: "JobTick";
  id: string;
  status: JobTickStatus;
  timestamp: number;
  skipReason: string | null;
  runIds: string[];
  error: SchedulerRootQuery_repositoriesOrError_RepositoryConnection_nodes_schedules_scheduleState_ticks_error | null;
}

export interface SchedulerRootQuery_repositoriesOrError_RepositoryConnection_nodes_schedules_scheduleState {
  __typename: "JobState";
  id: string;
  name: string;
  jobType: JobType;
  status: JobStatus;
  repositoryOrigin: SchedulerRootQuery_repositoriesOrError_RepositoryConnection_nodes_schedules_scheduleState_repositoryOrigin;
  jobSpecificData: SchedulerRootQuery_repositoriesOrError_RepositoryConnection_nodes_schedules_scheduleState_jobSpecificData | null;
  runs: SchedulerRootQuery_repositoriesOrError_RepositoryConnection_nodes_schedules_scheduleState_runs[];
  ticks: SchedulerRootQuery_repositoriesOrError_RepositoryConnection_nodes_schedules_scheduleState_ticks[];
  runningCount: number;
}

export interface SchedulerRootQuery_repositoriesOrError_RepositoryConnection_nodes_schedules_futureTicks_results {
  __typename: "FutureJobTick";
  timestamp: number;
}

export interface SchedulerRootQuery_repositoriesOrError_RepositoryConnection_nodes_schedules_futureTicks {
  __typename: "FutureJobTicks";
  results: SchedulerRootQuery_repositoriesOrError_RepositoryConnection_nodes_schedules_futureTicks_results[];
}

export interface SchedulerRootQuery_repositoriesOrError_RepositoryConnection_nodes_schedules {
  __typename: "Schedule";
  id: string;
  name: string;
  cronSchedule: string;
  executionTimezone: string | null;
  pipelineName: string;
  solidSelection: (string | null)[] | null;
  mode: string;
  partitionSet: SchedulerRootQuery_repositoriesOrError_RepositoryConnection_nodes_schedules_partitionSet | null;
  scheduleState: SchedulerRootQuery_repositoriesOrError_RepositoryConnection_nodes_schedules_scheduleState;
  futureTicks: SchedulerRootQuery_repositoriesOrError_RepositoryConnection_nodes_schedules_futureTicks;
}

export interface SchedulerRootQuery_repositoriesOrError_RepositoryConnection_nodes {
  __typename: "Repository";
  id: string;
  name: string;
  origin: SchedulerRootQuery_repositoriesOrError_RepositoryConnection_nodes_origin;
  location: SchedulerRootQuery_repositoriesOrError_RepositoryConnection_nodes_location;
  schedules: SchedulerRootQuery_repositoriesOrError_RepositoryConnection_nodes_schedules[];
}

export interface SchedulerRootQuery_repositoriesOrError_RepositoryConnection {
  __typename: "RepositoryConnection";
  nodes: SchedulerRootQuery_repositoriesOrError_RepositoryConnection_nodes[];
}

export interface SchedulerRootQuery_repositoriesOrError_PythonError_cause {
  __typename: "PythonError";
  message: string;
  stack: string[];
}

export interface SchedulerRootQuery_repositoriesOrError_PythonError {
  __typename: "PythonError";
  message: string;
  stack: string[];
  cause: SchedulerRootQuery_repositoriesOrError_PythonError_cause | null;
}

export type SchedulerRootQuery_repositoriesOrError = SchedulerRootQuery_repositoriesOrError_RepositoryConnection | SchedulerRootQuery_repositoriesOrError_PythonError;

export interface SchedulerRootQuery_scheduler_SchedulerNotDefinedError {
  __typename: "SchedulerNotDefinedError";
  message: string;
}

export interface SchedulerRootQuery_scheduler_Scheduler {
  __typename: "Scheduler";
  schedulerClass: string | null;
}

export interface SchedulerRootQuery_scheduler_PythonError_cause {
  __typename: "PythonError";
  message: string;
  stack: string[];
}

export interface SchedulerRootQuery_scheduler_PythonError {
  __typename: "PythonError";
  message: string;
  stack: string[];
  cause: SchedulerRootQuery_scheduler_PythonError_cause | null;
}

export type SchedulerRootQuery_scheduler = SchedulerRootQuery_scheduler_SchedulerNotDefinedError | SchedulerRootQuery_scheduler_Scheduler | SchedulerRootQuery_scheduler_PythonError;

export interface SchedulerRootQuery_unloadableJobStatesOrError_JobStates_results_repositoryOrigin_repositoryLocationMetadata {
  __typename: "RepositoryMetadata";
  key: string;
  value: string;
}

export interface SchedulerRootQuery_unloadableJobStatesOrError_JobStates_results_repositoryOrigin {
  __typename: "RepositoryOrigin";
  repositoryLocationName: string;
  repositoryName: string;
  repositoryLocationMetadata: SchedulerRootQuery_unloadableJobStatesOrError_JobStates_results_repositoryOrigin_repositoryLocationMetadata[];
}

export interface SchedulerRootQuery_unloadableJobStatesOrError_JobStates_results_jobSpecificData_SensorJobData {
  __typename: "SensorJobData";
  lastRunKey: string | null;
}

export interface SchedulerRootQuery_unloadableJobStatesOrError_JobStates_results_jobSpecificData_ScheduleJobData {
  __typename: "ScheduleJobData";
  cronSchedule: string;
}

export type SchedulerRootQuery_unloadableJobStatesOrError_JobStates_results_jobSpecificData = SchedulerRootQuery_unloadableJobStatesOrError_JobStates_results_jobSpecificData_SensorJobData | SchedulerRootQuery_unloadableJobStatesOrError_JobStates_results_jobSpecificData_ScheduleJobData;

export interface SchedulerRootQuery_unloadableJobStatesOrError_JobStates_results_runs {
  __typename: "PipelineRun";
  id: string;
  runId: string;
  status: PipelineRunStatus;
}

export interface SchedulerRootQuery_unloadableJobStatesOrError_JobStates_results_ticks_error_cause {
  __typename: "PythonError";
  message: string;
  stack: string[];
}

export interface SchedulerRootQuery_unloadableJobStatesOrError_JobStates_results_ticks_error {
  __typename: "PythonError";
  message: string;
  stack: string[];
  cause: SchedulerRootQuery_unloadableJobStatesOrError_JobStates_results_ticks_error_cause | null;
}

export interface SchedulerRootQuery_unloadableJobStatesOrError_JobStates_results_ticks {
  __typename: "JobTick";
  id: string;
  status: JobTickStatus;
  timestamp: number;
  skipReason: string | null;
  runIds: string[];
  error: SchedulerRootQuery_unloadableJobStatesOrError_JobStates_results_ticks_error | null;
}

export interface SchedulerRootQuery_unloadableJobStatesOrError_JobStates_results {
  __typename: "JobState";
  id: string;
  name: string;
  jobType: JobType;
  status: JobStatus;
  repositoryOrigin: SchedulerRootQuery_unloadableJobStatesOrError_JobStates_results_repositoryOrigin;
  jobSpecificData: SchedulerRootQuery_unloadableJobStatesOrError_JobStates_results_jobSpecificData | null;
  runs: SchedulerRootQuery_unloadableJobStatesOrError_JobStates_results_runs[];
  ticks: SchedulerRootQuery_unloadableJobStatesOrError_JobStates_results_ticks[];
  runningCount: number;
}

export interface SchedulerRootQuery_unloadableJobStatesOrError_JobStates {
  __typename: "JobStates";
  results: SchedulerRootQuery_unloadableJobStatesOrError_JobStates_results[];
}

export interface SchedulerRootQuery_unloadableJobStatesOrError_PythonError_cause {
  __typename: "PythonError";
  message: string;
  stack: string[];
}

export interface SchedulerRootQuery_unloadableJobStatesOrError_PythonError {
  __typename: "PythonError";
  message: string;
  stack: string[];
  cause: SchedulerRootQuery_unloadableJobStatesOrError_PythonError_cause | null;
}

export type SchedulerRootQuery_unloadableJobStatesOrError = SchedulerRootQuery_unloadableJobStatesOrError_JobStates | SchedulerRootQuery_unloadableJobStatesOrError_PythonError;

export interface SchedulerRootQuery {
  repositoriesOrError: SchedulerRootQuery_repositoriesOrError;
  scheduler: SchedulerRootQuery_scheduler;
  unloadableJobStatesOrError: SchedulerRootQuery_unloadableJobStatesOrError;
}
