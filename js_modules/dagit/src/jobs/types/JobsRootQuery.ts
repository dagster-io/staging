// @generated
/* tslint:disable */
/* eslint-disable */
// @generated
// This file was automatically generated and should not be edited.

import { RepositorySelector, PartitionRunStatus, JobType, JobStatus, PipelineRunStatus, JobTickStatus } from "./../../types/globalTypes";

// ====================================================
// GraphQL query operation: JobsRootQuery
// ====================================================

export interface JobsRootQuery_repositoryOrError_RepositoryNotFoundError {
  __typename: "RepositoryNotFoundError";
}

export interface JobsRootQuery_repositoryOrError_PythonError_cause {
  __typename: "PythonError";
  message: string;
  stack: string[];
}

export interface JobsRootQuery_repositoryOrError_PythonError {
  __typename: "PythonError";
  message: string;
  stack: string[];
  cause: JobsRootQuery_repositoryOrError_PythonError_cause | null;
}

export interface JobsRootQuery_repositoryOrError_Repository_origin_repositoryLocationMetadata {
  __typename: "RepositoryMetadata";
  key: string;
  value: string;
}

export interface JobsRootQuery_repositoryOrError_Repository_origin {
  __typename: "RepositoryOrigin";
  repositoryLocationName: string;
  repositoryName: string;
  repositoryLocationMetadata: JobsRootQuery_repositoryOrError_Repository_origin_repositoryLocationMetadata[];
}

export interface JobsRootQuery_repositoryOrError_Repository_location {
  __typename: "RepositoryLocation";
  id: string;
  name: string;
}

export interface JobsRootQuery_repositoryOrError_Repository_schedules_partitionSet_partitionsOrError_PythonError {
  __typename: "PythonError";
}

export interface JobsRootQuery_repositoryOrError_Repository_schedules_partitionSet_partitionsOrError_Partitions_results {
  __typename: "Partition";
  name: string;
  status: PartitionRunStatus;
}

export interface JobsRootQuery_repositoryOrError_Repository_schedules_partitionSet_partitionsOrError_Partitions {
  __typename: "Partitions";
  results: JobsRootQuery_repositoryOrError_Repository_schedules_partitionSet_partitionsOrError_Partitions_results[];
}

export type JobsRootQuery_repositoryOrError_Repository_schedules_partitionSet_partitionsOrError = JobsRootQuery_repositoryOrError_Repository_schedules_partitionSet_partitionsOrError_PythonError | JobsRootQuery_repositoryOrError_Repository_schedules_partitionSet_partitionsOrError_Partitions;

export interface JobsRootQuery_repositoryOrError_Repository_schedules_partitionSet {
  __typename: "PartitionSet";
  name: string;
  partitionsOrError: JobsRootQuery_repositoryOrError_Repository_schedules_partitionSet_partitionsOrError;
}

export interface JobsRootQuery_repositoryOrError_Repository_schedules_scheduleState_repositoryOrigin_repositoryLocationMetadata {
  __typename: "RepositoryMetadata";
  key: string;
  value: string;
}

export interface JobsRootQuery_repositoryOrError_Repository_schedules_scheduleState_repositoryOrigin {
  __typename: "RepositoryOrigin";
  repositoryLocationName: string;
  repositoryName: string;
  repositoryLocationMetadata: JobsRootQuery_repositoryOrError_Repository_schedules_scheduleState_repositoryOrigin_repositoryLocationMetadata[];
}

export interface JobsRootQuery_repositoryOrError_Repository_schedules_scheduleState_jobSpecificData_SensorJobData {
  __typename: "SensorJobData";
  lastRunKey: string | null;
}

export interface JobsRootQuery_repositoryOrError_Repository_schedules_scheduleState_jobSpecificData_ScheduleJobData {
  __typename: "ScheduleJobData";
  cronSchedule: string;
}

export type JobsRootQuery_repositoryOrError_Repository_schedules_scheduleState_jobSpecificData = JobsRootQuery_repositoryOrError_Repository_schedules_scheduleState_jobSpecificData_SensorJobData | JobsRootQuery_repositoryOrError_Repository_schedules_scheduleState_jobSpecificData_ScheduleJobData;

export interface JobsRootQuery_repositoryOrError_Repository_schedules_scheduleState_runs {
  __typename: "PipelineRun";
  id: string;
  runId: string;
  status: PipelineRunStatus;
}

export interface JobsRootQuery_repositoryOrError_Repository_schedules_scheduleState_ticks_runs {
  __typename: "PipelineRun";
  id: string;
}

export interface JobsRootQuery_repositoryOrError_Repository_schedules_scheduleState_ticks_error_cause {
  __typename: "PythonError";
  message: string;
  stack: string[];
}

export interface JobsRootQuery_repositoryOrError_Repository_schedules_scheduleState_ticks_error {
  __typename: "PythonError";
  message: string;
  stack: string[];
  cause: JobsRootQuery_repositoryOrError_Repository_schedules_scheduleState_ticks_error_cause | null;
}

export interface JobsRootQuery_repositoryOrError_Repository_schedules_scheduleState_ticks {
  __typename: "JobTick";
  id: string;
  status: JobTickStatus;
  timestamp: number;
  skipReason: string | null;
  runs: JobsRootQuery_repositoryOrError_Repository_schedules_scheduleState_ticks_runs[];
  error: JobsRootQuery_repositoryOrError_Repository_schedules_scheduleState_ticks_error | null;
}

export interface JobsRootQuery_repositoryOrError_Repository_schedules_scheduleState {
  __typename: "JobState";
  id: string;
  name: string;
  jobType: JobType;
  status: JobStatus;
  repositoryOrigin: JobsRootQuery_repositoryOrError_Repository_schedules_scheduleState_repositoryOrigin;
  jobSpecificData: JobsRootQuery_repositoryOrError_Repository_schedules_scheduleState_jobSpecificData | null;
  runs: JobsRootQuery_repositoryOrError_Repository_schedules_scheduleState_runs[];
  ticks: JobsRootQuery_repositoryOrError_Repository_schedules_scheduleState_ticks[];
  runningCount: number;
}

export interface JobsRootQuery_repositoryOrError_Repository_schedules_futureTicks_results {
  __typename: "FutureJobTick";
  timestamp: number;
}

export interface JobsRootQuery_repositoryOrError_Repository_schedules_futureTicks {
  __typename: "FutureJobTicks";
  results: JobsRootQuery_repositoryOrError_Repository_schedules_futureTicks_results[];
}

export interface JobsRootQuery_repositoryOrError_Repository_schedules {
  __typename: "Schedule";
  id: string;
  name: string;
  cronSchedule: string;
  executionTimezone: string | null;
  pipelineName: string;
  solidSelection: (string | null)[] | null;
  mode: string;
  partitionSet: JobsRootQuery_repositoryOrError_Repository_schedules_partitionSet | null;
  scheduleState: JobsRootQuery_repositoryOrError_Repository_schedules_scheduleState;
  futureTicks: JobsRootQuery_repositoryOrError_Repository_schedules_futureTicks;
}

export interface JobsRootQuery_repositoryOrError_Repository_sensors_futureTick {
  __typename: "FutureJobTick";
  timestamp: number;
}

export interface JobsRootQuery_repositoryOrError_Repository_sensors_sensorState_repositoryOrigin_repositoryLocationMetadata {
  __typename: "RepositoryMetadata";
  key: string;
  value: string;
}

export interface JobsRootQuery_repositoryOrError_Repository_sensors_sensorState_repositoryOrigin {
  __typename: "RepositoryOrigin";
  repositoryLocationName: string;
  repositoryName: string;
  repositoryLocationMetadata: JobsRootQuery_repositoryOrError_Repository_sensors_sensorState_repositoryOrigin_repositoryLocationMetadata[];
}

export interface JobsRootQuery_repositoryOrError_Repository_sensors_sensorState_jobSpecificData_SensorJobData {
  __typename: "SensorJobData";
  lastRunKey: string | null;
}

export interface JobsRootQuery_repositoryOrError_Repository_sensors_sensorState_jobSpecificData_ScheduleJobData {
  __typename: "ScheduleJobData";
  cronSchedule: string;
}

export type JobsRootQuery_repositoryOrError_Repository_sensors_sensorState_jobSpecificData = JobsRootQuery_repositoryOrError_Repository_sensors_sensorState_jobSpecificData_SensorJobData | JobsRootQuery_repositoryOrError_Repository_sensors_sensorState_jobSpecificData_ScheduleJobData;

export interface JobsRootQuery_repositoryOrError_Repository_sensors_sensorState_runs {
  __typename: "PipelineRun";
  id: string;
  runId: string;
  status: PipelineRunStatus;
}

export interface JobsRootQuery_repositoryOrError_Repository_sensors_sensorState_ticks_runs {
  __typename: "PipelineRun";
  id: string;
}

export interface JobsRootQuery_repositoryOrError_Repository_sensors_sensorState_ticks_error_cause {
  __typename: "PythonError";
  message: string;
  stack: string[];
}

export interface JobsRootQuery_repositoryOrError_Repository_sensors_sensorState_ticks_error {
  __typename: "PythonError";
  message: string;
  stack: string[];
  cause: JobsRootQuery_repositoryOrError_Repository_sensors_sensorState_ticks_error_cause | null;
}

export interface JobsRootQuery_repositoryOrError_Repository_sensors_sensorState_ticks {
  __typename: "JobTick";
  id: string;
  status: JobTickStatus;
  timestamp: number;
  skipReason: string | null;
  runs: JobsRootQuery_repositoryOrError_Repository_sensors_sensorState_ticks_runs[];
  error: JobsRootQuery_repositoryOrError_Repository_sensors_sensorState_ticks_error | null;
}

export interface JobsRootQuery_repositoryOrError_Repository_sensors_sensorState {
  __typename: "JobState";
  id: string;
  name: string;
  jobType: JobType;
  status: JobStatus;
  repositoryOrigin: JobsRootQuery_repositoryOrError_Repository_sensors_sensorState_repositoryOrigin;
  jobSpecificData: JobsRootQuery_repositoryOrError_Repository_sensors_sensorState_jobSpecificData | null;
  runs: JobsRootQuery_repositoryOrError_Repository_sensors_sensorState_runs[];
  ticks: JobsRootQuery_repositoryOrError_Repository_sensors_sensorState_ticks[];
  runningCount: number;
}

export interface JobsRootQuery_repositoryOrError_Repository_sensors {
  __typename: "Sensor";
  id: string;
  jobOriginId: string;
  name: string;
  pipelineName: string;
  solidSelection: (string | null)[] | null;
  mode: string;
  futureTick: JobsRootQuery_repositoryOrError_Repository_sensors_futureTick | null;
  sensorState: JobsRootQuery_repositoryOrError_Repository_sensors_sensorState;
}

export interface JobsRootQuery_repositoryOrError_Repository {
  __typename: "Repository";
  id: string;
  name: string;
  origin: JobsRootQuery_repositoryOrError_Repository_origin;
  location: JobsRootQuery_repositoryOrError_Repository_location;
  schedules: JobsRootQuery_repositoryOrError_Repository_schedules[];
  sensors: JobsRootQuery_repositoryOrError_Repository_sensors[];
}

export type JobsRootQuery_repositoryOrError = JobsRootQuery_repositoryOrError_RepositoryNotFoundError | JobsRootQuery_repositoryOrError_PythonError | JobsRootQuery_repositoryOrError_Repository;

export interface JobsRootQuery {
  repositoryOrError: JobsRootQuery_repositoryOrError;
}

export interface JobsRootQueryVariables {
  repositorySelector: RepositorySelector;
}
