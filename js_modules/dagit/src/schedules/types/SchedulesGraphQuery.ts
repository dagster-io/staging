// @generated
/* tslint:disable */
/* eslint-disable */
// @generated
// This file was automatically generated and should not be edited.

import { RepositorySelector, JobStatus, JobTickStatus } from "./../../types/globalTypes";

// ====================================================
// GraphQL query operation: SchedulesGraphQuery
// ====================================================

export interface SchedulesGraphQuery_repositoryOrError_PythonError {
  __typename: "PythonError" | "RepositoryNotFoundError";
}

export interface SchedulesGraphQuery_repositoryOrError_Repository_schedules_scheduleState_ticks_error_cause {
  __typename: "PythonError";
  message: string;
  stack: string[];
}

export interface SchedulesGraphQuery_repositoryOrError_Repository_schedules_scheduleState_ticks_error {
  __typename: "PythonError";
  message: string;
  stack: string[];
  cause: SchedulesGraphQuery_repositoryOrError_Repository_schedules_scheduleState_ticks_error_cause | null;
}

export interface SchedulesGraphQuery_repositoryOrError_Repository_schedules_scheduleState_ticks {
  __typename: "JobTick";
  id: string;
  status: JobTickStatus;
  timestamp: number;
  skipReason: string | null;
  runIds: string[];
  error: SchedulesGraphQuery_repositoryOrError_Repository_schedules_scheduleState_ticks_error | null;
}

export interface SchedulesGraphQuery_repositoryOrError_Repository_schedules_scheduleState {
  __typename: "JobState";
  id: string;
  status: JobStatus;
  ticks: SchedulesGraphQuery_repositoryOrError_Repository_schedules_scheduleState_ticks[];
}

export interface SchedulesGraphQuery_repositoryOrError_Repository_schedules_futureTicks_results {
  __typename: "FutureJobTick";
  timestamp: number;
}

export interface SchedulesGraphQuery_repositoryOrError_Repository_schedules_futureTicks {
  __typename: "FutureJobTicks";
  results: SchedulesGraphQuery_repositoryOrError_Repository_schedules_futureTicks_results[];
}

export interface SchedulesGraphQuery_repositoryOrError_Repository_schedules {
  __typename: "Schedule";
  id: string;
  name: string;
  scheduleState: SchedulesGraphQuery_repositoryOrError_Repository_schedules_scheduleState;
  futureTicks: SchedulesGraphQuery_repositoryOrError_Repository_schedules_futureTicks;
}

export interface SchedulesGraphQuery_repositoryOrError_Repository {
  __typename: "Repository";
  id: string;
  schedules: SchedulesGraphQuery_repositoryOrError_Repository_schedules[];
}

export type SchedulesGraphQuery_repositoryOrError = SchedulesGraphQuery_repositoryOrError_PythonError | SchedulesGraphQuery_repositoryOrError_Repository;

export interface SchedulesGraphQuery {
  repositoryOrError: SchedulesGraphQuery_repositoryOrError;
}

export interface SchedulesGraphQueryVariables {
  repositorySelector: RepositorySelector;
  tickLimit: number;
}
