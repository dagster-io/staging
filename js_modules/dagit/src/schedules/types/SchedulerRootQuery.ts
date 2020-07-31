// @generated
/* tslint:disable */
/* eslint-disable */
// This file was automatically generated and should not be edited.

import { ScheduleStatus } from "./../../types/globalTypes";

// ====================================================
// GraphQL query operation: SchedulerRootQuery
// ====================================================

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

export interface SchedulerRootQuery_scheduleStatesOrError_RepositoryNotFoundError {
  __typename: "RepositoryNotFoundError";
}

export interface SchedulerRootQuery_scheduleStatesOrError_ScheduleStates_results_repositoryOrigin {
  __typename: "RepositoryOrigin";
  executablePath: string;
  codePointerDescription: string;
}

export interface SchedulerRootQuery_scheduleStatesOrError_ScheduleStates_results {
  __typename: "ScheduleState";
  id: string;
  scheduleName: string;
  cronSchedule: string;
  status: ScheduleStatus;
  repositoryOriginId: string;
  repositoryOrigin: SchedulerRootQuery_scheduleStatesOrError_ScheduleStates_results_repositoryOrigin;
}

export interface SchedulerRootQuery_scheduleStatesOrError_ScheduleStates {
  __typename: "ScheduleStates";
  results: SchedulerRootQuery_scheduleStatesOrError_ScheduleStates_results[];
}

export interface SchedulerRootQuery_scheduleStatesOrError_PythonError_cause {
  __typename: "PythonError";
  message: string;
  stack: string[];
}

export interface SchedulerRootQuery_scheduleStatesOrError_PythonError {
  __typename: "PythonError";
  message: string;
  stack: string[];
  cause: SchedulerRootQuery_scheduleStatesOrError_PythonError_cause | null;
}

export type SchedulerRootQuery_scheduleStatesOrError = SchedulerRootQuery_scheduleStatesOrError_RepositoryNotFoundError | SchedulerRootQuery_scheduleStatesOrError_ScheduleStates | SchedulerRootQuery_scheduleStatesOrError_PythonError;

export interface SchedulerRootQuery {
  scheduler: SchedulerRootQuery_scheduler;
  scheduleStatesOrError: SchedulerRootQuery_scheduleStatesOrError;
}
