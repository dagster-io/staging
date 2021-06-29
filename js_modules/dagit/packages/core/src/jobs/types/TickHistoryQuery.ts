// @generated
/* tslint:disable */
/* eslint-disable */
// @generated
// This file was automatically generated and should not be edited.

import { JobSelector, InstigationType, InstigationTickStatus } from "./../../types/globalTypes";

// ====================================================
// GraphQL query operation: TickHistoryQuery
// ====================================================

export interface TickHistoryQuery_jobStateOrError_JobState_nextTick {
  __typename: "FutureInstigationTick";
  timestamp: number;
}

export interface TickHistoryQuery_jobStateOrError_JobState_ticks_error_cause {
  __typename: "PythonError";
  message: string;
  stack: string[];
}

export interface TickHistoryQuery_jobStateOrError_JobState_ticks_error {
  __typename: "PythonError";
  message: string;
  stack: string[];
  cause: TickHistoryQuery_jobStateOrError_JobState_ticks_error_cause | null;
}

export interface TickHistoryQuery_jobStateOrError_JobState_ticks {
  __typename: "InstigationTick";
  id: string;
  status: InstigationTickStatus;
  timestamp: number;
  skipReason: string | null;
  runIds: string[];
  originRunIds: string[];
  error: TickHistoryQuery_jobStateOrError_JobState_ticks_error | null;
}

export interface TickHistoryQuery_jobStateOrError_JobState {
  __typename: "JobState";
  id: string;
  jobType: InstigationType;
  nextTick: TickHistoryQuery_jobStateOrError_JobState_nextTick | null;
  ticks: TickHistoryQuery_jobStateOrError_JobState_ticks[];
}

export interface TickHistoryQuery_jobStateOrError_PythonError_cause {
  __typename: "PythonError";
  message: string;
  stack: string[];
}

export interface TickHistoryQuery_jobStateOrError_PythonError {
  __typename: "PythonError";
  message: string;
  stack: string[];
  cause: TickHistoryQuery_jobStateOrError_PythonError_cause | null;
}

export type TickHistoryQuery_jobStateOrError = TickHistoryQuery_jobStateOrError_JobState | TickHistoryQuery_jobStateOrError_PythonError;

export interface TickHistoryQuery {
  jobStateOrError: TickHistoryQuery_jobStateOrError;
}

export interface TickHistoryQueryVariables {
  jobSelector: JobSelector;
  dayRange?: number | null;
  limit?: number | null;
}
