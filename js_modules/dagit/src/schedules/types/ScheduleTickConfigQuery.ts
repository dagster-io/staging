// @generated
/* tslint:disable */
/* eslint-disable */
// @generated
// This file was automatically generated and should not be edited.

import { ScheduleSelector } from "./../../types/globalTypes";

// ====================================================
// GraphQL query operation: ScheduleTickConfigQuery
// ====================================================

export interface ScheduleTickConfigQuery_scheduleOrError_ScheduleNotFoundError {
  __typename: "ScheduleNotFoundError" | "PythonError";
}

export interface ScheduleTickConfigQuery_scheduleOrError_Schedule_futureTick_runRequests_tags {
  __typename: "PipelineTag";
  key: string;
  value: string;
}

export interface ScheduleTickConfigQuery_scheduleOrError_Schedule_futureTick_runRequests {
  __typename: "RunRequest";
  runKey: string | null;
  runConfigYaml: string;
  tags: ScheduleTickConfigQuery_scheduleOrError_Schedule_futureTick_runRequests_tags[];
}

export interface ScheduleTickConfigQuery_scheduleOrError_Schedule_futureTick {
  __typename: "FutureJobTick";
  runRequests: (ScheduleTickConfigQuery_scheduleOrError_Schedule_futureTick_runRequests | null)[] | null;
}

export interface ScheduleTickConfigQuery_scheduleOrError_Schedule {
  __typename: "Schedule";
  id: string;
  futureTick: ScheduleTickConfigQuery_scheduleOrError_Schedule_futureTick;
}

export type ScheduleTickConfigQuery_scheduleOrError = ScheduleTickConfigQuery_scheduleOrError_ScheduleNotFoundError | ScheduleTickConfigQuery_scheduleOrError_Schedule;

export interface ScheduleTickConfigQuery {
  scheduleOrError: ScheduleTickConfigQuery_scheduleOrError;
}

export interface ScheduleTickConfigQueryVariables {
  scheduleSelector: ScheduleSelector;
  tickTimestamp: number;
}
