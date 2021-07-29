// @generated
/* tslint:disable */
/* eslint-disable */
// @generated
// This file was automatically generated and should not be edited.

import { InstigationStatus, InstigationTickStatus } from "./../../types/globalTypes";

// ====================================================
// GraphQL fragment: LooseDependencyScheduleFragment
// ====================================================

export interface LooseDependencyScheduleFragment_scheduleState_ticks_error_cause {
  __typename: "PythonError";
  message: string;
  stack: string[];
}

export interface LooseDependencyScheduleFragment_scheduleState_ticks_error {
  __typename: "PythonError";
  message: string;
  stack: string[];
  cause: LooseDependencyScheduleFragment_scheduleState_ticks_error_cause | null;
}

export interface LooseDependencyScheduleFragment_scheduleState_ticks {
  __typename: "InstigationTick";
  id: string;
  status: InstigationTickStatus;
  timestamp: number;
  skipReason: string | null;
  runIds: string[];
  error: LooseDependencyScheduleFragment_scheduleState_ticks_error | null;
}

export interface LooseDependencyScheduleFragment_scheduleState {
  __typename: "InstigationState";
  id: string;
  status: InstigationStatus;
  ticks: LooseDependencyScheduleFragment_scheduleState_ticks[];
}

export interface LooseDependencyScheduleFragment {
  __typename: "Schedule";
  id: string;
  name: string;
  mode: string;
  cronSchedule: string;
  scheduleState: LooseDependencyScheduleFragment_scheduleState;
}
