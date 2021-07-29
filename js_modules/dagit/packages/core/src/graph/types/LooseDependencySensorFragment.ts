// @generated
/* tslint:disable */
/* eslint-disable */
// @generated
// This file was automatically generated and should not be edited.

import { InstigationStatus, InstigationTickStatus } from "./../../types/globalTypes";

// ====================================================
// GraphQL fragment: LooseDependencySensorFragment
// ====================================================

export interface LooseDependencySensorFragment_sensorState_ticks_error_cause {
  __typename: "PythonError";
  message: string;
  stack: string[];
}

export interface LooseDependencySensorFragment_sensorState_ticks_error {
  __typename: "PythonError";
  message: string;
  stack: string[];
  cause: LooseDependencySensorFragment_sensorState_ticks_error_cause | null;
}

export interface LooseDependencySensorFragment_sensorState_ticks {
  __typename: "InstigationTick";
  id: string;
  status: InstigationTickStatus;
  timestamp: number;
  skipReason: string | null;
  runIds: string[];
  error: LooseDependencySensorFragment_sensorState_ticks_error | null;
}

export interface LooseDependencySensorFragment_sensorState {
  __typename: "InstigationState";
  id: string;
  status: InstigationStatus;
  ticks: LooseDependencySensorFragment_sensorState_ticks[];
}

export interface LooseDependencySensorFragment {
  __typename: "Sensor";
  id: string;
  name: string;
  minIntervalSeconds: number;
  sensorState: LooseDependencySensorFragment_sensorState;
}
