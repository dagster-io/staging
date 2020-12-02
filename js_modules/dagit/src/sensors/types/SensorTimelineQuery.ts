// @generated
/* tslint:disable */
/* eslint-disable */
// @generated
// This file was automatically generated and should not be edited.

import { SensorSelector, JobTickStatus } from "./../../types/globalTypes";

// ====================================================
// GraphQL query operation: SensorTimelineQuery
// ====================================================

export interface SensorTimelineQuery_sensorOrError_SensorNotFoundError {
  __typename: "SensorNotFoundError" | "PythonError";
}

export interface SensorTimelineQuery_sensorOrError_Sensor_futureTick {
  __typename: "FutureJobTick";
  timestamp: number;
}

export interface SensorTimelineQuery_sensorOrError_Sensor_sensorState_ticks_error_cause {
  __typename: "PythonError";
  message: string;
  stack: string[];
}

export interface SensorTimelineQuery_sensorOrError_Sensor_sensorState_ticks_error {
  __typename: "PythonError";
  message: string;
  stack: string[];
  cause: SensorTimelineQuery_sensorOrError_Sensor_sensorState_ticks_error_cause | null;
}

export interface SensorTimelineQuery_sensorOrError_Sensor_sensorState_ticks {
  __typename: "JobTick";
  id: string;
  status: JobTickStatus;
  timestamp: number;
  skipReason: string | null;
  runIds: string[];
  error: SensorTimelineQuery_sensorOrError_Sensor_sensorState_ticks_error | null;
}

export interface SensorTimelineQuery_sensorOrError_Sensor_sensorState {
  __typename: "JobState";
  id: string;
  ticks: SensorTimelineQuery_sensorOrError_Sensor_sensorState_ticks[];
}

export interface SensorTimelineQuery_sensorOrError_Sensor {
  __typename: "Sensor";
  id: string;
  futureTick: SensorTimelineQuery_sensorOrError_Sensor_futureTick | null;
  sensorState: SensorTimelineQuery_sensorOrError_Sensor_sensorState;
}

export type SensorTimelineQuery_sensorOrError = SensorTimelineQuery_sensorOrError_SensorNotFoundError | SensorTimelineQuery_sensorOrError_Sensor;

export interface SensorTimelineQuery_instance_daemonHealth_daemonStatus {
  __typename: "DaemonStatus";
  healthy: boolean | null;
  lastHeartbeatTime: number | null;
}

export interface SensorTimelineQuery_instance_daemonHealth {
  __typename: "DaemonHealth";
  daemonStatus: SensorTimelineQuery_instance_daemonHealth_daemonStatus;
}

export interface SensorTimelineQuery_instance {
  __typename: "Instance";
  daemonHealth: SensorTimelineQuery_instance_daemonHealth;
}

export interface SensorTimelineQuery {
  sensorOrError: SensorTimelineQuery_sensorOrError;
  instance: SensorTimelineQuery_instance;
}

export interface SensorTimelineQueryVariables {
  sensorSelector: SensorSelector;
}
