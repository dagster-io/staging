// @generated
/* tslint:disable */
/* eslint-disable */
// @generated
// This file was automatically generated and should not be edited.

import { InstigationStatus } from "./../../types/globalTypes";

// ====================================================
// GraphQL fragment: NavSensorFragment
// ====================================================

export interface NavSensorFragment_sensorState {
  __typename: "JobState";
  id: string;
  status: InstigationStatus;
}

export interface NavSensorFragment {
  __typename: "Sensor";
  id: string;
  mode: string | null;
  name: string;
  sensorState: NavSensorFragment_sensorState;
}
