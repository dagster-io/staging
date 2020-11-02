// @generated
/* tslint:disable */
/* eslint-disable */
// @generated
// This file was automatically generated and should not be edited.

import { HandleStateChangeEventType } from "./../../types/globalTypes";

// ====================================================
// GraphQL subscription operation: HandleStateChangeSubscription
// ====================================================

export interface HandleStateChangeSubscription_handleStateChangeEvents_event {
  __typename: "HandleStateChangeEvent";
  eventType: HandleStateChangeEventType;
  message: string;
  locationName: string;
}

export interface HandleStateChangeSubscription_handleStateChangeEvents {
  __typename: "HandleStateChangeSubscription";
  event: HandleStateChangeSubscription_handleStateChangeEvents_event;
}

export interface HandleStateChangeSubscription {
  handleStateChangeEvents: HandleStateChangeSubscription_handleStateChangeEvents;
}
