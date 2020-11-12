// @generated
/* tslint:disable */
/* eslint-disable */
// @generated
// This file was automatically generated and should not be edited.

import { GrpcServerStateChangeEventType } from "./../../types/globalTypes";

// ====================================================
// GraphQL subscription operation: GrpcServerStateChangeSubscription
// ====================================================

export interface GrpcServerStateChangeSubscription_grpcServerStateChangeEvents_event {
  __typename: "GrpcServerStateChangeEvent";
  message: string;
  locationName: string;
  eventType: GrpcServerStateChangeEventType;
  serverId: string | null;
}

export interface GrpcServerStateChangeSubscription_grpcServerStateChangeEvents {
  __typename: "GrpcServerStateChangeSubscription";
  event: GrpcServerStateChangeSubscription_grpcServerStateChangeEvents_event;
}

export interface GrpcServerStateChangeSubscription {
  grpcServerStateChangeEvents: GrpcServerStateChangeSubscription_grpcServerStateChangeEvents;
}
