// @generated
/* tslint:disable */
/* eslint-disable */
// @generated
// This file was automatically generated and should not be edited.

import { RepositoryLocationLoadStatus } from "./../../types/globalTypes";

// ====================================================
// GraphQL query operation: InstanceHealthQuery
// ====================================================

export interface InstanceHealthQuery_instance_daemonHealth_allDaemonStatuses_lastHeartbeatErrors_cause {
  __typename: "PythonError";
  message: string;
  stack: string[];
}

export interface InstanceHealthQuery_instance_daemonHealth_allDaemonStatuses_lastHeartbeatErrors {
  __typename: "PythonError";
  message: string;
  stack: string[];
  cause: InstanceHealthQuery_instance_daemonHealth_allDaemonStatuses_lastHeartbeatErrors_cause | null;
}

export interface InstanceHealthQuery_instance_daemonHealth_allDaemonStatuses {
  __typename: "DaemonStatus";
  id: string;
  daemonType: string;
  required: boolean;
  healthy: boolean | null;
  lastHeartbeatErrors: InstanceHealthQuery_instance_daemonHealth_allDaemonStatuses_lastHeartbeatErrors[];
  lastHeartbeatTime: number | null;
}

export interface InstanceHealthQuery_instance_daemonHealth {
  __typename: "DaemonHealth";
  id: string;
  allDaemonStatuses: InstanceHealthQuery_instance_daemonHealth_allDaemonStatuses[];
}

export interface InstanceHealthQuery_instance {
  __typename: "Instance";
  daemonHealth: InstanceHealthQuery_instance_daemonHealth;
}

export interface InstanceHealthQuery_workspaceOrError_WorkspaceConnection_nodes_displayMetadata {
  __typename: "RepositoryMetadata";
  key: string;
  value: string;
}

export interface InstanceHealthQuery_workspaceOrError_WorkspaceConnection_nodes_location {
  __typename: "RepositoryLocation";
  id: string;
  isReloadSupported: boolean;
  serverId: string | null;
  name: string;
}

export interface InstanceHealthQuery_workspaceOrError_WorkspaceConnection_nodes_loadError {
  __typename: "PythonError";
  message: string;
}

export interface InstanceHealthQuery_workspaceOrError_WorkspaceConnection_nodes {
  __typename: "WorkspaceLocationEntry";
  id: string;
  name: string;
  loadStatus: RepositoryLocationLoadStatus;
  displayMetadata: InstanceHealthQuery_workspaceOrError_WorkspaceConnection_nodes_displayMetadata[];
  updatedTimestamp: number;
  location: InstanceHealthQuery_workspaceOrError_WorkspaceConnection_nodes_location | null;
  loadError: InstanceHealthQuery_workspaceOrError_WorkspaceConnection_nodes_loadError | null;
}

export interface InstanceHealthQuery_workspaceOrError_WorkspaceConnection {
  __typename: "WorkspaceConnection";
  nodes: InstanceHealthQuery_workspaceOrError_WorkspaceConnection_nodes[];
}

export interface InstanceHealthQuery_workspaceOrError_PythonError_cause {
  __typename: "PythonError";
  message: string;
  stack: string[];
}

export interface InstanceHealthQuery_workspaceOrError_PythonError {
  __typename: "PythonError";
  message: string;
  stack: string[];
  cause: InstanceHealthQuery_workspaceOrError_PythonError_cause | null;
}

export type InstanceHealthQuery_workspaceOrError = InstanceHealthQuery_workspaceOrError_WorkspaceConnection | InstanceHealthQuery_workspaceOrError_PythonError;

export interface InstanceHealthQuery {
  instance: InstanceHealthQuery_instance;
  workspaceOrError: InstanceHealthQuery_workspaceOrError;
}
