// @generated
/* tslint:disable */
/* eslint-disable */
// @generated
// This file was automatically generated and should not be edited.

import { RepositoryLocationLoadStatus } from "./../../types/globalTypes";

// ====================================================
// GraphQL query operation: RootRepositoriesQuery
// ====================================================

export interface RootRepositoriesQuery_workspaceOrError_WorkspaceConnection_locationEntries_displayMetadata {
  __typename: "RepositoryMetadata";
  key: string;
  value: string;
}

export interface RootRepositoriesQuery_workspaceOrError_WorkspaceConnection_locationEntries_location_repositories_pipelines {
  __typename: "Pipeline";
  id: string;
  name: string;
  pipelineSnapshotId: string;
}

export interface RootRepositoriesQuery_workspaceOrError_WorkspaceConnection_locationEntries_location_repositories_partitionSets {
  __typename: "PartitionSet";
  id: string;
  pipelineName: string;
}

export interface RootRepositoriesQuery_workspaceOrError_WorkspaceConnection_locationEntries_location_repositories_location {
  __typename: "RepositoryLocation";
  id: string;
  name: string;
}

export interface RootRepositoriesQuery_workspaceOrError_WorkspaceConnection_locationEntries_location_repositories_displayMetadata {
  __typename: "RepositoryMetadata";
  key: string;
  value: string;
}

export interface RootRepositoriesQuery_workspaceOrError_WorkspaceConnection_locationEntries_location_repositories {
  __typename: "Repository";
  id: string;
  name: string;
  pipelines: RootRepositoriesQuery_workspaceOrError_WorkspaceConnection_locationEntries_location_repositories_pipelines[];
  partitionSets: RootRepositoriesQuery_workspaceOrError_WorkspaceConnection_locationEntries_location_repositories_partitionSets[];
  location: RootRepositoriesQuery_workspaceOrError_WorkspaceConnection_locationEntries_location_repositories_location;
  displayMetadata: RootRepositoriesQuery_workspaceOrError_WorkspaceConnection_locationEntries_location_repositories_displayMetadata[];
}

export interface RootRepositoriesQuery_workspaceOrError_WorkspaceConnection_locationEntries_location {
  __typename: "RepositoryLocation";
  id: string;
  isReloadSupported: boolean;
  serverId: string | null;
  name: string;
  repositories: RootRepositoriesQuery_workspaceOrError_WorkspaceConnection_locationEntries_location_repositories[];
}

export interface RootRepositoriesQuery_workspaceOrError_WorkspaceConnection_locationEntries_loadError_cause {
  __typename: "PythonError";
  message: string;
  stack: string[];
}

export interface RootRepositoriesQuery_workspaceOrError_WorkspaceConnection_locationEntries_loadError {
  __typename: "PythonError";
  message: string;
  stack: string[];
  cause: RootRepositoriesQuery_workspaceOrError_WorkspaceConnection_locationEntries_loadError_cause | null;
}

export interface RootRepositoriesQuery_workspaceOrError_WorkspaceConnection_locationEntries {
  __typename: "WorkspaceLocationEntry";
  id: string;
  name: string;
  loadStatus: RepositoryLocationLoadStatus;
  displayMetadata: RootRepositoriesQuery_workspaceOrError_WorkspaceConnection_locationEntries_displayMetadata[];
  updatedTimestamp: number;
  location: RootRepositoriesQuery_workspaceOrError_WorkspaceConnection_locationEntries_location | null;
  loadError: RootRepositoriesQuery_workspaceOrError_WorkspaceConnection_locationEntries_loadError | null;
}

export interface RootRepositoriesQuery_workspaceOrError_WorkspaceConnection {
  __typename: "WorkspaceConnection";
  locationEntries: RootRepositoriesQuery_workspaceOrError_WorkspaceConnection_locationEntries[];
}

export interface RootRepositoriesQuery_workspaceOrError_PythonError_cause {
  __typename: "PythonError";
  message: string;
  stack: string[];
}

export interface RootRepositoriesQuery_workspaceOrError_PythonError {
  __typename: "PythonError";
  message: string;
  stack: string[];
  cause: RootRepositoriesQuery_workspaceOrError_PythonError_cause | null;
}

export type RootRepositoriesQuery_workspaceOrError = RootRepositoriesQuery_workspaceOrError_WorkspaceConnection | RootRepositoriesQuery_workspaceOrError_PythonError;

export interface RootRepositoriesQuery {
  workspaceOrError: RootRepositoriesQuery_workspaceOrError;
}
