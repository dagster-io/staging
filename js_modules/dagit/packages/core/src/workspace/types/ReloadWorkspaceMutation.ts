// @generated
/* tslint:disable */
/* eslint-disable */
// @generated
// This file was automatically generated and should not be edited.

import { RepositoryLocationLoadStatus } from "./../../types/globalTypes";

// ====================================================
// GraphQL mutation operation: ReloadWorkspaceMutation
// ====================================================

export interface ReloadWorkspaceMutation_reloadWorkspace_WorkspaceConnection_locationEntries_location_repositories_pipelines {
  __typename: "Pipeline";
  id: string;
  name: string;
}

export interface ReloadWorkspaceMutation_reloadWorkspace_WorkspaceConnection_locationEntries_location_repositories {
  __typename: "Repository";
  id: string;
  name: string;
  pipelines: ReloadWorkspaceMutation_reloadWorkspace_WorkspaceConnection_locationEntries_location_repositories_pipelines[];
}

export interface ReloadWorkspaceMutation_reloadWorkspace_WorkspaceConnection_locationEntries_location {
  __typename: "RepositoryLocation";
  id: string;
  repositories: ReloadWorkspaceMutation_reloadWorkspace_WorkspaceConnection_locationEntries_location_repositories[];
}

export interface ReloadWorkspaceMutation_reloadWorkspace_WorkspaceConnection_locationEntries_loadError {
  __typename: "PythonError";
  message: string;
}

export interface ReloadWorkspaceMutation_reloadWorkspace_WorkspaceConnection_locationEntries {
  __typename: "WorkspaceLocationEntry";
  name: string;
  id: string;
  loadStatus: RepositoryLocationLoadStatus;
  location: ReloadWorkspaceMutation_reloadWorkspace_WorkspaceConnection_locationEntries_location | null;
  loadError: ReloadWorkspaceMutation_reloadWorkspace_WorkspaceConnection_locationEntries_loadError | null;
}

export interface ReloadWorkspaceMutation_reloadWorkspace_WorkspaceConnection {
  __typename: "WorkspaceConnection";
  locationEntries: ReloadWorkspaceMutation_reloadWorkspace_WorkspaceConnection_locationEntries[];
}

export interface ReloadWorkspaceMutation_reloadWorkspace_ReadOnlyError {
  __typename: "ReadOnlyError";
  message: string;
}

export interface ReloadWorkspaceMutation_reloadWorkspace_PythonError_cause {
  __typename: "PythonError";
  message: string;
  stack: string[];
}

export interface ReloadWorkspaceMutation_reloadWorkspace_PythonError {
  __typename: "PythonError";
  message: string;
  stack: string[];
  cause: ReloadWorkspaceMutation_reloadWorkspace_PythonError_cause | null;
}

export type ReloadWorkspaceMutation_reloadWorkspace = ReloadWorkspaceMutation_reloadWorkspace_WorkspaceConnection | ReloadWorkspaceMutation_reloadWorkspace_ReadOnlyError | ReloadWorkspaceMutation_reloadWorkspace_PythonError;

export interface ReloadWorkspaceMutation {
  reloadWorkspace: ReloadWorkspaceMutation_reloadWorkspace;
}
