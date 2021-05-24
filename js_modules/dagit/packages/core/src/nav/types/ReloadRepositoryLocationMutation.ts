// @generated
/* tslint:disable */
/* eslint-disable */
// @generated
// This file was automatically generated and should not be edited.

import { RepositoryLocationLoadStatus } from "./../../types/globalTypes";

// ====================================================
// GraphQL mutation operation: ReloadRepositoryLocationMutation
// ====================================================

export interface ReloadRepositoryLocationMutation_reloadRepositoryLocation_WorkspaceLocationEntry_location_repositories_pipelines {
  __typename: "Pipeline";
  id: string;
  name: string;
}

export interface ReloadRepositoryLocationMutation_reloadRepositoryLocation_WorkspaceLocationEntry_location_repositories {
  __typename: "Repository";
  id: string;
  name: string;
  pipelines: ReloadRepositoryLocationMutation_reloadRepositoryLocation_WorkspaceLocationEntry_location_repositories_pipelines[];
}

export interface ReloadRepositoryLocationMutation_reloadRepositoryLocation_WorkspaceLocationEntry_location {
  __typename: "RepositoryLocation";
  id: string;
  repositories: ReloadRepositoryLocationMutation_reloadRepositoryLocation_WorkspaceLocationEntry_location_repositories[];
}

export interface ReloadRepositoryLocationMutation_reloadRepositoryLocation_WorkspaceLocationEntry_loadError {
  __typename: "PythonError";
  message: string;
}

export interface ReloadRepositoryLocationMutation_reloadRepositoryLocation_WorkspaceLocationEntry {
  __typename: "WorkspaceLocationEntry";
  id: string;
  name: string;
  loadStatus: RepositoryLocationLoadStatus;
  location: ReloadRepositoryLocationMutation_reloadRepositoryLocation_WorkspaceLocationEntry_location | null;
  loadError: ReloadRepositoryLocationMutation_reloadRepositoryLocation_WorkspaceLocationEntry_loadError | null;
}

export interface ReloadRepositoryLocationMutation_reloadRepositoryLocation_ReadOnlyError {
  __typename: "ReadOnlyError";
  message: string;
}

export interface ReloadRepositoryLocationMutation_reloadRepositoryLocation_ReloadNotSupported {
  __typename: "ReloadNotSupported";
  message: string;
}

export interface ReloadRepositoryLocationMutation_reloadRepositoryLocation_RepositoryLocationNotFound {
  __typename: "RepositoryLocationNotFound";
  message: string;
}

export type ReloadRepositoryLocationMutation_reloadRepositoryLocation = ReloadRepositoryLocationMutation_reloadRepositoryLocation_WorkspaceLocationEntry | ReloadRepositoryLocationMutation_reloadRepositoryLocation_ReadOnlyError | ReloadRepositoryLocationMutation_reloadRepositoryLocation_ReloadNotSupported | ReloadRepositoryLocationMutation_reloadRepositoryLocation_RepositoryLocationNotFound;

export interface ReloadRepositoryLocationMutation {
  reloadRepositoryLocation: ReloadRepositoryLocationMutation_reloadRepositoryLocation;
}

export interface ReloadRepositoryLocationMutationVariables {
  location: string;
}
