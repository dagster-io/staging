// @generated
/* tslint:disable */
/* eslint-disable */
// @generated
// This file was automatically generated and should not be edited.

import { RepositoryLocationLoadStatus } from "./../../types/globalTypes";

// ====================================================
// GraphQL fragment: RepositoryLocationsFragment
// ====================================================

export interface RepositoryLocationsFragment_WorkspaceConnection_nodes_displayMetadata {
  __typename: "RepositoryMetadata";
  key: string;
  value: string;
}

export interface RepositoryLocationsFragment_WorkspaceConnection_nodes_location {
  __typename: "RepositoryLocation";
  id: string;
  isReloadSupported: boolean;
  serverId: string | null;
  name: string;
}

export interface RepositoryLocationsFragment_WorkspaceConnection_nodes_loadError {
  __typename: "PythonError";
  message: string;
}

export interface RepositoryLocationsFragment_WorkspaceConnection_nodes {
  __typename: "WorkspaceLocationEntry";
  id: string;
  name: string;
  loadStatus: RepositoryLocationLoadStatus;
  displayMetadata: RepositoryLocationsFragment_WorkspaceConnection_nodes_displayMetadata[];
  updatedTimestamp: number;
  location: RepositoryLocationsFragment_WorkspaceConnection_nodes_location | null;
  loadError: RepositoryLocationsFragment_WorkspaceConnection_nodes_loadError | null;
}

export interface RepositoryLocationsFragment_WorkspaceConnection {
  __typename: "WorkspaceConnection";
  nodes: RepositoryLocationsFragment_WorkspaceConnection_nodes[];
}

export interface RepositoryLocationsFragment_PythonError_cause {
  __typename: "PythonError";
  message: string;
  stack: string[];
}

export interface RepositoryLocationsFragment_PythonError {
  __typename: "PythonError";
  message: string;
  stack: string[];
  cause: RepositoryLocationsFragment_PythonError_cause | null;
}

export type RepositoryLocationsFragment = RepositoryLocationsFragment_WorkspaceConnection | RepositoryLocationsFragment_PythonError;
