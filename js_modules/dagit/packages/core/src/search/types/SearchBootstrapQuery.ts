// @generated
/* tslint:disable */
/* eslint-disable */
// @generated
// This file was automatically generated and should not be edited.

// ====================================================
// GraphQL query operation: SearchBootstrapQuery
// ====================================================

export interface SearchBootstrapQuery_workspaceOrError_PythonError {
  __typename: "PythonError";
}

export interface SearchBootstrapQuery_workspaceOrError_WorkspaceConnection_nodes_location_repositories_pipelines {
  __typename: "Pipeline";
  id: string;
  name: string;
}

export interface SearchBootstrapQuery_workspaceOrError_WorkspaceConnection_nodes_location_repositories_schedules {
  __typename: "Schedule";
  id: string;
  name: string;
}

export interface SearchBootstrapQuery_workspaceOrError_WorkspaceConnection_nodes_location_repositories_sensors {
  __typename: "Sensor";
  id: string;
  name: string;
}

export interface SearchBootstrapQuery_workspaceOrError_WorkspaceConnection_nodes_location_repositories_partitionSets {
  __typename: "PartitionSet";
  id: string;
  name: string;
  pipelineName: string;
}

export interface SearchBootstrapQuery_workspaceOrError_WorkspaceConnection_nodes_location_repositories {
  __typename: "Repository";
  id: string;
  name: string;
  pipelines: SearchBootstrapQuery_workspaceOrError_WorkspaceConnection_nodes_location_repositories_pipelines[];
  schedules: SearchBootstrapQuery_workspaceOrError_WorkspaceConnection_nodes_location_repositories_schedules[];
  sensors: SearchBootstrapQuery_workspaceOrError_WorkspaceConnection_nodes_location_repositories_sensors[];
  partitionSets: SearchBootstrapQuery_workspaceOrError_WorkspaceConnection_nodes_location_repositories_partitionSets[];
}

export interface SearchBootstrapQuery_workspaceOrError_WorkspaceConnection_nodes_location {
  __typename: "RepositoryLocation";
  id: string;
  name: string;
  repositories: SearchBootstrapQuery_workspaceOrError_WorkspaceConnection_nodes_location_repositories[];
}

export interface SearchBootstrapQuery_workspaceOrError_WorkspaceConnection_nodes {
  __typename: "WorkspaceLocationEntry";
  id: string;
  location: SearchBootstrapQuery_workspaceOrError_WorkspaceConnection_nodes_location | null;
}

export interface SearchBootstrapQuery_workspaceOrError_WorkspaceConnection {
  __typename: "WorkspaceConnection";
  nodes: SearchBootstrapQuery_workspaceOrError_WorkspaceConnection_nodes[];
}

export type SearchBootstrapQuery_workspaceOrError = SearchBootstrapQuery_workspaceOrError_PythonError | SearchBootstrapQuery_workspaceOrError_WorkspaceConnection;

export interface SearchBootstrapQuery {
  workspaceOrError: SearchBootstrapQuery_workspaceOrError;
}
