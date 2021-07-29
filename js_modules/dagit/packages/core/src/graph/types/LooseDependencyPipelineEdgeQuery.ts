// @generated
/* tslint:disable */
/* eslint-disable */
// @generated
// This file was automatically generated and should not be edited.

// ====================================================
// GraphQL query operation: LooseDependencyPipelineEdgeQuery
// ====================================================

export interface LooseDependencyPipelineEdgeQuery_repositoriesOrError_PythonError {
  __typename: "PythonError";
}

export interface LooseDependencyPipelineEdgeQuery_repositoriesOrError_RepositoryConnection_nodes_location {
  __typename: "RepositoryLocation";
  id: string;
  name: string;
}

export interface LooseDependencyPipelineEdgeQuery_repositoriesOrError_RepositoryConnection_nodes_pipelines_runs_assets_key {
  __typename: "AssetKey";
  path: string[];
}

export interface LooseDependencyPipelineEdgeQuery_repositoriesOrError_RepositoryConnection_nodes_pipelines_runs_assets {
  __typename: "Asset";
  id: string;
  key: LooseDependencyPipelineEdgeQuery_repositoriesOrError_RepositoryConnection_nodes_pipelines_runs_assets_key;
}

export interface LooseDependencyPipelineEdgeQuery_repositoriesOrError_RepositoryConnection_nodes_pipelines_runs {
  __typename: "PipelineRun";
  id: string;
  assets: LooseDependencyPipelineEdgeQuery_repositoriesOrError_RepositoryConnection_nodes_pipelines_runs_assets[];
}

export interface LooseDependencyPipelineEdgeQuery_repositoriesOrError_RepositoryConnection_nodes_pipelines_schedules {
  __typename: "Schedule";
  id: string;
  name: string;
}

export interface LooseDependencyPipelineEdgeQuery_repositoriesOrError_RepositoryConnection_nodes_pipelines_sensors_assets_key {
  __typename: "AssetKey";
  path: string[];
}

export interface LooseDependencyPipelineEdgeQuery_repositoriesOrError_RepositoryConnection_nodes_pipelines_sensors_assets {
  __typename: "Asset";
  id: string;
  key: LooseDependencyPipelineEdgeQuery_repositoriesOrError_RepositoryConnection_nodes_pipelines_sensors_assets_key;
}

export interface LooseDependencyPipelineEdgeQuery_repositoriesOrError_RepositoryConnection_nodes_pipelines_sensors {
  __typename: "Sensor";
  id: string;
  name: string;
  isAssetSensor: boolean | null;
  assets: LooseDependencyPipelineEdgeQuery_repositoriesOrError_RepositoryConnection_nodes_pipelines_sensors_assets[] | null;
}

export interface LooseDependencyPipelineEdgeQuery_repositoriesOrError_RepositoryConnection_nodes_pipelines_solids_outputs_definition_asset_key {
  __typename: "AssetKey";
  path: string[];
}

export interface LooseDependencyPipelineEdgeQuery_repositoriesOrError_RepositoryConnection_nodes_pipelines_solids_outputs_definition_asset {
  __typename: "Asset";
  id: string;
  key: LooseDependencyPipelineEdgeQuery_repositoriesOrError_RepositoryConnection_nodes_pipelines_solids_outputs_definition_asset_key;
}

export interface LooseDependencyPipelineEdgeQuery_repositoriesOrError_RepositoryConnection_nodes_pipelines_solids_outputs_definition {
  __typename: "OutputDefinition";
  name: string;
  asset: LooseDependencyPipelineEdgeQuery_repositoriesOrError_RepositoryConnection_nodes_pipelines_solids_outputs_definition_asset | null;
}

export interface LooseDependencyPipelineEdgeQuery_repositoriesOrError_RepositoryConnection_nodes_pipelines_solids_outputs {
  __typename: "Output";
  definition: LooseDependencyPipelineEdgeQuery_repositoriesOrError_RepositoryConnection_nodes_pipelines_solids_outputs_definition;
}

export interface LooseDependencyPipelineEdgeQuery_repositoriesOrError_RepositoryConnection_nodes_pipelines_solids {
  __typename: "Solid";
  name: string;
  outputs: LooseDependencyPipelineEdgeQuery_repositoriesOrError_RepositoryConnection_nodes_pipelines_solids_outputs[];
}

export interface LooseDependencyPipelineEdgeQuery_repositoriesOrError_RepositoryConnection_nodes_pipelines {
  __typename: "Pipeline";
  id: string;
  name: string;
  runs: LooseDependencyPipelineEdgeQuery_repositoriesOrError_RepositoryConnection_nodes_pipelines_runs[];
  schedules: LooseDependencyPipelineEdgeQuery_repositoriesOrError_RepositoryConnection_nodes_pipelines_schedules[];
  sensors: LooseDependencyPipelineEdgeQuery_repositoriesOrError_RepositoryConnection_nodes_pipelines_sensors[];
  solids: LooseDependencyPipelineEdgeQuery_repositoriesOrError_RepositoryConnection_nodes_pipelines_solids[];
}

export interface LooseDependencyPipelineEdgeQuery_repositoriesOrError_RepositoryConnection_nodes {
  __typename: "Repository";
  id: string;
  name: string;
  location: LooseDependencyPipelineEdgeQuery_repositoriesOrError_RepositoryConnection_nodes_location;
  pipelines: LooseDependencyPipelineEdgeQuery_repositoriesOrError_RepositoryConnection_nodes_pipelines[];
}

export interface LooseDependencyPipelineEdgeQuery_repositoriesOrError_RepositoryConnection {
  __typename: "RepositoryConnection";
  nodes: LooseDependencyPipelineEdgeQuery_repositoriesOrError_RepositoryConnection_nodes[];
}

export type LooseDependencyPipelineEdgeQuery_repositoriesOrError = LooseDependencyPipelineEdgeQuery_repositoriesOrError_PythonError | LooseDependencyPipelineEdgeQuery_repositoriesOrError_RepositoryConnection;

export interface LooseDependencyPipelineEdgeQuery {
  repositoriesOrError: LooseDependencyPipelineEdgeQuery_repositoriesOrError;
}

export interface LooseDependencyPipelineEdgeQueryVariables {
  includeHistorical: boolean;
}
