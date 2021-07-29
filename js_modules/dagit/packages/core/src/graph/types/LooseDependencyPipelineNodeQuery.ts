// @generated
/* tslint:disable */
/* eslint-disable */
// @generated
// This file was automatically generated and should not be edited.

import { NodeParam, PipelineRunStatus, InstigationStatus, InstigationTickStatus } from "./../../types/globalTypes";

// ====================================================
// GraphQL query operation: LooseDependencyPipelineNodeQuery
// ====================================================

export interface LooseDependencyPipelineNodeQuery_nodesOrError_PythonError_cause {
  __typename: "PythonError";
  message: string;
  stack: string[];
}

export interface LooseDependencyPipelineNodeQuery_nodesOrError_PythonError {
  __typename: "PythonError";
  message: string;
  stack: string[];
  cause: LooseDependencyPipelineNodeQuery_nodesOrError_PythonError_cause | null;
}

export interface LooseDependencyPipelineNodeQuery_nodesOrError_Nodes_nodes_Pipeline_runs {
  __typename: "PipelineRun";
  id: string;
  runId: string;
  status: PipelineRunStatus;
}

export interface LooseDependencyPipelineNodeQuery_nodesOrError_Nodes_nodes_Pipeline {
  __typename: "Pipeline";
  id: string;
  name: string;
  runs: LooseDependencyPipelineNodeQuery_nodesOrError_Nodes_nodes_Pipeline_runs[];
}

export interface LooseDependencyPipelineNodeQuery_nodesOrError_Nodes_nodes_Schedule_scheduleState_ticks_error_cause {
  __typename: "PythonError";
  message: string;
  stack: string[];
}

export interface LooseDependencyPipelineNodeQuery_nodesOrError_Nodes_nodes_Schedule_scheduleState_ticks_error {
  __typename: "PythonError";
  message: string;
  stack: string[];
  cause: LooseDependencyPipelineNodeQuery_nodesOrError_Nodes_nodes_Schedule_scheduleState_ticks_error_cause | null;
}

export interface LooseDependencyPipelineNodeQuery_nodesOrError_Nodes_nodes_Schedule_scheduleState_ticks {
  __typename: "InstigationTick";
  id: string;
  status: InstigationTickStatus;
  timestamp: number;
  skipReason: string | null;
  runIds: string[];
  error: LooseDependencyPipelineNodeQuery_nodesOrError_Nodes_nodes_Schedule_scheduleState_ticks_error | null;
}

export interface LooseDependencyPipelineNodeQuery_nodesOrError_Nodes_nodes_Schedule_scheduleState {
  __typename: "InstigationState";
  id: string;
  status: InstigationStatus;
  ticks: LooseDependencyPipelineNodeQuery_nodesOrError_Nodes_nodes_Schedule_scheduleState_ticks[];
}

export interface LooseDependencyPipelineNodeQuery_nodesOrError_Nodes_nodes_Schedule {
  __typename: "Schedule";
  id: string;
  name: string;
  mode: string;
  cronSchedule: string;
  scheduleState: LooseDependencyPipelineNodeQuery_nodesOrError_Nodes_nodes_Schedule_scheduleState;
}

export interface LooseDependencyPipelineNodeQuery_nodesOrError_Nodes_nodes_Asset_key {
  __typename: "AssetKey";
  path: string[];
}

export interface LooseDependencyPipelineNodeQuery_nodesOrError_Nodes_nodes_Asset_mostRecentMaterialization_materializationEvent {
  __typename: "StepMaterializationEvent";
  timestamp: string;
}

export interface LooseDependencyPipelineNodeQuery_nodesOrError_Nodes_nodes_Asset_mostRecentMaterialization {
  __typename: "AssetMaterialization";
  materializationEvent: LooseDependencyPipelineNodeQuery_nodesOrError_Nodes_nodes_Asset_mostRecentMaterialization_materializationEvent;
}

export interface LooseDependencyPipelineNodeQuery_nodesOrError_Nodes_nodes_Asset_assetMaterializations_runOrError_PipelineRunNotFoundError {
  __typename: "PipelineRunNotFoundError" | "PythonError";
}

export interface LooseDependencyPipelineNodeQuery_nodesOrError_Nodes_nodes_Asset_assetMaterializations_runOrError_PipelineRun {
  __typename: "PipelineRun";
  id: string;
  runId: string;
  mode: string;
  status: PipelineRunStatus;
  pipelineName: string;
  pipelineSnapshotId: string | null;
}

export type LooseDependencyPipelineNodeQuery_nodesOrError_Nodes_nodes_Asset_assetMaterializations_runOrError = LooseDependencyPipelineNodeQuery_nodesOrError_Nodes_nodes_Asset_assetMaterializations_runOrError_PipelineRunNotFoundError | LooseDependencyPipelineNodeQuery_nodesOrError_Nodes_nodes_Asset_assetMaterializations_runOrError_PipelineRun;

export interface LooseDependencyPipelineNodeQuery_nodesOrError_Nodes_nodes_Asset_assetMaterializations_materializationEvent_stepStats {
  __typename: "PipelineRunStepStats";
  endTime: number | null;
  startTime: number | null;
}

export interface LooseDependencyPipelineNodeQuery_nodesOrError_Nodes_nodes_Asset_assetMaterializations_materializationEvent_materialization_metadataEntries_EventPathMetadataEntry {
  __typename: "EventPathMetadataEntry";
  label: string;
  description: string | null;
  path: string;
}

export interface LooseDependencyPipelineNodeQuery_nodesOrError_Nodes_nodes_Asset_assetMaterializations_materializationEvent_materialization_metadataEntries_EventJsonMetadataEntry {
  __typename: "EventJsonMetadataEntry";
  label: string;
  description: string | null;
  jsonString: string;
}

export interface LooseDependencyPipelineNodeQuery_nodesOrError_Nodes_nodes_Asset_assetMaterializations_materializationEvent_materialization_metadataEntries_EventUrlMetadataEntry {
  __typename: "EventUrlMetadataEntry";
  label: string;
  description: string | null;
  url: string;
}

export interface LooseDependencyPipelineNodeQuery_nodesOrError_Nodes_nodes_Asset_assetMaterializations_materializationEvent_materialization_metadataEntries_EventTextMetadataEntry {
  __typename: "EventTextMetadataEntry";
  label: string;
  description: string | null;
  text: string;
}

export interface LooseDependencyPipelineNodeQuery_nodesOrError_Nodes_nodes_Asset_assetMaterializations_materializationEvent_materialization_metadataEntries_EventMarkdownMetadataEntry {
  __typename: "EventMarkdownMetadataEntry";
  label: string;
  description: string | null;
  mdStr: string;
}

export interface LooseDependencyPipelineNodeQuery_nodesOrError_Nodes_nodes_Asset_assetMaterializations_materializationEvent_materialization_metadataEntries_EventPythonArtifactMetadataEntry {
  __typename: "EventPythonArtifactMetadataEntry";
  label: string;
  description: string | null;
  module: string;
  name: string;
}

export interface LooseDependencyPipelineNodeQuery_nodesOrError_Nodes_nodes_Asset_assetMaterializations_materializationEvent_materialization_metadataEntries_EventFloatMetadataEntry {
  __typename: "EventFloatMetadataEntry";
  label: string;
  description: string | null;
  floatValue: number | null;
}

export interface LooseDependencyPipelineNodeQuery_nodesOrError_Nodes_nodes_Asset_assetMaterializations_materializationEvent_materialization_metadataEntries_EventIntMetadataEntry {
  __typename: "EventIntMetadataEntry";
  label: string;
  description: string | null;
  intValue: number | null;
  intRepr: string;
}

export interface LooseDependencyPipelineNodeQuery_nodesOrError_Nodes_nodes_Asset_assetMaterializations_materializationEvent_materialization_metadataEntries_EventPipelineRunMetadataEntry {
  __typename: "EventPipelineRunMetadataEntry";
  label: string;
  description: string | null;
  runId: string;
}

export interface LooseDependencyPipelineNodeQuery_nodesOrError_Nodes_nodes_Asset_assetMaterializations_materializationEvent_materialization_metadataEntries_EventAssetMetadataEntry_assetKey {
  __typename: "AssetKey";
  path: string[];
}

export interface LooseDependencyPipelineNodeQuery_nodesOrError_Nodes_nodes_Asset_assetMaterializations_materializationEvent_materialization_metadataEntries_EventAssetMetadataEntry {
  __typename: "EventAssetMetadataEntry";
  label: string;
  description: string | null;
  assetKey: LooseDependencyPipelineNodeQuery_nodesOrError_Nodes_nodes_Asset_assetMaterializations_materializationEvent_materialization_metadataEntries_EventAssetMetadataEntry_assetKey;
}

export type LooseDependencyPipelineNodeQuery_nodesOrError_Nodes_nodes_Asset_assetMaterializations_materializationEvent_materialization_metadataEntries = LooseDependencyPipelineNodeQuery_nodesOrError_Nodes_nodes_Asset_assetMaterializations_materializationEvent_materialization_metadataEntries_EventPathMetadataEntry | LooseDependencyPipelineNodeQuery_nodesOrError_Nodes_nodes_Asset_assetMaterializations_materializationEvent_materialization_metadataEntries_EventJsonMetadataEntry | LooseDependencyPipelineNodeQuery_nodesOrError_Nodes_nodes_Asset_assetMaterializations_materializationEvent_materialization_metadataEntries_EventUrlMetadataEntry | LooseDependencyPipelineNodeQuery_nodesOrError_Nodes_nodes_Asset_assetMaterializations_materializationEvent_materialization_metadataEntries_EventTextMetadataEntry | LooseDependencyPipelineNodeQuery_nodesOrError_Nodes_nodes_Asset_assetMaterializations_materializationEvent_materialization_metadataEntries_EventMarkdownMetadataEntry | LooseDependencyPipelineNodeQuery_nodesOrError_Nodes_nodes_Asset_assetMaterializations_materializationEvent_materialization_metadataEntries_EventPythonArtifactMetadataEntry | LooseDependencyPipelineNodeQuery_nodesOrError_Nodes_nodes_Asset_assetMaterializations_materializationEvent_materialization_metadataEntries_EventFloatMetadataEntry | LooseDependencyPipelineNodeQuery_nodesOrError_Nodes_nodes_Asset_assetMaterializations_materializationEvent_materialization_metadataEntries_EventIntMetadataEntry | LooseDependencyPipelineNodeQuery_nodesOrError_Nodes_nodes_Asset_assetMaterializations_materializationEvent_materialization_metadataEntries_EventPipelineRunMetadataEntry | LooseDependencyPipelineNodeQuery_nodesOrError_Nodes_nodes_Asset_assetMaterializations_materializationEvent_materialization_metadataEntries_EventAssetMetadataEntry;

export interface LooseDependencyPipelineNodeQuery_nodesOrError_Nodes_nodes_Asset_assetMaterializations_materializationEvent_materialization {
  __typename: "Materialization";
  label: string;
  description: string | null;
  metadataEntries: LooseDependencyPipelineNodeQuery_nodesOrError_Nodes_nodes_Asset_assetMaterializations_materializationEvent_materialization_metadataEntries[];
}

export interface LooseDependencyPipelineNodeQuery_nodesOrError_Nodes_nodes_Asset_assetMaterializations_materializationEvent_assetLineage_assetKey {
  __typename: "AssetKey";
  path: string[];
}

export interface LooseDependencyPipelineNodeQuery_nodesOrError_Nodes_nodes_Asset_assetMaterializations_materializationEvent_assetLineage {
  __typename: "AssetLineageInfo";
  assetKey: LooseDependencyPipelineNodeQuery_nodesOrError_Nodes_nodes_Asset_assetMaterializations_materializationEvent_assetLineage_assetKey;
  partitions: string[];
}

export interface LooseDependencyPipelineNodeQuery_nodesOrError_Nodes_nodes_Asset_assetMaterializations_materializationEvent {
  __typename: "StepMaterializationEvent";
  runId: string;
  timestamp: string;
  stepKey: string | null;
  stepStats: LooseDependencyPipelineNodeQuery_nodesOrError_Nodes_nodes_Asset_assetMaterializations_materializationEvent_stepStats;
  materialization: LooseDependencyPipelineNodeQuery_nodesOrError_Nodes_nodes_Asset_assetMaterializations_materializationEvent_materialization;
  assetLineage: LooseDependencyPipelineNodeQuery_nodesOrError_Nodes_nodes_Asset_assetMaterializations_materializationEvent_assetLineage[];
}

export interface LooseDependencyPipelineNodeQuery_nodesOrError_Nodes_nodes_Asset_assetMaterializations {
  __typename: "AssetMaterialization";
  partition: string | null;
  runOrError: LooseDependencyPipelineNodeQuery_nodesOrError_Nodes_nodes_Asset_assetMaterializations_runOrError;
  materializationEvent: LooseDependencyPipelineNodeQuery_nodesOrError_Nodes_nodes_Asset_assetMaterializations_materializationEvent;
}

export interface LooseDependencyPipelineNodeQuery_nodesOrError_Nodes_nodes_Asset {
  __typename: "Asset";
  id: string;
  key: LooseDependencyPipelineNodeQuery_nodesOrError_Nodes_nodes_Asset_key;
  mostRecentMaterialization: LooseDependencyPipelineNodeQuery_nodesOrError_Nodes_nodes_Asset_mostRecentMaterialization[];
  assetMaterializations: LooseDependencyPipelineNodeQuery_nodesOrError_Nodes_nodes_Asset_assetMaterializations[];
}

export interface LooseDependencyPipelineNodeQuery_nodesOrError_Nodes_nodes_Sensor_sensorState_ticks_error_cause {
  __typename: "PythonError";
  message: string;
  stack: string[];
}

export interface LooseDependencyPipelineNodeQuery_nodesOrError_Nodes_nodes_Sensor_sensorState_ticks_error {
  __typename: "PythonError";
  message: string;
  stack: string[];
  cause: LooseDependencyPipelineNodeQuery_nodesOrError_Nodes_nodes_Sensor_sensorState_ticks_error_cause | null;
}

export interface LooseDependencyPipelineNodeQuery_nodesOrError_Nodes_nodes_Sensor_sensorState_ticks {
  __typename: "InstigationTick";
  id: string;
  status: InstigationTickStatus;
  timestamp: number;
  skipReason: string | null;
  runIds: string[];
  error: LooseDependencyPipelineNodeQuery_nodesOrError_Nodes_nodes_Sensor_sensorState_ticks_error | null;
}

export interface LooseDependencyPipelineNodeQuery_nodesOrError_Nodes_nodes_Sensor_sensorState {
  __typename: "InstigationState";
  id: string;
  status: InstigationStatus;
  ticks: LooseDependencyPipelineNodeQuery_nodesOrError_Nodes_nodes_Sensor_sensorState_ticks[];
}

export interface LooseDependencyPipelineNodeQuery_nodesOrError_Nodes_nodes_Sensor {
  __typename: "Sensor";
  id: string;
  name: string;
  minIntervalSeconds: number;
  sensorState: LooseDependencyPipelineNodeQuery_nodesOrError_Nodes_nodes_Sensor_sensorState;
}

export type LooseDependencyPipelineNodeQuery_nodesOrError_Nodes_nodes = LooseDependencyPipelineNodeQuery_nodesOrError_Nodes_nodes_Pipeline | LooseDependencyPipelineNodeQuery_nodesOrError_Nodes_nodes_Schedule | LooseDependencyPipelineNodeQuery_nodesOrError_Nodes_nodes_Asset | LooseDependencyPipelineNodeQuery_nodesOrError_Nodes_nodes_Sensor;

export interface LooseDependencyPipelineNodeQuery_nodesOrError_Nodes {
  __typename: "Nodes";
  nodes: LooseDependencyPipelineNodeQuery_nodesOrError_Nodes_nodes[];
}

export type LooseDependencyPipelineNodeQuery_nodesOrError = LooseDependencyPipelineNodeQuery_nodesOrError_PythonError | LooseDependencyPipelineNodeQuery_nodesOrError_Nodes;

export interface LooseDependencyPipelineNodeQuery {
  nodesOrError: LooseDependencyPipelineNodeQuery_nodesOrError;
}

export interface LooseDependencyPipelineNodeQueryVariables {
  params: NodeParam[];
  limit?: number | null;
  before?: string | null;
}
