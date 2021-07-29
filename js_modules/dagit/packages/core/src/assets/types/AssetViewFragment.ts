// @generated
/* tslint:disable */
/* eslint-disable */
// @generated
// This file was automatically generated and should not be edited.

import { PipelineRunStatus } from "./../../types/globalTypes";

// ====================================================
// GraphQL fragment: AssetViewFragment
// ====================================================

export interface AssetViewFragment_key {
  __typename: "AssetKey";
  path: string[];
}

export interface AssetViewFragment_mostRecentMaterialization_materializationEvent {
  __typename: "StepMaterializationEvent";
  timestamp: string;
}

export interface AssetViewFragment_mostRecentMaterialization {
  __typename: "AssetMaterialization";
  materializationEvent: AssetViewFragment_mostRecentMaterialization_materializationEvent;
}

export interface AssetViewFragment_assetMaterializations_runOrError_PipelineRunNotFoundError {
  __typename: "PipelineRunNotFoundError" | "PythonError";
}

export interface AssetViewFragment_assetMaterializations_runOrError_PipelineRun {
  __typename: "PipelineRun";
  id: string;
  runId: string;
  mode: string;
  status: PipelineRunStatus;
  pipelineName: string;
  pipelineSnapshotId: string | null;
}

export type AssetViewFragment_assetMaterializations_runOrError = AssetViewFragment_assetMaterializations_runOrError_PipelineRunNotFoundError | AssetViewFragment_assetMaterializations_runOrError_PipelineRun;

export interface AssetViewFragment_assetMaterializations_materializationEvent_stepStats {
  __typename: "PipelineRunStepStats";
  endTime: number | null;
  startTime: number | null;
}

export interface AssetViewFragment_assetMaterializations_materializationEvent_materialization_metadataEntries_EventPathMetadataEntry {
  __typename: "EventPathMetadataEntry";
  label: string;
  description: string | null;
  path: string;
}

export interface AssetViewFragment_assetMaterializations_materializationEvent_materialization_metadataEntries_EventJsonMetadataEntry {
  __typename: "EventJsonMetadataEntry";
  label: string;
  description: string | null;
  jsonString: string;
}

export interface AssetViewFragment_assetMaterializations_materializationEvent_materialization_metadataEntries_EventUrlMetadataEntry {
  __typename: "EventUrlMetadataEntry";
  label: string;
  description: string | null;
  url: string;
}

export interface AssetViewFragment_assetMaterializations_materializationEvent_materialization_metadataEntries_EventTextMetadataEntry {
  __typename: "EventTextMetadataEntry";
  label: string;
  description: string | null;
  text: string;
}

export interface AssetViewFragment_assetMaterializations_materializationEvent_materialization_metadataEntries_EventMarkdownMetadataEntry {
  __typename: "EventMarkdownMetadataEntry";
  label: string;
  description: string | null;
  mdStr: string;
}

export interface AssetViewFragment_assetMaterializations_materializationEvent_materialization_metadataEntries_EventPythonArtifactMetadataEntry {
  __typename: "EventPythonArtifactMetadataEntry";
  label: string;
  description: string | null;
  module: string;
  name: string;
}

export interface AssetViewFragment_assetMaterializations_materializationEvent_materialization_metadataEntries_EventFloatMetadataEntry {
  __typename: "EventFloatMetadataEntry";
  label: string;
  description: string | null;
  floatValue: number | null;
}

export interface AssetViewFragment_assetMaterializations_materializationEvent_materialization_metadataEntries_EventIntMetadataEntry {
  __typename: "EventIntMetadataEntry";
  label: string;
  description: string | null;
  intValue: number | null;
  intRepr: string;
}

export interface AssetViewFragment_assetMaterializations_materializationEvent_materialization_metadataEntries_EventPipelineRunMetadataEntry {
  __typename: "EventPipelineRunMetadataEntry";
  label: string;
  description: string | null;
  runId: string;
}

export interface AssetViewFragment_assetMaterializations_materializationEvent_materialization_metadataEntries_EventAssetMetadataEntry_assetKey {
  __typename: "AssetKey";
  path: string[];
}

export interface AssetViewFragment_assetMaterializations_materializationEvent_materialization_metadataEntries_EventAssetMetadataEntry {
  __typename: "EventAssetMetadataEntry";
  label: string;
  description: string | null;
  assetKey: AssetViewFragment_assetMaterializations_materializationEvent_materialization_metadataEntries_EventAssetMetadataEntry_assetKey;
}

export type AssetViewFragment_assetMaterializations_materializationEvent_materialization_metadataEntries = AssetViewFragment_assetMaterializations_materializationEvent_materialization_metadataEntries_EventPathMetadataEntry | AssetViewFragment_assetMaterializations_materializationEvent_materialization_metadataEntries_EventJsonMetadataEntry | AssetViewFragment_assetMaterializations_materializationEvent_materialization_metadataEntries_EventUrlMetadataEntry | AssetViewFragment_assetMaterializations_materializationEvent_materialization_metadataEntries_EventTextMetadataEntry | AssetViewFragment_assetMaterializations_materializationEvent_materialization_metadataEntries_EventMarkdownMetadataEntry | AssetViewFragment_assetMaterializations_materializationEvent_materialization_metadataEntries_EventPythonArtifactMetadataEntry | AssetViewFragment_assetMaterializations_materializationEvent_materialization_metadataEntries_EventFloatMetadataEntry | AssetViewFragment_assetMaterializations_materializationEvent_materialization_metadataEntries_EventIntMetadataEntry | AssetViewFragment_assetMaterializations_materializationEvent_materialization_metadataEntries_EventPipelineRunMetadataEntry | AssetViewFragment_assetMaterializations_materializationEvent_materialization_metadataEntries_EventAssetMetadataEntry;

export interface AssetViewFragment_assetMaterializations_materializationEvent_materialization {
  __typename: "Materialization";
  label: string;
  description: string | null;
  metadataEntries: AssetViewFragment_assetMaterializations_materializationEvent_materialization_metadataEntries[];
}

export interface AssetViewFragment_assetMaterializations_materializationEvent_assetLineage_assetKey {
  __typename: "AssetKey";
  path: string[];
}

export interface AssetViewFragment_assetMaterializations_materializationEvent_assetLineage {
  __typename: "AssetLineageInfo";
  assetKey: AssetViewFragment_assetMaterializations_materializationEvent_assetLineage_assetKey;
  partitions: string[];
}

export interface AssetViewFragment_assetMaterializations_materializationEvent {
  __typename: "StepMaterializationEvent";
  runId: string;
  timestamp: string;
  stepKey: string | null;
  stepStats: AssetViewFragment_assetMaterializations_materializationEvent_stepStats;
  materialization: AssetViewFragment_assetMaterializations_materializationEvent_materialization;
  assetLineage: AssetViewFragment_assetMaterializations_materializationEvent_assetLineage[];
}

export interface AssetViewFragment_assetMaterializations {
  __typename: "AssetMaterialization";
  partition: string | null;
  runOrError: AssetViewFragment_assetMaterializations_runOrError;
  materializationEvent: AssetViewFragment_assetMaterializations_materializationEvent;
}

export interface AssetViewFragment {
  __typename: "Asset";
  id: string;
  key: AssetViewFragment_key;
  mostRecentMaterialization: AssetViewFragment_mostRecentMaterialization[];
  assetMaterializations: AssetViewFragment_assetMaterializations[];
}
