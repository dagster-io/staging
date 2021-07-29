// @generated
/* tslint:disable */
/* eslint-disable */
// @generated
// This file was automatically generated and should not be edited.

import { PipelineRunStatus } from "./../../types/globalTypes";

// ====================================================
// GraphQL fragment: LooseDependencyAssetFragment
// ====================================================

export interface LooseDependencyAssetFragment_key {
  __typename: "AssetKey";
  path: string[];
}

export interface LooseDependencyAssetFragment_mostRecentMaterialization_materializationEvent {
  __typename: "StepMaterializationEvent";
  timestamp: string;
}

export interface LooseDependencyAssetFragment_mostRecentMaterialization {
  __typename: "AssetMaterialization";
  materializationEvent: LooseDependencyAssetFragment_mostRecentMaterialization_materializationEvent;
}

export interface LooseDependencyAssetFragment_assetMaterializations_runOrError_PipelineRunNotFoundError {
  __typename: "PipelineRunNotFoundError" | "PythonError";
}

export interface LooseDependencyAssetFragment_assetMaterializations_runOrError_PipelineRun {
  __typename: "PipelineRun";
  id: string;
  runId: string;
  mode: string;
  status: PipelineRunStatus;
  pipelineName: string;
  pipelineSnapshotId: string | null;
}

export type LooseDependencyAssetFragment_assetMaterializations_runOrError = LooseDependencyAssetFragment_assetMaterializations_runOrError_PipelineRunNotFoundError | LooseDependencyAssetFragment_assetMaterializations_runOrError_PipelineRun;

export interface LooseDependencyAssetFragment_assetMaterializations_materializationEvent_stepStats {
  __typename: "PipelineRunStepStats";
  endTime: number | null;
  startTime: number | null;
}

export interface LooseDependencyAssetFragment_assetMaterializations_materializationEvent_materialization_metadataEntries_EventPathMetadataEntry {
  __typename: "EventPathMetadataEntry";
  label: string;
  description: string | null;
  path: string;
}

export interface LooseDependencyAssetFragment_assetMaterializations_materializationEvent_materialization_metadataEntries_EventJsonMetadataEntry {
  __typename: "EventJsonMetadataEntry";
  label: string;
  description: string | null;
  jsonString: string;
}

export interface LooseDependencyAssetFragment_assetMaterializations_materializationEvent_materialization_metadataEntries_EventUrlMetadataEntry {
  __typename: "EventUrlMetadataEntry";
  label: string;
  description: string | null;
  url: string;
}

export interface LooseDependencyAssetFragment_assetMaterializations_materializationEvent_materialization_metadataEntries_EventTextMetadataEntry {
  __typename: "EventTextMetadataEntry";
  label: string;
  description: string | null;
  text: string;
}

export interface LooseDependencyAssetFragment_assetMaterializations_materializationEvent_materialization_metadataEntries_EventMarkdownMetadataEntry {
  __typename: "EventMarkdownMetadataEntry";
  label: string;
  description: string | null;
  mdStr: string;
}

export interface LooseDependencyAssetFragment_assetMaterializations_materializationEvent_materialization_metadataEntries_EventPythonArtifactMetadataEntry {
  __typename: "EventPythonArtifactMetadataEntry";
  label: string;
  description: string | null;
  module: string;
  name: string;
}

export interface LooseDependencyAssetFragment_assetMaterializations_materializationEvent_materialization_metadataEntries_EventFloatMetadataEntry {
  __typename: "EventFloatMetadataEntry";
  label: string;
  description: string | null;
  floatValue: number | null;
}

export interface LooseDependencyAssetFragment_assetMaterializations_materializationEvent_materialization_metadataEntries_EventIntMetadataEntry {
  __typename: "EventIntMetadataEntry";
  label: string;
  description: string | null;
  intValue: number | null;
  intRepr: string;
}

export interface LooseDependencyAssetFragment_assetMaterializations_materializationEvent_materialization_metadataEntries_EventPipelineRunMetadataEntry {
  __typename: "EventPipelineRunMetadataEntry";
  label: string;
  description: string | null;
  runId: string;
}

export interface LooseDependencyAssetFragment_assetMaterializations_materializationEvent_materialization_metadataEntries_EventAssetMetadataEntry_assetKey {
  __typename: "AssetKey";
  path: string[];
}

export interface LooseDependencyAssetFragment_assetMaterializations_materializationEvent_materialization_metadataEntries_EventAssetMetadataEntry {
  __typename: "EventAssetMetadataEntry";
  label: string;
  description: string | null;
  assetKey: LooseDependencyAssetFragment_assetMaterializations_materializationEvent_materialization_metadataEntries_EventAssetMetadataEntry_assetKey;
}

export type LooseDependencyAssetFragment_assetMaterializations_materializationEvent_materialization_metadataEntries = LooseDependencyAssetFragment_assetMaterializations_materializationEvent_materialization_metadataEntries_EventPathMetadataEntry | LooseDependencyAssetFragment_assetMaterializations_materializationEvent_materialization_metadataEntries_EventJsonMetadataEntry | LooseDependencyAssetFragment_assetMaterializations_materializationEvent_materialization_metadataEntries_EventUrlMetadataEntry | LooseDependencyAssetFragment_assetMaterializations_materializationEvent_materialization_metadataEntries_EventTextMetadataEntry | LooseDependencyAssetFragment_assetMaterializations_materializationEvent_materialization_metadataEntries_EventMarkdownMetadataEntry | LooseDependencyAssetFragment_assetMaterializations_materializationEvent_materialization_metadataEntries_EventPythonArtifactMetadataEntry | LooseDependencyAssetFragment_assetMaterializations_materializationEvent_materialization_metadataEntries_EventFloatMetadataEntry | LooseDependencyAssetFragment_assetMaterializations_materializationEvent_materialization_metadataEntries_EventIntMetadataEntry | LooseDependencyAssetFragment_assetMaterializations_materializationEvent_materialization_metadataEntries_EventPipelineRunMetadataEntry | LooseDependencyAssetFragment_assetMaterializations_materializationEvent_materialization_metadataEntries_EventAssetMetadataEntry;

export interface LooseDependencyAssetFragment_assetMaterializations_materializationEvent_materialization {
  __typename: "Materialization";
  label: string;
  description: string | null;
  metadataEntries: LooseDependencyAssetFragment_assetMaterializations_materializationEvent_materialization_metadataEntries[];
}

export interface LooseDependencyAssetFragment_assetMaterializations_materializationEvent_assetLineage_assetKey {
  __typename: "AssetKey";
  path: string[];
}

export interface LooseDependencyAssetFragment_assetMaterializations_materializationEvent_assetLineage {
  __typename: "AssetLineageInfo";
  assetKey: LooseDependencyAssetFragment_assetMaterializations_materializationEvent_assetLineage_assetKey;
  partitions: string[];
}

export interface LooseDependencyAssetFragment_assetMaterializations_materializationEvent {
  __typename: "StepMaterializationEvent";
  runId: string;
  timestamp: string;
  stepKey: string | null;
  stepStats: LooseDependencyAssetFragment_assetMaterializations_materializationEvent_stepStats;
  materialization: LooseDependencyAssetFragment_assetMaterializations_materializationEvent_materialization;
  assetLineage: LooseDependencyAssetFragment_assetMaterializations_materializationEvent_assetLineage[];
}

export interface LooseDependencyAssetFragment_assetMaterializations {
  __typename: "AssetMaterialization";
  partition: string | null;
  runOrError: LooseDependencyAssetFragment_assetMaterializations_runOrError;
  materializationEvent: LooseDependencyAssetFragment_assetMaterializations_materializationEvent;
}

export interface LooseDependencyAssetFragment {
  __typename: "Asset";
  id: string;
  key: LooseDependencyAssetFragment_key;
  mostRecentMaterialization: LooseDependencyAssetFragment_mostRecentMaterialization[];
  assetMaterializations: LooseDependencyAssetFragment_assetMaterializations[];
}
