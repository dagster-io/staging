// @generated
/* tslint:disable */
/* eslint-disable */
// This file was automatically generated and should not be edited.

import { RepositorySelector } from "./globalTypes";

// ====================================================
// GraphQL query operation: FetchTriggerConfig
// ====================================================

export interface FetchTriggerConfig_triggerDefinitionOrError_RepositoryNotFoundError {
  __typename: "RepositoryNotFoundError" | "TriggerDefinitionNotFoundError";
}

export interface FetchTriggerConfig_triggerDefinitionOrError_TriggerDefinition_runConfigOrError_RunConfig {
  __typename: "RunConfig";
  yaml: string;
}

export interface FetchTriggerConfig_triggerDefinitionOrError_TriggerDefinition_runConfigOrError_PythonError_cause {
  __typename: "PythonError";
  message: string;
  stack: string[];
}

export interface FetchTriggerConfig_triggerDefinitionOrError_TriggerDefinition_runConfigOrError_PythonError {
  __typename: "PythonError";
  message: string;
  stack: string[];
  cause: FetchTriggerConfig_triggerDefinitionOrError_TriggerDefinition_runConfigOrError_PythonError_cause | null;
}

export type FetchTriggerConfig_triggerDefinitionOrError_TriggerDefinition_runConfigOrError = FetchTriggerConfig_triggerDefinitionOrError_TriggerDefinition_runConfigOrError_RunConfig | FetchTriggerConfig_triggerDefinitionOrError_TriggerDefinition_runConfigOrError_PythonError;

export interface FetchTriggerConfig_triggerDefinitionOrError_TriggerDefinition {
  __typename: "TriggerDefinition";
  runConfigOrError: FetchTriggerConfig_triggerDefinitionOrError_TriggerDefinition_runConfigOrError | null;
}

export interface FetchTriggerConfig_triggerDefinitionOrError_PythonError_cause {
  __typename: "PythonError";
  message: string;
  stack: string[];
}

export interface FetchTriggerConfig_triggerDefinitionOrError_PythonError {
  __typename: "PythonError";
  message: string;
  stack: string[];
  cause: FetchTriggerConfig_triggerDefinitionOrError_PythonError_cause | null;
}

export type FetchTriggerConfig_triggerDefinitionOrError = FetchTriggerConfig_triggerDefinitionOrError_RepositoryNotFoundError | FetchTriggerConfig_triggerDefinitionOrError_TriggerDefinition | FetchTriggerConfig_triggerDefinitionOrError_PythonError;

export interface FetchTriggerConfig {
  triggerDefinitionOrError: FetchTriggerConfig_triggerDefinitionOrError;
}

export interface FetchTriggerConfigVariables {
  repositorySelector: RepositorySelector;
  triggerName: string;
}
