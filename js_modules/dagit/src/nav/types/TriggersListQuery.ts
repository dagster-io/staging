// @generated
/* tslint:disable */
/* eslint-disable */
// This file was automatically generated and should not be edited.

import { RepositorySelector } from "./../../types/globalTypes";

// ====================================================
// GraphQL query operation: TriggersListQuery
// ====================================================

export interface TriggersListQuery_triggerDefinitionsOrError_RepositoryNotFoundError {
  __typename: "RepositoryNotFoundError" | "PythonError";
}

export interface TriggersListQuery_triggerDefinitionsOrError_TriggerDefinitions_results {
  __typename: "TriggerDefinition";
  name: string;
}

export interface TriggersListQuery_triggerDefinitionsOrError_TriggerDefinitions {
  __typename: "TriggerDefinitions";
  results: TriggersListQuery_triggerDefinitionsOrError_TriggerDefinitions_results[];
}

export type TriggersListQuery_triggerDefinitionsOrError = TriggersListQuery_triggerDefinitionsOrError_RepositoryNotFoundError | TriggersListQuery_triggerDefinitionsOrError_TriggerDefinitions;

export interface TriggersListQuery {
  triggerDefinitionsOrError: TriggersListQuery_triggerDefinitionsOrError;
}

export interface TriggersListQueryVariables {
  repositorySelector: RepositorySelector;
}
