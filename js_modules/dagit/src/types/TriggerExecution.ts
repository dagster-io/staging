// @generated
/* tslint:disable */
/* eslint-disable */
// This file was automatically generated and should not be edited.

import { TriggerSelector } from "./globalTypes";

// ====================================================
// GraphQL mutation operation: TriggerExecution
// ====================================================

export interface TriggerExecution_triggerExecution_InvalidStepError {
  __typename: "InvalidStepError" | "InvalidOutputError" | "PipelineConfigValidationInvalid" | "PipelineNotFoundError" | "PipelineRunConflict" | "PresetNotFoundError" | "ConflictingExecutionParamsError";
}

export interface TriggerExecution_triggerExecution_TriggerExecutionSuccess {
  __typename: "TriggerExecutionSuccess";
  launchedRunIds: string[];
}

export interface TriggerExecution_triggerExecution_PythonError {
  __typename: "PythonError";
  message: string;
  stack: string[];
}

export type TriggerExecution_triggerExecution = TriggerExecution_triggerExecution_InvalidStepError | TriggerExecution_triggerExecution_TriggerExecutionSuccess | TriggerExecution_triggerExecution_PythonError;

export interface TriggerExecution {
  triggerExecution: TriggerExecution_triggerExecution;
}

export interface TriggerExecutionVariables {
  triggerSelector: TriggerSelector;
}
