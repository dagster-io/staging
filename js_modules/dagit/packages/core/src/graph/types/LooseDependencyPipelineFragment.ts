// @generated
/* tslint:disable */
/* eslint-disable */
// @generated
// This file was automatically generated and should not be edited.

import { PipelineRunStatus } from "./../../types/globalTypes";

// ====================================================
// GraphQL fragment: LooseDependencyPipelineFragment
// ====================================================

export interface LooseDependencyPipelineFragment_runs {
  __typename: "PipelineRun";
  id: string;
  runId: string;
  status: PipelineRunStatus;
}

export interface LooseDependencyPipelineFragment {
  __typename: "Pipeline";
  id: string;
  name: string;
  runs: LooseDependencyPipelineFragment_runs[];
}
