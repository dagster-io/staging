// @generated
/* tslint:disable */
/* eslint-disable */
// This file was automatically generated and should not be edited.

import { PipelineRunStatus } from "./../../types/globalTypes";

// ====================================================
// GraphQL fragment: PartitionTablePartitionRunFragment
// ====================================================

export interface PartitionTablePartitionRunFragment {
  __typename: "PipelineRun";
  runId: string;
  status: PipelineRunStatus;
  pipelineName: string;
}
