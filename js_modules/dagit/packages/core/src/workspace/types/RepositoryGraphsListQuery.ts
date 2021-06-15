// @generated
/* tslint:disable */
/* eslint-disable */
// @generated
// This file was automatically generated and should not be edited.

import { RepositorySelector } from "./../../types/globalTypes";

// ====================================================
// GraphQL query operation: RepositoryGraphsListQuery
// ====================================================

export interface RepositoryGraphsListQuery_repositoryOrError_PythonError {
  __typename: "PythonError";
}

export interface RepositoryGraphsListQuery_repositoryOrError_Repository_pipelines {
  __typename: "Pipeline";
  id: string;
  description: string | null;
  name: string;
}

export interface RepositoryGraphsListQuery_repositoryOrError_Repository {
  __typename: "Repository";
  id: string;
  pipelines: RepositoryGraphsListQuery_repositoryOrError_Repository_pipelines[];
}

export interface RepositoryGraphsListQuery_repositoryOrError_RepositoryNotFoundError {
  __typename: "RepositoryNotFoundError";
  message: string;
}

export type RepositoryGraphsListQuery_repositoryOrError = RepositoryGraphsListQuery_repositoryOrError_PythonError | RepositoryGraphsListQuery_repositoryOrError_Repository | RepositoryGraphsListQuery_repositoryOrError_RepositoryNotFoundError;

export interface RepositoryGraphsListQuery {
  repositoryOrError: RepositoryGraphsListQuery_repositoryOrError;
}

export interface RepositoryGraphsListQueryVariables {
  repositorySelector: RepositorySelector;
}
