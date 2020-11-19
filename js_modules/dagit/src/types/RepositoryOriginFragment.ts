// @generated
/* tslint:disable */
/* eslint-disable */
// @generated
// This file was automatically generated and should not be edited.

// ====================================================
// GraphQL fragment: RepositoryOriginFragment
// ====================================================

export interface RepositoryOriginFragment_PythonRepositoryOrigin {
  __typename: "PythonRepositoryOrigin";
  executablePath: string;
  pythonFile: string | null;
  moduleName: string | null;
  workingDirectory: string | null;
  attribute: string | null;
  packageName: string | null;
}

export interface RepositoryOriginFragment_GrpcRepositoryOrigin {
  __typename: "GrpcRepositoryOrigin";
  grpcUrl: string;
}

export type RepositoryOriginFragment = RepositoryOriginFragment_PythonRepositoryOrigin | RepositoryOriginFragment_GrpcRepositoryOrigin;
