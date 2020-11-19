// @generated
/* tslint:disable */
/* eslint-disable */
// @generated
// This file was automatically generated and should not be edited.

// ====================================================
// GraphQL fragment: RepositoryInfoFragment
// ====================================================

export interface RepositoryInfoFragment_origin_PythonRepositoryOrigin {
  __typename: "PythonRepositoryOrigin";
  executablePath: string;
  pythonFile: string | null;
  moduleName: string | null;
  workingDirectory: string | null;
  attribute: string | null;
  packageName: string | null;
}

export interface RepositoryInfoFragment_origin_GrpcRepositoryOrigin {
  __typename: "GrpcRepositoryOrigin";
  grpcUrl: string;
}

export type RepositoryInfoFragment_origin = RepositoryInfoFragment_origin_PythonRepositoryOrigin | RepositoryInfoFragment_origin_GrpcRepositoryOrigin;

export interface RepositoryInfoFragment_location {
  __typename: "RepositoryLocation";
  id: string;
  name: string;
}

export interface RepositoryInfoFragment {
  __typename: "Repository";
  id: string;
  name: string;
  origin: RepositoryInfoFragment_origin;
  location: RepositoryInfoFragment_location;
}
