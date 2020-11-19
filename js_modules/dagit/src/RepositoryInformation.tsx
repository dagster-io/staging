import {gql} from '@apollo/client';
import * as React from 'react';

import {RepositoryInfoFragment} from 'src/types/RepositoryInfoFragment';
import {RepositoryOriginFragment} from 'src/types/RepositoryOriginFragment';

export const RepositoryInformationFragment = gql`
  fragment RepositoryOriginFragment on RepositoryOrigin {
    ... on PythonRepositoryOrigin {
      executablePath
      pythonFile
      moduleName
      workingDirectory
      attribute
      packageName
    }
    ... on GrpcRepositoryOrigin {
      grpcUrl
    }
  }
  fragment RepositoryInfoFragment on Repository {
    id
    name
    origin {
      ...RepositoryOriginFragment
    }
    location {
      id
      name
    }
  }
`;

export const RepositoryOriginInformation: React.FunctionComponent<{
  origin: RepositoryOriginFragment;
  dagitExecutablePath?: string;
}> = ({origin, dagitExecutablePath}) => {
  if (origin.__typename === 'PythonRepositoryOrigin') {
    return (
      <>
        {origin.pythonFile ? (
          <>
            <span style={{marginRight: 5}}>python file:</span>
            <span style={{opacity: 0.5}}>{origin.pythonFile}</span>
          </>
        ) : null}
        {origin.moduleName ? (
          <>
            <span style={{marginRight: 5}}>python module:</span>
            <span style={{opacity: 0.5}}>{origin.moduleName}</span>
          </>
        ) : null}
        {origin.packageName ? (
          <>
            <span style={{marginRight: 5}}>python package:</span>
            <span style={{opacity: 0.5}}>{origin.packageName}</span>
          </>
        ) : null}
        {origin.attribute ? (
          <>
            <span style={{marginRight: 5}}>attribute:</span>
            <span style={{opacity: 0.5}}>{origin.attribute}</span>
          </>
        ) : null}
        {dagitExecutablePath && dagitExecutablePath === origin.executablePath ? null : (
          <div>
            <span style={{marginRight: 5}}>executable:</span>
            <span style={{opacity: 0.5}}>{origin.executablePath}</span>
          </div>
        )}
      </>
    );
  } else {
    return <div>{origin.grpcUrl}</div>;
  }
};

export const RepositoryInformation: React.FunctionComponent<{
  repository: RepositoryInfoFragment;
  dagitExecutablePath?: string;
}> = ({repository, dagitExecutablePath}) => {
  return (
    <div>
      <div>
        {repository.name}
        <span style={{marginRight: 5, marginLeft: 5}}>&middot;</span>
        <span style={{opacity: 0.5}}>{repository.location.name}</span>
      </div>
      <div style={{fontSize: 11, marginTop: 5}}>
        <RepositoryOriginInformation
          origin={repository.origin}
          dagitExecutablePath={dagitExecutablePath}
        />
      </div>
    </div>
  );
};
