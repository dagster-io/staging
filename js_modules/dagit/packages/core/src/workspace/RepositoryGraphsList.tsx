import {gql, useQuery} from '@apollo/client';
import {Colors, NonIdealState} from '@blueprintjs/core';
import * as React from 'react';
import {Link} from 'react-router-dom';
import styled from 'styled-components/macro';

import {Group} from '../ui/Group';
import {Page} from '../ui/Page';
import {Table} from '../ui/Table';

import {repoAddressAsString} from './repoAddressAsString';
import {repoAddressToSelector} from './repoAddressToSelector';
import {RepoAddress} from './types';
import {RepositoryGraphsListQuery} from './types/RepositoryGraphsListQuery';
import {workspacePath} from './workspacePath';

const REPOSITORY_GRAPHS_LIST_QUERY = gql`
  query RepositoryGraphsListQuery($repositorySelector: RepositorySelector!) {
    repositoryOrError(repositorySelector: $repositorySelector) {
      __typename
      ... on Repository {
        id
        pipelines {
          id
          description
          name
        }
      }
      ... on RepositoryNotFoundError {
        message
      }
    }
  }
`;

interface Props {
  repoAddress: RepoAddress;
}

export const RepositoryGraphsList: React.FC<Props> = (props) => {
  const {repoAddress} = props;
  const repositorySelector = repoAddressToSelector(repoAddress);

  const {data, error, loading} = useQuery<RepositoryGraphsListQuery>(REPOSITORY_GRAPHS_LIST_QUERY, {
    fetchPolicy: 'cache-and-network',
    variables: {repositorySelector},
  });

  const repo = data?.repositoryOrError;
  const graphsForTable = React.useMemo(() => {
    if (!repo || repo.__typename !== 'Repository') {
      return null;
    }
    return repo.pipelines.map((pipeline) => ({
      pipeline,
      repoAddress,
    }));
  }, [repo, repoAddress]);

  if (loading) {
    return null;
  }

  if (error || !graphsForTable) {
    return (
      <NonIdealState
        title="Unable to load graphs"
        description={`Could not load graphs for ${repoAddressAsString(repoAddress)}`}
      />
    );
  }

  return (
    <Page>
      <Table>
        <thead>
          <tr>
            <th style={{width: '50%', minWidth: '400px'}}>Graph</th>
          </tr>
        </thead>
        <tbody>
          {graphsForTable.map(({pipeline, repoAddress}) => (
            <tr key={`${pipeline.name}-${repoAddressAsString(repoAddress)}`}>
              <td>
                <Group direction="column" spacing={4}>
                  <Link
                    to={workspacePath(
                      repoAddress.name,
                      repoAddress.location,
                      `/graphs/${pipeline.name}`,
                    )}
                  >
                    {pipeline.name}
                  </Link>
                  <Description>{pipeline.description}</Description>
                </Group>
              </td>
            </tr>
          ))}
        </tbody>
      </Table>
    </Page>
  );
};

const Description = styled.div`
  color: ${Colors.GRAY3};
  font-size: 12px;
`;
