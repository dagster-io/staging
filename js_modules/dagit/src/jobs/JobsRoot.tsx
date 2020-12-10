import {useQuery, gql} from '@apollo/client';
import {NonIdealState} from '@blueprintjs/core';
import {IconNames} from '@blueprintjs/icons';
import React from 'react';

import {Loading} from 'src/Loading';
import {PythonErrorInfo} from 'src/PythonErrorInfo';
import {REPOSITORY_INFO_FRAGMENT} from 'src/RepositoryInformation';
import {useDocumentTitle} from 'src/hooks/useDocumentTitle';
import {JobsRootQuery} from 'src/jobs/types/JobsRootQuery';
import {SCHEDULE_FRAGMENT} from 'src/schedules/ScheduleUtils';
import {SchedulesTable} from 'src/schedules/SchedulesTable';
import {SENSOR_FRAGMENT} from 'src/sensors/SensorFragment';
import {SensorsTable} from 'src/sensors/SensorsTable';
import {Group} from 'src/ui/Group';
import {Page} from 'src/ui/Page';
import {Subheading} from 'src/ui/Text';
import {repoAddressToSelector} from 'src/workspace/repoAddressToSelector';
import {RepoAddress} from 'src/workspace/types';

interface Props {
  repoAddress: RepoAddress;
}

export const JobsRoot = (props: Props) => {
  const {repoAddress} = props;
  useDocumentTitle('Jobs');
  const repositorySelector = repoAddressToSelector(repoAddress);

  const queryResult = useQuery<JobsRootQuery>(JOBS_ROOT_QUERY, {
    variables: {
      repositorySelector: repositorySelector,
    },
    fetchPolicy: 'cache-and-network',
    pollInterval: 50 * 1000,
    partialRefetch: true,
  });

  return (
    <Page>
      <Loading queryResult={queryResult} allowStaleData={true}>
        {(result) => {
          const {repositoryOrError: repository} = result;
          const content = () => {
            if (repository.__typename === 'PythonError') {
              return <PythonErrorInfo error={repository} />;
            }
            if (repository.__typename === 'RepositoryNotFoundError') {
              return (
                <NonIdealState
                  icon={IconNames.ERROR}
                  title="Repository not found"
                  description="Could not load this repository."
                />
              );
            }

            const schedules = repository.schedules.length ? (
              <Group direction="vertical" spacing={12}>
                <Subheading>Schedules</Subheading>
                <SchedulesTable repoAddress={repoAddress} schedules={repository.schedules} />
              </Group>
            ) : null;

            const sensors = repository.sensors.length ? (
              <Group direction="vertical" spacing={12}>
                <Subheading>Sensors</Subheading>
                <SensorsTable repoAddress={repoAddress} sensors={repository.sensors} />
              </Group>
            ) : null;

            return (
              <Group direction="vertical" spacing={20}>
                {schedules}
                {sensors}
              </Group>
            );
          };

          return <div>{content()}</div>;
        }}
      </Loading>
    </Page>
  );
};

const JOBS_ROOT_QUERY = gql`
  query JobsRootQuery($repositorySelector: RepositorySelector!) {
    repositoryOrError(repositorySelector: $repositorySelector) {
      __typename
      ...PythonErrorFragment
      ...RepositoryInfoFragment
      ... on Repository {
        id
        schedules {
          id
          ...ScheduleFragment
        }
        sensors {
          id
          ...SensorFragment
        }
      }
    }
  }
  ${PythonErrorInfo.fragments.PythonErrorFragment}
  ${REPOSITORY_INFO_FRAGMENT}
  ${SCHEDULE_FRAGMENT}
  ${SENSOR_FRAGMENT}
`;
