import {gql, useQuery} from '@apollo/client';
import {Divider, IBreadcrumbProps, NonIdealState} from '@blueprintjs/core';
import {IconNames} from '@blueprintjs/icons';
import React from 'react';

import {ScrollContainer} from 'src/ListComponents';
import {Loading} from 'src/Loading';
import {PythonErrorInfo} from 'src/PythonErrorInfo';
import {useDocumentTitle} from 'src/hooks/useDocumentTitle';
import {TopNav} from 'src/nav/TopNav';
import {
  REPOSITORY_SCHEDULES_FRAGMENT,
  SCHEDULE_DEFINITION_FRAGMENT,
  SCHEDULE_STATE_FRAGMENT,
  SchedulerTimezoneNote,
} from 'src/schedules/ScheduleUtils';
import {SCHEDULER_FRAGMENT, SchedulerInfo} from 'src/schedules/SchedulerInfo';
import {SchedulesTable, UnLoadableSchedules} from 'src/schedules/SchedulesRoot';
import {SchedulerRootQuery} from 'src/schedules/types/SchedulerRootQuery';

export const SchedulerRoot = () => {
  useDocumentTitle('Scheduler');
  const queryResult = useQuery<SchedulerRootQuery>(SCHEDULER_ROOT_QUERY, {
    variables: {},
    fetchPolicy: 'cache-and-network',
  });

  const breadcrumbs: IBreadcrumbProps[] = [{icon: 'time', text: 'Scheduler'}];

  return (
    <ScrollContainer>
      <TopNav breadcrumbs={breadcrumbs} />
      <div style={{padding: '16px'}}>
        <Loading queryResult={queryResult} allowStaleData={true}>
          {(result) => {
            const {
              scheduler,
              repositoriesOrError,
              scheduleStatesOrError: unLoadableScheduleStates,
            } = result;

            let unLoadableSchedules = null;

            if (repositoriesOrError.__typename === 'PythonError') {
              return <PythonErrorInfo error={repositoriesOrError} />;
            }
            if (unLoadableScheduleStates.__typename === 'RepositoryNotFoundError') {
              return (
                <NonIdealState
                  icon={IconNames.ERROR}
                  title="Repository not found"
                  description="Could not load this repository."
                />
              );
            }
            if (unLoadableScheduleStates.__typename === 'PythonError') {
              return <PythonErrorInfo error={unLoadableScheduleStates} />;
            } else {
              unLoadableSchedules = unLoadableScheduleStates.results;
            }

            const repositoryDefinitionsSection = (
              <div>
                <div style={{display: 'flex'}}>
                  <h2 style={{marginBottom: 0}}>All Schedules:</h2>
                  <div style={{flex: 1}} />
                  <SchedulerTimezoneNote schedulerOrError={scheduler} />
                </div>
                <Divider />
                {repositoriesOrError.nodes.map((repository) => (
                  <div style={{marginTop: 32}} key={repository.name}>
                    <SchedulesTable repository={repository} />
                  </div>
                ))}
              </div>
            );

            return (
              <>
                <SchedulerInfo schedulerOrError={scheduler} />
                {repositoryDefinitionsSection}
                {unLoadableSchedules.length > 0 && (
                  <UnLoadableSchedules unLoadableSchedules={unLoadableSchedules} />
                )}
              </>
            );
          }}
        </Loading>
      </div>
    </ScrollContainer>
  );
};

const SCHEDULER_ROOT_QUERY = gql`
  query SchedulerRootQuery {
    repositoriesOrError {
      __typename
      ... on RepositoryConnection {
        nodes {
          ...RepositorySchedulesFragment
        }
      }
      ...PythonErrorFragment
    }
    scheduler {
      ...SchedulerFragment
    }
    scheduleStatesOrError(withNoScheduleDefinition: true) {
      __typename
      ... on ScheduleStates {
        results {
          ...ScheduleStateFragment
        }
      }
      ...PythonErrorFragment
    }
  }

  ${SCHEDULE_DEFINITION_FRAGMENT}
  ${SCHEDULER_FRAGMENT}
  ${PythonErrorInfo.fragments.PythonErrorFragment}
  ${REPOSITORY_SCHEDULES_FRAGMENT}
  ${SCHEDULE_STATE_FRAGMENT}
`;
