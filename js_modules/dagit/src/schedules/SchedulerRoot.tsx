import {gql, useQuery} from '@apollo/client';
import {Button, Callout, Code, Divider, IBreadcrumbProps, Intent} from '@blueprintjs/core';
import React, {useState} from 'react';

import {ScrollContainer} from 'src/ListComponents';
import {Loading} from 'src/Loading';
import {PythonErrorInfo} from 'src/PythonErrorInfo';
import {RepositoryInformationFragment} from 'src/RepositoryInformation';
import {useDocumentTitle} from 'src/hooks/useDocumentTitle';
import {TopNav} from 'src/nav/TopNav';
import {
  SCHEDULE_DEFINITION_FRAGMENT,
  SCHEDULE_STATE_FRAGMENT,
  SchedulerTimezoneNote,
} from 'src/schedules/ScheduleUtils';
import {SCHEDULER_FRAGMENT, SchedulerInfo} from 'src/schedules/SchedulerInfo';
import {SchedulesTable} from 'src/schedules/SchedulesRoot';
import {
  SchedulerRootQuery,
  SchedulerRootQuery_scheduler,
  SchedulerRootQuery_repositoriesOrError_RepositoryConnection_nodes,
} from 'src/schedules/types/SchedulerRootQuery';

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
            const {scheduler, repositoriesOrError} = result;

            let repositoryDefinitionsSection = null;

            if (repositoriesOrError.__typename === 'PythonError') {
              repositoryDefinitionsSection = <PythonErrorInfo error={repositoriesOrError} />;
            } else {
              repositoryDefinitionsSection = (
                <ScheduleStates
                  repositoriesOrError={repositoriesOrError.nodes}
                  schedulerOrError={scheduler}
                />
              );
            }

            return (
              <>
                <SchedulerInfo schedulerOrError={scheduler} />
                {repositoryDefinitionsSection}
              </>
            );
          }}
        </Loading>
      </div>
    </ScrollContainer>
  );
};

export const UnloadableScheduleInfo = () => {
  const [showMore, setShowMore] = useState(false);

  return (
    <Callout style={{marginBottom: 20}} intent={Intent.WARNING}>
      <div style={{display: 'flex', justifyContent: 'space-between'}}>
        <h4 style={{margin: 0}}>
          Note: You can turn off any of following running schedules, but you cannot turn them back
          on.{' '}
        </h4>

        {!showMore && (
          <Button small={true} onClick={() => setShowMore(true)}>
            Show more info
          </Button>
        )}
      </div>

      {showMore && (
        <div style={{marginTop: 10}}>
          <p>
            Each schedule below was been previously reconciled and stored, but its corresponding{' '}
            <Code>ScheduleDefinition</Code> is not available in any of the currently loaded
            repositories. This is most likely because the schedule definition belongs to a workspace
            different than the one currently loaded, or because the repository origin for the
            schedule definition has changed.
          </p>
        </div>
      )}
    </Callout>
  );
};

export const ScheduleStates: React.FunctionComponent<{
  repositoriesOrError: SchedulerRootQuery_repositoriesOrError_RepositoryConnection_nodes[];
  schedulerOrError: SchedulerRootQuery_scheduler;
}> = ({repositoriesOrError, schedulerOrError}) => {
  return (
    <div>
      <div style={{display: 'flex'}}>
        <h2 style={{marginBottom: 0}}>All Schedules:</h2>
        <div style={{flex: 1}} />
        <SchedulerTimezoneNote schedulerOrError={schedulerOrError} />
      </div>
      <Divider />
      {repositoriesOrError.map((repository) => (
        <div style={{marginTop: 32}} key={repository.name}>
          <SchedulesTable repository={repository} />
        </div>
      ))}
    </div>
  );
};

const SCHEDULER_ROOT_QUERY = gql`
  query SchedulerRootQuery {
    repositoriesOrError {
      __typename
      ... on RepositoryConnection {
        nodes {
          name
          id
          scheduleDefinitions {
            ...ScheduleDefinitionFragment
          }
          ...RepositoryInfoFragment
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
  ${RepositoryInformationFragment}
  ${SCHEDULE_STATE_FRAGMENT}
`;
