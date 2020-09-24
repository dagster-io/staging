import {Button, Callout, Code, Divider, Intent} from '@blueprintjs/core';
import gql from 'graphql-tag';
import React, {useState} from 'react';
import {useQuery} from 'react-apollo';
import {useHistory} from 'react-router-dom';

import {
  DagsterRepoOption,
  useCurrentRepositoryState,
  useRepositoryOptions,
} from '../DagsterRepositoryContext';
import {Header, ScrollContainer} from '../ListComponents';
import Loading from '../Loading';
import PythonErrorInfo from '../PythonErrorInfo';

import {ScheduleStateRow} from './ScheduleRow';
import {SCHEDULE_STATE_FRAGMENT, SchedulerTimezoneNote} from './ScheduleUtils';
import {SCHEDULER_FRAGMENT, SchedulerInfo} from './SchedulerInfo';
import {
  SchedulerRootQuery,
  SchedulerRootQuery_scheduleStatesOrError,
  SchedulerRootQuery_scheduleStatesOrError_ScheduleStates_results,
} from './types/SchedulerRootQuery';

export const SchedulerRoot: React.FunctionComponent<{}> = () => {
  const queryResult = useQuery<SchedulerRootQuery>(SCHEDULER_ROOT_QUERY, {
    variables: {},
    fetchPolicy: 'cache-and-network',
  });

  return (
    <ScrollContainer>
      <Header>Scheduler</Header>
      <Loading queryResult={queryResult} allowStaleData={true}>
        {(result) => {
          const {scheduler, scheduleStatesOrError} = result;
          return (
            <>
              <SchedulerInfo schedulerOrError={scheduler} />
              <ScheduleStates scheduleStatesOrError={scheduleStatesOrError} />
            </>
          );
        }}
      </Loading>
    </ScrollContainer>
  );
};

const UnloadableScheduleInfo: React.FunctionComponent<{}> = () => {
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
            Show more info.
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

const ScheduleStates: React.FunctionComponent<{
  scheduleStatesOrError: SchedulerRootQuery_scheduleStatesOrError;
}> = ({scheduleStatesOrError}) => {
  const history = useHistory();
  const {options, error} = useRepositoryOptions();
  const [_, setRepo] = useCurrentRepositoryState(options);

  if (error) {
    return <PythonErrorInfo error={error} />;
  } else if (scheduleStatesOrError.__typename === 'PythonError') {
    return <PythonErrorInfo error={scheduleStatesOrError} />;
  } else if (scheduleStatesOrError.__typename === 'RepositoryNotFoundError') {
    // Can't reach this case because we didn't use a repository selector
    return null;
  }

  const {results: scheduleStates} = scheduleStatesOrError;

  // Build map of repositoryOriginId to DagsterRepoOption
  const repositoryOriginIdMap = {};
  for (const option of options) {
    repositoryOriginIdMap[option.repository.id] = option;
  }

  // Seperate out schedules into in-scope and out-of-scope
  const loadableSchedules = scheduleStates.filter(({repositoryOriginId}) =>
    repositoryOriginIdMap.hasOwnProperty(repositoryOriginId),
  );

  const unLoadableSchedules = scheduleStates.filter(
    ({repositoryOriginId}) => !repositoryOriginIdMap.hasOwnProperty(repositoryOriginId),
  );

  // Group loadable schedules by repository
  const loadableSchedulesByRepositoryOriginId = {};
  for (const loadableSchedule of loadableSchedules) {
    const {repositoryOriginId} = loadableSchedule;
    if (!loadableSchedulesByRepositoryOriginId.hasOwnProperty(repositoryOriginId)) {
      loadableSchedulesByRepositoryOriginId[repositoryOriginId] = [];
    }

    loadableSchedulesByRepositoryOriginId[repositoryOriginId].push(loadableSchedule);
  }

  const goToRepositorySchedules = (dagsterRepoOption: DagsterRepoOption) => {
    setRepo(dagsterRepoOption);
    history.push(`/schedules`);
  };

  return (
    <div>
      <div style={{display: 'flex'}}>
        <h2 style={{marginBottom: 0}}>All Schedules:</h2>
        <div style={{flex: 1}} />
        <SchedulerTimezoneNote />
      </div>
      <Divider />

      {Object.entries(loadableSchedulesByRepositoryOriginId).map((item, i) => {
        // @ts-ignore
        const [repositoryOriginId, repositoryloadableSchedules]: [
          string,
          SchedulerRootQuery_scheduleStatesOrError_ScheduleStates_results[],
        ] = item;

        const option = repositoryOriginIdMap[repositoryOriginId];
        const {repository} = option;

        return (
          <div style={{marginTop: 30}}>
            <div style={{display: 'flex', justifyContent: 'space-between', marginBottom: 20}}>
              <h3 style={{marginTop: 0, marginBottom: 0}}>Repository: {repository.name}</h3>
              <Button
                style={{marginLeft: 10}}
                onClick={() => goToRepositorySchedules(option)}
                small={true}
              >
                Go to repository schedules
              </Button>{' '}
            </div>
            {repositoryloadableSchedules.map((scheduleState) => (
              <ScheduleStateRow
                scheduleState={scheduleState}
                key={scheduleState.scheduleOriginId}
                showStatus={true}
                dagsterRepoOption={repositoryOriginIdMap[scheduleState.repositoryOriginId]}
              />
            ))}
          </div>
        );
      })}

      {unLoadableSchedules.length > 0 && (
        <>
          <h3 style={{marginTop: 20}}>Unloadable schedules:</h3>
          <UnloadableScheduleInfo />

          {unLoadableSchedules.map((scheduleState) => (
            <ScheduleStateRow
              scheduleState={scheduleState}
              key={scheduleState.scheduleOriginId}
              showStatus={true}
            />
          ))}
        </>
      )}
    </div>
  );
};

const SCHEDULER_ROOT_QUERY = gql`
  query SchedulerRootQuery {
    scheduler {
      ...SchedulerFragment
    }
    scheduleStatesOrError {
      ... on ScheduleStates {
        results {
          ...ScheduleStateFragment
        }
      }
      ...PythonErrorFragment
    }
  }

  ${SCHEDULER_FRAGMENT}
  ${SCHEDULE_STATE_FRAGMENT}
`;
