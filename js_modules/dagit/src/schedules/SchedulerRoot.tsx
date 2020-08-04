import * as React from "react";
import { ScrollContainer, Header } from "../ListComponents";
import { SchedulerInfo, SCHEDULER_FRAGMENT } from "./SchedulerInfo";
import { useQuery } from "react-apollo";
import Loading from "../Loading";
import gql from "graphql-tag";
import {
  SchedulerRootQuery,
  SchedulerRootQuery_scheduleStatesOrError
} from "./types/SchedulerRootQuery";
import { ScheduleStateRow } from "./ScheduleRow";
import PythonErrorInfo from "../PythonErrorInfo";
import { SCHEDULE_STATE_FRAGMENT } from "./ScheduleUtils";
import { useRepositoryOptions } from "../DagsterRepositoryContext";

export const SchedulerRoot: React.FunctionComponent<{}> = () => {
  const queryResult = useQuery<SchedulerRootQuery>(SCHEDULER_ROOT_QUERY, {
    variables: {},
    fetchPolicy: "cache-and-network"
  });

  return (
    <ScrollContainer>
      <Header>Scheduler</Header>
      <Loading queryResult={queryResult} allowStaleData={true}>
        {result => {
          const { scheduler, scheduleStatesOrError } = result;
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

const ScheduleStates: React.FunctionComponent<{
  scheduleStatesOrError: SchedulerRootQuery_scheduleStatesOrError;
}> = ({ scheduleStatesOrError }) => {
  const { options, error } = useRepositoryOptions();

  if (error) {
    return <PythonErrorInfo error={error} />;
  } else if (scheduleStatesOrError.__typename === "PythonError") {
    return <PythonErrorInfo error={scheduleStatesOrError} />;
  } else if (scheduleStatesOrError.__typename === "RepositoryNotFoundError") {
    // Can't reach this case because we didn't use a repository selector
    return null;
  }

  const { results: scheduleStates } = scheduleStatesOrError;

  // Build map of repositoryOriginId to DagsterRepoOption
  const repositoryOriginIdMap = {};
  for (const option of options) {
    repositoryOriginIdMap[option.repository.id] = option;
  }

  // Seperate out schedules into in-scope and out-of-scope
  const inContextSchedules = scheduleStates.filter(({ repositoryOriginId }) =>
    repositoryOriginIdMap.hasOwnProperty(repositoryOriginId)
  );

  const outOfContextSchedules = scheduleStates.filter(
    ({ repositoryOriginId }) => !repositoryOriginIdMap.hasOwnProperty(repositoryOriginId)
  );

  return (
    <div>
      <h2>All Schedules:</h2>
      <Divider />

      <h3>In context schedules:</h3>
      {inContextSchedules.map(scheduleState => (
        <ScheduleStateRow
          scheduleState={scheduleState}
          key={scheduleState.scheduleOriginId}
          showStatus={true}
          dagsterRepoOption={repositoryOriginIdMap[scheduleState.repositoryOriginId]}
        />
      ))}

      <h3>Out of context schedules:</h3>
      {outOfContextSchedules.map(scheduleState => (
        <ScheduleStateRow
          scheduleState={scheduleState}
          key={scheduleState.scheduleOriginId}
          showStatus={true}
        />
      ))}
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
