import * as React from "react";
import { Switch, Code, Callout, Intent } from "@blueprintjs/core";
import {
  Header,
  ScrollContainer,
  Legend,
  LegendColumn,
  RowColumn,
  RowContainer
} from "../ListComponents";
import { useQuery } from "react-apollo";
import Loading from "../Loading";
import gql from "graphql-tag";
import PythonErrorInfo from "../PythonErrorInfo";
import {
  SchedulerRootQuery,
  SchedulerRootQuery_scheduleStatesOrError,
  SchedulerRootQuery_scheduler
} from "./types/SchedulerRootQuery";
import { ScheduleStatus } from "../types/globalTypes";
import { SchedulerNotConfigured } from "./ScheduleNotConfigured";

export const SchedulerRoot: React.FunctionComponent<{}> = () => {
  const queryResult = useQuery<SchedulerRootQuery>(SCHEDULER_ROOT_QUERY, {
    variables: {},
    fetchPolicy: "cache-and-network"
    // pollInterval: 15 * 1000,
    // partialRefetch: true
  });

  return (
    <Loading queryResult={queryResult} allowStaleData={true}>
      {result => {
        const { scheduler, scheduleStatesOrError } = result;
        return (
          <ScrollContainer>
            <SchedulerSection scheduler={scheduler} />
            <ScheduleStateSection states={scheduleStatesOrError} />
          </ScrollContainer>
        );
      }}
    </Loading>
  );
};

const SchedulerSection: React.FunctionComponent<{ scheduler: SchedulerRootQuery_scheduler }> = ({
  scheduler
}) => {
  let content = null;
  if (scheduler.__typename === "PythonError") {
    content = <PythonErrorInfo error={scheduler} />;
  } else if (scheduler.__typename === "SchedulerNotDefinedError") {
    content = <SchedulerNotConfigured />;
  } else {
    content = (
      <Callout icon="calendar" style={{ marginBottom: 40 }}>
        <span style={{ fontWeight: "bold" }}>Scheduler:</span>{" "}
        <pre style={{ display: "inline" }}>{scheduler.schedulerClass}</pre>
      </Callout>
    );
  }

  return (
    <div>
      <Header>Scheduler</Header>
      {content}
    </div>
  );
};

const ScheduleStateSection: React.FunctionComponent<{
  states: SchedulerRootQuery_scheduleStatesOrError;
}> = ({ states }) => {
  let content = null;
  if (states.__typename === "PythonError") {
    content = <PythonErrorInfo error={states} />;
  } else if (states.__typename === "ScheduleStates") {
    content = (
      <>
        <Legend>
          <LegendColumn style={{ maxWidth: 60, paddingRight: 2 }}></LegendColumn>
          <LegendColumn>Schedule Name</LegendColumn>
          <LegendColumn>Repository Origin</LegendColumn>
        </Legend>
        {states.results.map(scheduleState => (
          <RowContainer key={scheduleState.id}>
            <RowColumn style={{ maxWidth: 60, paddingLeft: 0, textAlign: "center" }}>
              <Switch
                checked={scheduleState.status === ScheduleStatus.RUNNING}
                large={true}
                disabled={true}
                innerLabelChecked="on"
                innerLabel="off"
              />
            </RowColumn>
            <RowColumn>
              <div>{scheduleState.scheduleName}</div>
            </RowColumn>
            <RowColumn>
              <pre>
                python: <Code>{scheduleState.repositoryOrigin.executablePath}</Code>
                {"\n"}
                code: <Code>{scheduleState.repositoryOrigin.codePointerDescription}</Code>
                {"\n"}
                id: <Code>{scheduleState.repositoryOriginId}</Code>
              </pre>
            </RowColumn>
          </RowContainer>
        ))}
      </>
    );
  }

  return (
    <>
      <Header>Schedule States</Header>
      {content}
    </>
  );
};

export const SCHEDULER_ROOT_QUERY = gql`
  query SchedulerRootQuery {
    scheduler {
      __typename
      ... on SchedulerNotDefinedError {
        message
      }
      ... on Scheduler {
        schedulerClass
      }
      ...PythonErrorFragment
    }
    scheduleStatesOrError {
      __typename
      ... on ScheduleStates {
        results {
          id
          scheduleName
          cronSchedule
          status
          repositoryOriginId
          repositoryOrigin {
            executablePath
            codePointerDescription
          }
        }
      }
      ...PythonErrorFragment
    }
  }

  ${PythonErrorInfo.fragments.PythonErrorFragment}
`;
