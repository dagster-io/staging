import * as React from "react";

import { Header, ScrollContainer } from "../ListComponents";
import { useQuery } from "react-apollo";
import Loading from "../Loading";
import gql from "graphql-tag";
import { RouteComponentProps } from "react-router";
import { ScheduleRootQuery } from "./types/ScheduleRootQuery";
import { SensorRow, ScheduleRow, SensorRowHeader, ScheduleRowHeader } from "./ScheduleRow";

import { __RouterContext as RouterContext } from "react-router";
import * as querystring from "query-string";
import { PartitionView } from "../partitions/PartitionView";
import { useScheduleSelector } from "../DagsterRepositoryContext";
import { SCHEDULE_DEFINITION_FRAGMENT, SchedulerTimezoneNote } from "./ScheduleUtils";

export const ScheduleRoot: React.FunctionComponent<RouteComponentProps<{
  scheduleName?: string;
  sensorName?: string;
}>> = ({ match, location }) => {
  const isSensor = !match.path.startsWith("/schedules/");
  const name = isSensor ? match.params.sensorName : match.params.scheduleName;
  const scheduleSelector = useScheduleSelector(name || "");
  const { history } = React.useContext(RouterContext);
  const qs = querystring.parse(location.search);
  const cursor = (qs.cursor as string) || undefined;
  const setCursor = (cursor: string | undefined) => {
    history.push({ search: `?${querystring.stringify({ ...qs, cursor })}` });
  };
  const queryResult = useQuery<ScheduleRootQuery>(SCHEDULE_ROOT_QUERY, {
    variables: {
      scheduleSelector
    },
    fetchPolicy: "cache-and-network",
    pollInterval: 15 * 1000,
    partialRefetch: true
  });

  return (
    <Loading queryResult={queryResult} allowStaleData={true}>
      {result => {
        const { scheduleDefinitionOrError } = result;

        if (scheduleDefinitionOrError.__typename === "ScheduleDefinition") {
          const partitionSetName = scheduleDefinitionOrError.partitionSet?.name;
          if (isSensor) {
            return (
              <ScrollContainer>
                <Header>
                  Sensor <code>{name}</code>
                </Header>
                <SensorRowHeader />
                <SensorRow sensor={scheduleDefinitionOrError} />
                {partitionSetName ? (
                  <PartitionView
                    pipelineName={scheduleDefinitionOrError.pipelineName}
                    partitionSetName={partitionSetName}
                    cursor={cursor}
                    setCursor={setCursor}
                  />
                ) : null}
              </ScrollContainer>
            );
          } else {
            return (
              <ScrollContainer>
                <div style={{ display: "flex" }}>
                  <Header>
                    Schedule <code>{name}</code>
                  </Header>
                  <div style={{ flex: 1 }} />
                  <SchedulerTimezoneNote />
                </div>
                <ScheduleRowHeader schedule={scheduleDefinitionOrError} />
                <ScheduleRow schedule={scheduleDefinitionOrError} />
                {partitionSetName ? (
                  <PartitionView
                    pipelineName={scheduleDefinitionOrError.pipelineName}
                    partitionSetName={partitionSetName}
                    cursor={cursor}
                    setCursor={setCursor}
                  />
                ) : null}
              </ScrollContainer>
            );
          }
        } else {
          return null;
        }
      }}
    </Loading>
  );
};

export const SCHEDULE_ROOT_QUERY = gql`
  query ScheduleRootQuery($scheduleSelector: ScheduleSelector!) {
    scheduleDefinitionOrError(scheduleSelector: $scheduleSelector) {
      ... on ScheduleDefinition {
        ...ScheduleDefinitionFragment
      }
      ... on ScheduleDefinitionNotFoundError {
        message
      }
      ... on PythonError {
        message
        stack
      }
    }
  }

  ${SCHEDULE_DEFINITION_FRAGMENT}
`;
