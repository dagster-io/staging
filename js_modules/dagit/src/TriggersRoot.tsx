import * as React from "react";
import gql from "graphql-tag";
import * as qs from "query-string";

import { Button, Menu, MenuItem, MenuDivider, Popover, Tooltip, Spinner } from "@blueprintjs/core";
import { Icon, NonIdealState } from "@blueprintjs/core";
import { Header, Legend, LegendColumn, ScrollContainer } from "./ListComponents";
import { RowColumn, RowContainer } from "./ListComponents";
import { useQuery } from "react-apollo";
import {
  TriggersRootQuery,
  TriggersRootQuery_triggerDefinitionsOrError_TriggerDefinitions_results
} from "./types/TriggersRootQuery";
import Loading from "./Loading";
import PythonErrorInfo from "./PythonErrorInfo";

import { titleForRun } from "./runs/RunUtils";
import { Link, RouteComponentProps } from "react-router-dom";
import { useRepositorySelector } from "./DagsterRepositoryContext";
import { IconNames } from "@blueprintjs/icons";
import { showCustomAlert } from "./CustomAlertProvider";
import { RunStatus } from "./runs/RunStatusDots";
import { HighlightedCodeBlock } from "./HighlightedCodeBlock";
import { RunTable } from "./runs/RunTable";
import { TriggerGraphs, TRIGGER_GRAPHS_RUN_FRAGMENT } from "./TriggerGraphs";

type Trigger = TriggersRootQuery_triggerDefinitionsOrError_TriggerDefinitions_results;

export const TriggersRoot: React.FunctionComponent<RouteComponentProps> = ({ match }) => {
  const triggerName = match.params?.["triggerName"];
  const repositorySelector = useRepositorySelector();

  const queryResult = useQuery<TriggersRootQuery>(TRIGGERS_ROOT_QUERY, {
    variables: {
      repositorySelector: repositorySelector
    },
    fetchPolicy: "cache-and-network",
    pollInterval: 5 * 1000,
    partialRefetch: true
  });

  return (
    <Loading queryResult={queryResult} allowStaleData={true}>
      {result => {
        const { triggerDefinitionsOrError } = result;

        if (triggerDefinitionsOrError.__typename === "PythonError") {
          return <PythonErrorInfo error={triggerDefinitionsOrError} />;
        } else if (triggerDefinitionsOrError.__typename !== "TriggerDefinitions") {
          return null;
        }

        const triggerDefinitions = triggerDefinitionsOrError.results;

        if (!triggerDefinitions.length) {
          return (
            <NonIdealState
              icon={IconNames.ERROR}
              title="No Triggered Execution Definitions Found"
              description={<p>This repository does not have any triggered executions defined.</p>}
            />
          );
        }

        const matching = triggerName
          ? triggerDefinitions.filter(x => x.name === triggerName)
          : triggerDefinitions;

        if (triggerName && !matching.length) {
          return (
            <NonIdealState
              icon={IconNames.ERROR}
              title={`No Matching Triggered Execution "${triggerName}"`}
              description={
                <p>
                  This repository does not have a triggered execution defined named {triggerName}.
                </p>
              }
            />
          );
        }

        const allStepKeys = {};
        if (triggerName) {
          matching[0].runs.forEach(run => {
            run.stepStats.forEach(stat => {
              allStepKeys[stat.stepKey] = true;
            });
          });
        }

        return (
          <>
            <ScrollContainer>
              <Header>{triggerName || "Triggers"}</Header>
              <TriggersTable triggers={matching} />
              <div style={{ marginTop: 30 }}>
                {triggerName && matching[0].runs ? (
                  <TriggerGraphs runs={matching[0].runs} stepKeys={Object.keys(allStepKeys)} />
                ) : null}
              </div>
            </ScrollContainer>
          </>
        );
      }}
    </Loading>
  );
};

const TriggersTable: React.FunctionComponent<{
  triggers: Trigger[];
}> = ({ triggers }) => {
  if (triggers.length === 0) {
    return null;
  }

  return (
    <div style={{ marginTop: 30 }}>
      <Legend>
        <LegendColumn style={{ flex: 1.4 }}>Trigger Name</LegendColumn>
        <LegendColumn>Pipeline</LegendColumn>
        <LegendColumn style={{ flex: 1 }}>Latest Runs</LegendColumn>
        <LegendColumn style={{ flex: 1 }}>Execution Params</LegendColumn>
      </Legend>
      {triggers.map(trigger => (
        <TriggerRow trigger={trigger} key={trigger.name} />
      ))}
    </div>
  );
};

const TriggerRow: React.FunctionComponent<{
  trigger: Trigger;
}> = ({ trigger }) => {
  const repositorySelector = useRepositorySelector();
  const [configRequested, setConfigRequested] = React.useState(false);
  const { data, loading } = useQuery(FETCH_TRIGGER_CONFIG, {
    variables: { repositorySelector, triggerName: trigger.name },
    skip: !configRequested
  });

  const runConfigError =
    data?.triggerDefinitionOrError?.runConfigOrError.__typename === "PythonError"
      ? data.triggerDefinitionOrError.runConfigOrError
      : null;

  const runConfigYaml = runConfigError
    ? null
    : data?.triggerDefinitionOrError?.runConfigOrError.yaml;

  return (
    <RowContainer key={trigger.name}>
      <RowColumn style={{ flex: 1.4 }}>
        <Link to={`/triggers/${trigger.name}`}>{trigger.name}</Link>
      </RowColumn>
      <RowColumn>
        <Link to={`/pipeline/${trigger.pipelineName}/`}>
          <Icon icon="diagram-tree" /> {trigger.pipelineName}
        </Link>
      </RowColumn>
      <RowColumn
        style={{
          flex: 1,
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between"
        }}
      >
        <div>
          {trigger.runs.map(run => {
            const runLabel = titleForRun(run);
            return (
              <div
                style={{
                  display: "inline-block",
                  cursor: "pointer",
                  marginRight: 5
                }}
                key={run.runId}
              >
                <Link to={`/pipeline/${run.pipelineName}/runs/${run.runId}`}>
                  <Tooltip
                    position={"top"}
                    content={runLabel}
                    wrapperTagName="div"
                    targetTagName="div"
                  >
                    <RunStatus status={run.status} />
                  </Tooltip>
                </Link>
              </div>
            );
          })}
        </div>
      </RowColumn>
      <RowColumn
        style={{
          display: "flex",
          alignItems: "flex-start",
          flex: 1
        }}
      >
        <div style={{ flex: 1 }}>
          <div>{`Mode: ${trigger.mode}`}</div>
        </div>
        <Popover
          position={"bottom"}
          content={
            loading ? (
              <Spinner size={32} />
            ) : (
              <Menu>
                <MenuItem
                  text="View Configuration..."
                  icon="share"
                  onClick={() => {
                    if (runConfigError) {
                      showCustomAlert({
                        body: <PythonErrorInfo error={runConfigError} />
                      });
                    } else {
                      showCustomAlert({
                        title: "Config",
                        body: (
                          <HighlightedCodeBlock
                            value={runConfigYaml || "Unable to resolve config"}
                            languages={["yaml"]}
                          />
                        )
                      });
                    }
                  }}
                />
                <MenuItem
                  text="Open in Playground..."
                  icon="edit"
                  target="_blank"
                  disabled={!runConfigYaml}
                  href={`/pipeline/${trigger.pipelineName}/playground/setup?${qs.stringify({
                    mode: trigger.mode,
                    solidSelection: trigger.solidSelection,
                    config: runConfigYaml
                  })}`}
                />
                <MenuDivider />
              </Menu>
            )
          }
        >
          <Button
            minimal={true}
            icon="chevron-down"
            onClick={() => {
              setConfigRequested(true);
            }}
          />
        </Popover>
      </RowColumn>
    </RowContainer>
  );
};

const TRIGGERS_ROOT_QUERY = gql`
  query TriggersRootQuery($repositorySelector: RepositorySelector!) {
    triggerDefinitionsOrError(repositorySelector: $repositorySelector) {
      __typename
      ... on TriggerDefinitions {
        results {
          name
          pipelineName
          solidSelection
          mode
          runs {
            runId
            tags {
              key
              value
            }
            pipelineName
            status
            stepStats {
              stepKey
            }
            ...RunTableRunFragment
            ...TriggerGraphsRunFragment
          }
        }
      }
      ... on PythonError {
        ...PythonErrorFragment
      }
    }
  }
  ${PythonErrorInfo.fragments.PythonErrorFragment}
  ${RunTable.fragments.RunTableRunFragment}
  ${TRIGGER_GRAPHS_RUN_FRAGMENT}
`;

const FETCH_TRIGGER_CONFIG = gql`
  query FetchTriggerConfig($repositorySelector: RepositorySelector!, $triggerName: String!) {
    triggerDefinitionOrError(repositorySelector: $repositorySelector, triggerName: $triggerName) {
      ... on TriggerDefinition {
        runConfigOrError {
          ... on RunConfig {
            yaml
          }
          ... on PythonError {
            ...PythonErrorFragment
          }
        }
      }
      ... on PythonError {
        ...PythonErrorFragment
      }
    }
  }
  ${PythonErrorInfo.fragments.PythonErrorFragment}
`;
