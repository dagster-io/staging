import {NonIdealState} from '@blueprintjs/core';
import {IconNames} from '@blueprintjs/icons';
import ApolloClient from 'apollo-client';
import gql from 'graphql-tag';
import * as React from 'react';
import {useMutation} from 'react-apollo';
import styled from 'styled-components/macro';

import {showCustomAlert} from '../CustomAlertProvider';
import {DagsterRepositoryContext} from '../DagsterRepositoryContext';
import PythonErrorInfo from '../PythonErrorInfo';
import {IStepState, RunMetadataProvider} from '../RunMetadataProvider';
import {FirstOrSecondPanelToggle, SplitPanelContainer} from '../SplitPanelContainer';
import {GaantChart, GaantChartMode} from '../gaant/GaantChart';

import {GetDefaultLogFilter, LogFilter, LogsProvider} from './LogsProvider';
import LogsScrollingTable from './LogsScrollingTable';
import LogsToolbar from './LogsToolbar';
import {RunActionButtons} from './RunActionButtons';
import {RunContext} from './RunContext';
import {RunStatusToPageAttributes} from './RunStatusToPageAttributes';
import {
  LAUNCH_PIPELINE_REEXECUTION_MUTATION,
  getReexecutionVariables,
  handleLaunchResult,
  ReExecutionStyle,
} from './RunUtils';
import {
  LaunchPipelineReexecution,
  LaunchPipelineReexecutionVariables,
} from './types/LaunchPipelineReexecution';
import {RunFragment} from './types/RunFragment';
import {
  RunPipelineRunEventFragment,
  RunPipelineRunEventFragment_ExecutionStepFailureEvent,
} from './types/RunPipelineRunEventFragment';

interface RunProps {
  client: ApolloClient<any>;
  runId: string;
  run?: RunFragment;
}

interface RunState {
  logsFilter: LogFilter;
  selection: StepSelection;
}

export interface StepSelection {
  keys: string[];
  query: string;
}

export class Run extends React.Component<RunProps, RunState> {
  static fragments = {
    RunFragment: gql`
      fragment RunFragment on PipelineRun {
        ...RunStatusPipelineRunFragment

        runConfigYaml
        runId
        canTerminate
        status
        mode
        tags {
          key
          value
        }
        rootRunId
        parentRunId
        pipeline {
          __typename
          ... on PipelineReference {
            name
            solidSelection
          }
        }
        pipelineSnapshotId
        executionPlan {
          steps {
            key
            inputs {
              dependsOn {
                key
                outputs {
                  name
                  type {
                    name
                  }
                }
              }
            }
          }
          artifactsPersisted
          ...GaantChartExecutionPlanFragment
        }
        stepKeysToExecute
      }

      ${RunStatusToPageAttributes.fragments.RunStatusPipelineRunFragment}
      ${GaantChart.fragments.GaantChartExecutionPlanFragment}
    `,
    RunPipelineRunEventFragment: gql`
      fragment RunPipelineRunEventFragment on PipelineRunEvent {
        ... on MessageEvent {
          message
          timestamp
          level
          stepKey
        }

        ...LogsScrollingTableMessageFragment
        ...RunMetadataProviderMessageFragment
      }

      ${RunMetadataProvider.fragments.RunMetadataProviderMessageFragment}
      ${LogsScrollingTable.fragments.LogsScrollingTableMessageFragment}
      ${PythonErrorInfo.fragments.PythonErrorFragment}
    `,
  };

  state: RunState = {
    logsFilter: GetDefaultLogFilter(),
    selection: {
      query: '*',
      keys: [],
    },
  };

  onShowStateDetails = (stepKey: string, logs: RunPipelineRunEventFragment[]) => {
    const errorNode = logs.find(
      (node) => node.__typename === 'ExecutionStepFailureEvent' && node.stepKey === stepKey,
    ) as RunPipelineRunEventFragment_ExecutionStepFailureEvent;

    if (errorNode) {
      showCustomAlert({
        body: <PythonErrorInfo error={errorNode} />,
      });
    }
  };

  onSetLogsFilter = (logsFilter: LogFilter) => {
    this.setState({logsFilter});
  };

  onSetSelection = (selection: StepSelection) => {
    this.setState({selection});

    // filter the log following the DSL step selection
    this.setState((prevState) => {
      return {
        logsFilter: {
          ...prevState.logsFilter,
          values: selection.query !== '*' ? [{token: 'query', value: selection.query}] : [],
        },
      };
    });
  };

  render() {
    const {client, run, runId} = this.props;
    const {logsFilter, selection} = this.state;

    return (
      <RunContext.Provider value={run}>
        {run && <RunStatusToPageAttributes run={run} />}

        <LogsProvider
          key={runId}
          client={client}
          runId={runId}
          filter={logsFilter}
          selectedSteps={selection.keys}
        >
          {({filteredNodes, allNodes, loaded}) => (
            <RunWithData
              run={run}
              runId={runId}
              filteredNodes={filteredNodes}
              allNodes={allNodes}
              logsLoading={!loaded}
              logsFilter={logsFilter}
              selection={selection}
              onSetLogsFilter={this.onSetLogsFilter}
              onSetSelection={this.onSetSelection}
              onShowStateDetails={this.onShowStateDetails}
            />
          )}
        </LogsProvider>
      </RunContext.Provider>
    );
  }
}

interface RunWithDataProps {
  run?: RunFragment;
  runId: string;
  selection: StepSelection;
  allNodes: (RunPipelineRunEventFragment & {clientsideKey: string})[];
  filteredNodes: (RunPipelineRunEventFragment & {clientsideKey: string})[];
  logsFilter: LogFilter;
  logsLoading: boolean;
  onSetLogsFilter: (v: LogFilter) => void;
  onSetSelection: (v: StepSelection) => void;
  onShowStateDetails: (stepKey: string, logs: RunPipelineRunEventFragment[]) => void;
}

const RunWithData: React.FunctionComponent<RunWithDataProps> = ({
  run,
  runId,
  allNodes,
  filteredNodes,
  logsFilter,
  logsLoading,
  selection,
  onSetLogsFilter,
  onSetSelection,
}) => {
  const [launchPipelineReexecution] = useMutation<
    LaunchPipelineReexecution,
    LaunchPipelineReexecutionVariables
  >(LAUNCH_PIPELINE_REEXECUTION_MUTATION);
  const {repositoryLocation, repository} = React.useContext(DagsterRepositoryContext);
  const splitPanelContainer = React.createRef<SplitPanelContainer>();

  const onLaunch = async (style: ReExecutionStyle) => {
    if (!run || run.pipeline.__typename === 'UnknownPipeline') return;

    const variables = getReexecutionVariables({
      run,
      style,
      repositoryLocationName: repositoryLocation?.name,
      repositoryName: repository?.name,
    });
    const result = await launchPipelineReexecution({variables});
    handleLaunchResult(run.pipeline.name, result, {openInNewWindow: false});
  };

  const onClickStep = (stepKey: string, evt: React.MouseEvent<any>) => {
    const index = selection.keys.indexOf(stepKey);
    let newSelected: string[];

    if (evt.shiftKey) {
      // shift-click to multi select steps
      newSelected = [...selection.keys];

      if (index !== -1) {
        // deselect the step if already selected
        newSelected.splice(index, 1);
      } else {
        // select the step otherwise
        newSelected.push(stepKey);
      }
    } else {
      if (selection.keys.length === 1 && index !== -1) {
        // deselect the step if already selected
        newSelected = [];
      } else {
        // select the step otherwise
        newSelected = [stepKey];
      }
    }

    onSetSelection({
      query: newSelected.join(', ') || '*',
      keys: newSelected,
    });
  };

  return (
    <RunMetadataProvider logs={allNodes}>
      {(metadata) => (
        <SplitPanelContainer
          ref={splitPanelContainer}
          axis={'vertical'}
          identifier="run-gaant"
          firstInitialPercent={35}
          firstMinSize={40}
          first={
            logsLoading ? (
              <GaantChart.LoadingState runId={runId} />
            ) : run?.executionPlan ? (
              <GaantChart
                options={{
                  mode: GaantChartMode.WATERFALL_TIMED,
                }}
                toolbarLeftActions={
                  <FirstOrSecondPanelToggle axis={'vertical'} container={splitPanelContainer} />
                }
                toolbarActions={
                  <RunActionButtons
                    run={run}
                    executionPlan={run.executionPlan}
                    artifactsPersisted={run.executionPlan.artifactsPersisted}
                    onLaunch={onLaunch}
                    selection={selection}
                    selectionStates={selection.keys.map(
                      (key) => (key && metadata.steps[key]?.state) || IStepState.PREPARING,
                    )}
                  />
                }
                runId={runId}
                plan={run.executionPlan}
                metadata={metadata}
                selection={selection}
                onClickStep={onClickStep}
                onSetSelection={onSetSelection}
              />
            ) : (
              <NonIdealState icon={IconNames.ERROR} title="Unable to build execution plan" />
            )
          }
          second={
            <LogsContainer>
              <LogsToolbar
                filter={logsFilter}
                onSetFilter={onSetLogsFilter}
                steps={Object.keys(metadata.steps)}
                metadata={metadata}
              />
              <LogsScrollingTable
                nodes={filteredNodes}
                loading={logsLoading}
                filterKey={JSON.stringify(logsFilter)}
                metadata={metadata}
              />
            </LogsContainer>
          }
        />
      )}
    </RunMetadataProvider>
  );
};

const LogsContainer = styled.div`
  display: flex;
  flex-direction: column;
  height: 100%;
  background: #f1f6f9;
`;
