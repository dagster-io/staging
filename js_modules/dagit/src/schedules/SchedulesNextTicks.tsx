import {gql, useLazyQuery} from '@apollo/client';
import {
  Classes,
  NonIdealState,
  Colors,
  Button,
  Menu,
  MenuItem,
  Popover,
  Spinner,
  Dialog,
  Icon,
} from '@blueprintjs/core';
import {IconNames} from '@blueprintjs/icons';
import * as qs from 'query-string';
import * as React from 'react';
import {Link} from 'react-router-dom';
import styled from 'styled-components/macro';

import {showCustomAlert} from 'src/app/CustomAlertProvider';
import {copyValue} from 'src/app/DomUtils';
import {PythonErrorInfo, PYTHON_ERROR_FRAGMENT} from 'src/app/PythonErrorInfo';
import {RunTags} from 'src/runs/RunTags';
import {TimestampDisplay} from 'src/schedules/TimestampDisplay';
import {RepositorySchedulesFragment} from 'src/schedules/types/RepositorySchedulesFragment';
import {ScheduleFragment} from 'src/schedules/types/ScheduleFragment';
import {
  ScheduleTickConfigQuery,
  ScheduleTickConfigQuery_scheduleOrError_Schedule_futureTick_evaluationResult_runRequests,
} from 'src/schedules/types/ScheduleTickConfigQuery';
import {JobStatus} from 'src/types/globalTypes';
import {Box} from 'src/ui/Box';
import {ButtonLink} from 'src/ui/ButtonLink';
import {Group} from 'src/ui/Group';
import {HighlightedCodeBlock} from 'src/ui/HighlightedCodeBlock';
import {Table} from 'src/ui/Table';
import {FontFamily} from 'src/ui/styles';
import {repoAddressToSelector} from 'src/workspace/repoAddressToSelector';
import {RepoAddress} from 'src/workspace/types';
import {workspacePathFromAddress} from 'src/workspace/workspacePath';

interface ScheduleTick {
  schedule: ScheduleFragment;
  timestamp: number;
  repoAddress: RepoAddress;
}

export const SchedulesNextTicks: React.FC<{
  repos: RepositorySchedulesFragment[];
}> = React.memo(({repos}) => {
  const nextTicks: ScheduleTick[] = [];

  repos.forEach((repo) => {
    const {schedules} = repo;
    const repoAddress = {
      name: repo.name,
      location: repo.location.name,
    };

    const futureTickSchedules = schedules.filter(
      (schedule) =>
        schedule.futureTicks.results.length && schedule.scheduleState.status === JobStatus.RUNNING,
    );

    const minMaxTimestamp = Math.min(
      ...futureTickSchedules.map(
        (schedule) =>
          schedule.futureTicks.results[schedule.futureTicks.results.length - 1].timestamp,
      ),
    );

    futureTickSchedules.forEach((schedule) => {
      schedule.futureTicks.results.forEach((tick) => {
        if (tick.timestamp <= minMaxTimestamp) {
          nextTicks.push({schedule, timestamp: tick.timestamp, repoAddress});
        }
      });
    });
  });

  nextTicks.sort((a, b) => a.timestamp - b.timestamp);

  if (!nextTicks.length) {
    return (
      <Box margin={{top: 32}}>
        <NonIdealState
          title="No scheduled ticks"
          description="There are no running schedules. Start a schedule to see scheduled ticks."
        />
      </Box>
    );
  }

  return (
    <Table>
      <thead>
        <tr>
          <th style={{width: '200px'}}>Timestamp</th>
          <th style={{width: '30%'}}>Schedule</th>
          <th>Pipeline</th>
          <th>Execution Params</th>
        </tr>
      </thead>
      <tbody>
        {nextTicks.map(({schedule, timestamp, repoAddress}) => (
          <tr key={`${schedule.id}:${timestamp}`}>
            <td>
              <TimestampDisplay
                timestamp={timestamp}
                timezone={schedule.executionTimezone}
                format="MMM D, h:mm A z"
              />
            </td>
            <td>
              <Link to={workspacePathFromAddress(repoAddress, `/schedules/${schedule.name}`)}>
                {schedule.name}
              </Link>
            </td>
            <td>
              <Link
                to={workspacePathFromAddress(repoAddress, `/pipelines/${schedule.pipelineName}/`)}
              >
                {schedule.pipelineName}
              </Link>
            </td>
            <td>
              <NextTickMenu
                repoAddress={repoAddress}
                schedule={schedule}
                tickTimestamp={timestamp}
              />
            </td>
          </tr>
        ))}
      </tbody>
    </Table>
  );
});

const NextTickMenu: React.FC<{
  repoAddress: RepoAddress;
  schedule: ScheduleFragment;
  tickTimestamp: number;
}> = React.memo(({repoAddress, schedule, tickTimestamp}) => {
  const scheduleSelector = {
    ...repoAddressToSelector(repoAddress),
    scheduleName: schedule.name,
  };
  const [runRequestsOpen, setRunRequestsOpen] = React.useState<boolean>(false);
  const [selectedRunRequest, setSelectedRunRequest] = React.useState<
    | ScheduleTickConfigQuery_scheduleOrError_Schedule_futureTick_evaluationResult_runRequests
    | undefined
  >();
  const configRef = React.useRef<HTMLDivElement>(null);
  const [loadTickConfig, {called, loading, data}] = useLazyQuery<ScheduleTickConfigQuery>(
    SCHEDULE_TICK_CONFIG_QUERY,
    {
      variables: {
        scheduleSelector,
        tickTimestamp,
      },
    },
  );

  const infoReady = called ? !loading : false;

  const evaluationResult =
    data?.scheduleOrError?.__typename === 'Schedule'
      ? data.scheduleOrError.futureTick.evaluationResult
      : null;

  const runRequests = evaluationResult ? evaluationResult.runRequests : null;
  const skipReason = evaluationResult ? evaluationResult.skipReason : null;
  const error = evaluationResult ? evaluationResult.error : null;

  let menuContent;
  if (!infoReady) {
    menuContent = <Spinner size={16} />;
  } else if (!evaluationResult) {
    menuContent = <MenuItem text="Could not preview tick for this schedule" />;
  } else if (skipReason) {
    menuContent = (
      <MenuItem
        text={`View skip reason...`}
        onClick={() =>
          showCustomAlert({
            title: 'Tick Skipped',
            body: <div>{skipReason}</div>,
          })
        }
      />
    );
  } else if (error) {
    menuContent = (
      <MenuItem
        text="View error..."
        onClick={() =>
          showCustomAlert({
            body: <PythonErrorInfo error={error} />,
          })
        }
      />
    );
  } else if (!runRequests || !runRequests.length) {
    menuContent = <MenuItem text="No runs requested for this projected schedule tick" />;
  } else if (runRequests.length == 1) {
    const runRequest = runRequests[0];
    const runConfigYaml = runRequest ? runRequest.runConfigYaml : '';
    menuContent = (
      <>
        <MenuItem
          text={loading ? 'Loading Configuration...' : 'View Configuration...'}
          icon="share"
          onClick={() => setSelectedRunRequest(runRequest || undefined)}
        />
        <MenuItem
          text="Open in Playground..."
          icon="edit"
          target="_blank"
          href={`/workspace/pipelines/${schedule.pipelineName}/playground/setup?${qs.stringify({
            mode: schedule.mode,
            config: runConfigYaml,
            solidSelection: schedule.solidSelection,
          })}`}
        />
      </>
    );
  } else {
    menuContent = (
      <MenuItem
        text={`View ${runRequests.length} run requests...`}
        icon="edit"
        target="_blank"
        onClick={() => setRunRequestsOpen(true)}
      />
    );
  }

  return (
    <Group direction="row" spacing={2} alignItems="center">
      <div>{`Mode: ${schedule.mode}`}</div>
      <Popover
        content={<Menu>{menuContent}</Menu>}
        position="bottom"
        onOpening={() => {
          if (!called) {
            loadTickConfig();
          }
        }}
      >
        <Button small minimal icon="chevron-down" style={{marginLeft: '4px'}} />
      </Popover>
      <Dialog
        usePortal={true}
        onClose={() => setSelectedRunRequest(undefined)}
        style={{width: '50vw'}}
        title={
          <Box flex={{direction: 'row'}}>
            <TimestampDisplay
              timestamp={tickTimestamp}
              timezone={schedule.executionTimezone}
              format="MMM D, h:mm A z"
            />
            {selectedRunRequest?.runKey ? <div>: {selectedRunRequest?.runKey}</div> : null}
          </Box>
        }
        isOpen={!!selectedRunRequest}
      >
        {selectedRunRequest && selectedRunRequest.tags.length ? (
          <Box padding={12}>
            <RunTags tags={selectedRunRequest.tags} />
          </Box>
        ) : null}
        <ConfigBody>
          <div ref={configRef}>
            {selectedRunRequest ? (
              <HighlightedCodeBlock value={selectedRunRequest.runConfigYaml} language="yaml" />
            ) : null}
          </div>
        </ConfigBody>
        <div className={Classes.DIALOG_FOOTER}>
          <div className={Classes.DIALOG_FOOTER_ACTIONS}>
            <Button
              autoFocus={false}
              onClick={(e: React.MouseEvent<any, MouseEvent>) => {
                copyValue(
                  e,
                  configRef && configRef.current ? configRef.current.innerText : '' || '',
                );
              }}
            >
              Copy
            </Button>
            <Button
              intent="primary"
              autoFocus={true}
              onClick={() => setSelectedRunRequest(undefined)}
            >
              OK
            </Button>
          </div>
        </div>
      </Dialog>
      <Dialog
        isOpen={runRequestsOpen}
        onClose={() => setRunRequestsOpen(false)}
        style={{width: '50vw'}}
        title={
          <TimestampDisplay
            timestamp={tickTimestamp}
            timezone={schedule.executionTimezone}
            format="MMM D, h:mm A z"
          />
        }
      >
        <RunRequestBody>
          <Table striped style={{width: '100%'}}>
            <thead>
              <tr>
                <th>Run key</th>
                <th>Config</th>
                <th>Actions</th>
              </tr>
            </thead>
            <tbody>
              {runRequests?.map((runRequest, idx) => {
                if (!runRequest) {
                  return null;
                }
                return (
                  <tr key={idx}>
                    <td>{runRequest.runKey || <span>&mdash;</span>}</td>
                    <td>
                      <ButtonLink
                        onClick={() => setSelectedRunRequest(runRequest)}
                        underline={false}
                      >
                        <Group direction="row" spacing={8} alignItems="center">
                          <Icon icon={IconNames.SHARE} iconSize={12} />
                          <span>View config</span>
                        </Group>
                      </ButtonLink>
                    </td>
                    <td>
                      <Popover
                        content={
                          <Menu>
                            <MenuItem
                              text="Open in Playground..."
                              icon="edit"
                              target="_blank"
                              href={`/workspace/pipelines/${
                                schedule.pipelineName
                              }/playground/setup?${qs.stringify({
                                mode: schedule.mode,
                                config: runRequest.runConfigYaml,
                                solidSelection: schedule.solidSelection,
                              })}`}
                            />
                          </Menu>
                        }
                        position="bottom"
                      >
                        <Button small minimal icon="chevron-down" />
                      </Popover>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </Table>
        </RunRequestBody>
        <div className={Classes.DIALOG_FOOTER}>
          <div className={Classes.DIALOG_FOOTER_ACTIONS}>
            <Button intent="primary" autoFocus={true} onClick={() => setRunRequestsOpen(false)}>
              OK
            </Button>
          </div>
        </div>
      </Dialog>
    </Group>
  );
});

const SCHEDULE_TICK_CONFIG_QUERY = gql`
  query ScheduleTickConfigQuery($scheduleSelector: ScheduleSelector!, $tickTimestamp: Int!) {
    scheduleOrError(scheduleSelector: $scheduleSelector) {
      ... on Schedule {
        id
        futureTick(tickTimestamp: $tickTimestamp) {
          evaluationResult {
            runRequests {
              runKey
              runConfigYaml
              tags {
                key
                value
              }
            }
            skipReason
            error {
              ...PythonErrorFragment
            }
          }
        }
      }
    }
  }
  ${PYTHON_ERROR_FRAGMENT}
`;

const ConfigBody = styled.div`
  white-space: pre-line;
  font-family: ${FontFamily.monospace};
  font-size: 13px;
  overflow: scroll;
  background: ${Colors.WHITE};
  border-top: 1px solid ${Colors.LIGHT_GRAY3};
  padding: 20px;
  margin: 0;
  margin-bottom: 20px;
`;

const RunRequestBody = styled.div`
  font-size: 13px;
  background: ${Colors.WHITE};
  border-top: 1px solid ${Colors.LIGHT_GRAY3};
  padding: 20px;
  margin: 0;
  margin-bottom: 20px;
`;
