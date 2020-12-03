import {useMutation} from '@apollo/client';
import {Colors, Icon, NonIdealState, Switch, Tooltip} from '@blueprintjs/core';
import * as React from 'react';
import {Link} from 'react-router-dom';
import styled from 'styled-components';

import {timestampToString, TimezoneContext} from 'src/TimeComponents';
import {
  displayScheduleMutationErrors,
  START_SCHEDULE_MUTATION,
  STOP_SCHEDULE_MUTATION,
  TickTag,
} from 'src/schedules/ScheduleRow';
import {humanCronString} from 'src/schedules/humanCronString';
import {ScheduleDefinitionFragment} from 'src/schedules/types/ScheduleDefinitionFragment';
import {StartSchedule} from 'src/schedules/types/StartSchedule';
import {StopSchedule} from 'src/schedules/types/StopSchedule';
import {ScheduleStatus} from 'src/types/globalTypes';
import {Box} from 'src/ui/Box';
import {ButtonLink} from 'src/ui/ButtonLink';
import {Countdown, CountdownStatus} from 'src/ui/Countdown';
import {Group} from 'src/ui/Group';
import {MetadataTable} from 'src/ui/MetadataTable';
import {Code, Heading} from 'src/ui/Text';
import {FontFamily} from 'src/ui/styles';
import {useScheduleSelector} from 'src/workspace/WorkspaceContext';
import {repoAddressAsString} from 'src/workspace/repoAddressAsString';
import {RepoAddress} from 'src/workspace/types';
import {workspacePathFromAddress} from 'src/workspace/workspacePath';

interface TimestampDisplayProps {
  timestamp: number;
  timezone: string | null;
}

const TimestampDisplay = (props: TimestampDisplayProps) => {
  const {timestamp, timezone} = props;
  const [userTimezone] = React.useContext(TimezoneContext);

  return (
    <span>
      {timestampToString({unix: timestamp, format: 'MMM DD, h:mm A z'}, timezone || userTimezone)}
    </span>
  );
};

export const ScheduleDetails: React.FC<{
  schedule: ScheduleDefinitionFragment;
  repoAddress: RepoAddress;
  countdownDuration: number;
  countdownStatus: CountdownStatus;
  onRefresh: () => void;
}> = (props) => {
  const {repoAddress, schedule, countdownDuration, countdownStatus, onRefresh} = props;
  const {cronSchedule, executionTimezone, futureTicks, name, partitionSet, pipelineName} = schedule;

  const [copyText, setCopyText] = React.useState('Click to copy');

  const [startSchedule, {loading: toggleOnInFlight}] = useMutation<StartSchedule>(
    START_SCHEDULE_MUTATION,
    {
      onCompleted: displayScheduleMutationErrors,
    },
  );
  const [stopSchedule, {loading: toggleOffInFlight}] = useMutation<StopSchedule>(
    STOP_SCHEDULE_MUTATION,
    {
      onCompleted: displayScheduleMutationErrors,
    },
  );

  const scheduleSelector = useScheduleSelector(name);

  // Restore the tooltip text after a delay.
  React.useEffect(() => {
    let token: any;
    if (copyText === 'Copied!') {
      token = setTimeout(() => {
        setCopyText('Click to copy');
      }, 2000);
    }
    return () => {
      token && clearTimeout(token);
    };
  }, [copyText]);

  const {scheduleState} = schedule;

  // TODO dish: Port over something like the existing UI
  if (!scheduleState) {
    return (
      <NonIdealState
        icon="time"
        title="Schedule not found"
        description={
          <>
            Schedule <strong>{name}</strong> not found in{' '}
            <strong>{repoAddressAsString(repoAddress)}</strong>
          </>
        }
      />
    );
  }

  const {status, ticks, scheduleOriginId} = scheduleState;
  const latestTick = ticks.length > 0 ? ticks[0] : null;

  const onChangeSwitch = () => {
    if (status === ScheduleStatus.RUNNING) {
      stopSchedule({
        variables: {scheduleOriginId},
      });
    } else {
      startSchedule({
        variables: {scheduleSelector},
      });
    }
  };

  const copyId = () => {
    navigator.clipboard.writeText(scheduleOriginId);
    setCopyText('Copied!');
  };

  const countdownMessage = (timeRemaining: number) => {
    const refreshing = countdownStatus === 'idle' || timeRemaining === 0;
    const seconds = Math.floor(timeRemaining / 1000);
    return (
      <Group direction="horizontal" spacing={8} alignItems="center">
        <span style={{color: Colors.GRAY3, fontVariantNumeric: 'tabular-nums'}}>
          {refreshing ? 'Refreshing data…' : `0:${seconds < 10 ? `0${seconds}` : seconds}`}
        </span>
        <Tooltip content="Refresh now">
          <RefreshButton onClick={onRefresh}>
            <Icon iconSize={11} icon="refresh" color={Colors.GRAY3} />
          </RefreshButton>
        </Tooltip>
      </Group>
    );
  };

  const running = status === ScheduleStatus.RUNNING;

  return (
    <Box flex={{direction: 'row', justifyContent: 'space-between', alignItems: 'flex-start'}}>
      <Group direction="vertical" spacing={12}>
        <Group alignItems="center" direction="horizontal" spacing={4}>
          <Heading>{name}</Heading>
          <Box margin={{left: 12}}>
            <Switch
              checked={running}
              inline
              large
              disabled={toggleOffInFlight || toggleOnInFlight}
              innerLabelChecked="on"
              innerLabel="off"
              onChange={onChangeSwitch}
              style={{margin: '2px 0 0 0'}}
            />
          </Box>
          {futureTicks.results.length && running ? (
            <Group direction="horizontal" spacing={4}>
              <div>Next tick:</div>
              <TimestampDisplay
                timestamp={futureTicks.results[0].timestamp}
                timezone={executionTimezone}
              />
            </Group>
          ) : null}
        </Group>
        <MetadataTable
          rows={[
            {
              key: 'Latest tick',
              value: latestTick ? (
                <Group direction="horizontal" spacing={8} alignItems="center">
                  <TimestampDisplay timestamp={latestTick.timestamp} timezone={executionTimezone} />
                  <TickTag
                    status={latestTick.status}
                    eventSpecificData={latestTick.tickSpecificData}
                  />
                </Group>
              ) : (
                'None'
              ),
            },
            {
              key: 'Pipeline',
              value: (
                <Link to={workspacePathFromAddress(repoAddress, `/pipelines/${pipelineName}/`)}>
                  {pipelineName}
                </Link>
              ),
            },
            {
              key: 'Schedule',
              value: cronSchedule ? (
                <Group direction="horizontal" spacing={8}>
                  <span>{humanCronString(cronSchedule)}</span>
                  <Code>({cronSchedule})</Code>
                </Group>
              ) : (
                <div>-</div>
              ),
            },
            {
              key: 'Mode',
              value: schedule.mode,
            },
            {
              key: 'Partition set',
              value: partitionSet ? (
                <Link
                  to={workspacePathFromAddress(
                    repoAddress,
                    `/pipelines/${pipelineName}/partitions`,
                  )}
                >
                  {partitionSet.name}
                </Link>
              ) : (
                'None'
              ),
            },
          ]}
        />
      </Group>
      <Box margin={{top: 4}}>
        <Group direction="vertical" spacing={8} alignItems="flex-end">
          <div>
            <Countdown
              duration={countdownDuration}
              message={countdownMessage}
              status={countdownStatus}
            />
          </div>
          <Tooltip content={copyText}>
            <ButtonLink color={{link: Colors.GRAY3, hover: Colors.GRAY1}} onClick={copyId}>
              <span style={{fontFamily: FontFamily.monospace}}>{`id: ${scheduleOriginId.slice(
                0,
                8,
              )}`}</span>
            </ButtonLink>
          </Tooltip>
        </Group>
      </Box>
    </Box>
  );
};

const RefreshButton = styled.button`
  border: none;
  cursor: pointer;
  padding: 0;
  margin: 0;
  outline: none;
  background-color: transparent;

  .bp3-icon {
    display: block;

    &:hover {
      svg {
        fill: ${Colors.DARK_GRAY5};
      }
    }
  }
`;
