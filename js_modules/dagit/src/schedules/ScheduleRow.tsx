import {useMutation, gql} from '@apollo/client';
import {
  Button,
  Colors,
  Intent,
  Menu,
  MenuItem,
  Popover,
  PopoverInteractionKind,
  Position,
  Switch,
  Tag,
  Tooltip,
} from '@blueprintjs/core';
import * as React from 'react';
import {Link} from 'react-router-dom';

import {showCustomAlert} from 'src/CustomAlertProvider';
import {TickTag} from 'src/JobTick';
import {JobRunStatus} from 'src/JobUtils';
import {PythonErrorInfo} from 'src/PythonErrorInfo';
import {ReconcileButton} from 'src/schedules/ReconcileButton';
import {SchedulePartitionStatus} from 'src/schedules/SchedulePartitionStatus';
import {humanCronString} from 'src/schedules/humanCronString';
import {ScheduleFragment} from 'src/schedules/types/ScheduleFragment';
import {
  StartSchedule,
  StartSchedule_startSchedule_PythonError,
} from 'src/schedules/types/StartSchedule';
import {
  StopSchedule,
  StopSchedule_stopRunningSchedule_PythonError,
} from 'src/schedules/types/StopSchedule';
import {JobStatus, JobType} from 'src/types/globalTypes';
import {Code} from 'src/ui/Text';
import {RepoAddress} from 'src/workspace/types';
import {workspacePathFromAddress} from 'src/workspace/workspacePath';

const errorDisplay = (status: JobStatus, runningScheduleCount: number) => {
  if (status === JobStatus.STOPPED && runningScheduleCount === 0) {
    return null;
  } else if (status === JobStatus.RUNNING && runningScheduleCount === 1) {
    return null;
  }

  const errors = [];
  if (status === JobStatus.RUNNING && runningScheduleCount === 0) {
    errors.push(
      'Schedule is set to be running, but either the scheduler is not configured or the scheduler is not running the schedule',
    );
  } else if (status === JobStatus.STOPPED && runningScheduleCount > 0) {
    errors.push('Schedule is set to be stopped, but the scheduler is still running the schedule');
  }

  if (runningScheduleCount > 0) {
    errors.push('Duplicate cron job for schedule found.');
  }

  return (
    <Popover
      interactionKind={PopoverInteractionKind.CLICK}
      popoverClassName="bp3-popover-content-sizing"
      position={Position.RIGHT}
      fill={true}
    >
      <Tag fill={true} interactive={true} intent={Intent.DANGER}>
        Error
      </Tag>
      <div>
        <h3>There are errors with this schedule.</h3>

        <p>Errors:</p>
        <ul>
          {errors.map((error, index) => (
            <li key={index}>{error}</li>
          ))}
        </ul>

        <p>
          To resolve, click <ReconcileButton /> or run <Code>dagster schedule up</Code>
        </p>
      </div>
    </Popover>
  );
};

export const displayScheduleMutationErrors = (data: StartSchedule | StopSchedule) => {
  let error:
    | StartSchedule_startSchedule_PythonError
    | StopSchedule_stopRunningSchedule_PythonError
    | null = null;

  if ('startSchedule' in data && data.startSchedule.__typename === 'PythonError') {
    error = data.startSchedule;
  } else if (
    'stopRunningSchedule' in data &&
    data.stopRunningSchedule.__typename === 'PythonError'
  ) {
    error = data.stopRunningSchedule;
  }

  if (error) {
    showCustomAlert({
      title: 'Schedule Response',
      body: (
        <>
          <PythonErrorInfo error={error} />
        </>
      ),
    });
  }
};

export const ScheduleRow: React.FC<{
  schedule: ScheduleFragment;
  repoAddress: RepoAddress;
}> = (props) => {
  const {repoAddress, schedule} = props;

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

  const {name, cronSchedule, pipelineName, mode, scheduleState} = schedule;

  const scheduleSelector = {
    repositoryLocationName: repoAddress.location,
    repositoryName: repoAddress.name,
    scheduleName: name,
  };

  const displayName = (
    <Link to={workspacePathFromAddress(repoAddress, `/schedules/${name}`)}>{name}</Link>
  );

  const {id, status, ticks, runningCount: runningScheduleCount} = scheduleState;

  const latestTick = ticks.length > 0 ? ticks[0] : null;

  return (
    <tr key={name}>
      <td style={{maxWidth: '64px'}}>
        <Switch
          checked={status === JobStatus.RUNNING}
          large={true}
          disabled={toggleOffInFlight || toggleOnInFlight}
          innerLabelChecked="on"
          innerLabel="off"
          onChange={() => {
            if (status === JobStatus.RUNNING) {
              stopSchedule({
                variables: {scheduleOriginId: id},
              });
            } else {
              startSchedule({
                variables: {scheduleSelector},
              });
            }
          }}
        />

        {errorDisplay(status, runningScheduleCount)}
      </td>
      <td>{displayName}</td>
      <td>
        <Link to={workspacePathFromAddress(repoAddress, `/pipelines/${pipelineName}/`)}>
          {pipelineName}
        </Link>
      </td>
      <td
        style={{
          maxWidth: 150,
        }}
      >
        {cronSchedule ? (
          <Tooltip position={'bottom'} content={cronSchedule}>
            {humanCronString(cronSchedule)}
          </Tooltip>
        ) : (
          <div>-</div>
        )}
      </td>
      <td style={{maxWidth: 100}}>
        {latestTick ? (
          <TickTag tick={latestTick} jobType={JobType.SCHEDULE} />
        ) : (
          <span style={{color: Colors.GRAY4}}>None</span>
        )}
      </td>
      <td>
        <JobRunStatus jobState={scheduleState} />
      </td>
      <td>
        <SchedulePartitionStatus schedule={schedule} />
      </td>
      <td>
        <div style={{display: 'flex', alignItems: 'center'}}>
          <div>{`Mode: ${mode}`}</div>
          {schedule.partitionSet ? (
            <Popover
              content={
                <Menu>
                  <MenuItem
                    text="View Partition History..."
                    icon="multi-select"
                    target="_blank"
                    href={workspacePathFromAddress(
                      repoAddress,
                      `/pipelines/${pipelineName}/partitions`,
                    )}
                  />
                  <MenuItem
                    text="Launch Partition Backfill..."
                    icon="add"
                    target="_blank"
                    href={workspacePathFromAddress(
                      repoAddress,
                      `/pipelines/${pipelineName}/partitions`,
                    )}
                  />
                </Menu>
              }
              position="bottom"
            >
              <Button small minimal icon="chevron-down" style={{marginLeft: '4px'}} />
            </Popover>
          ) : null}
        </div>
      </td>
    </tr>
  );
};

export const START_SCHEDULE_MUTATION = gql`
  mutation StartSchedule($scheduleSelector: ScheduleSelector!) {
    startSchedule(scheduleSelector: $scheduleSelector) {
      __typename
      ... on ScheduleStateResult {
        scheduleState {
          __typename
          id
          status
          runningCount
        }
      }
      ... on PythonError {
        message
        stack
      }
    }
  }
`;

export const STOP_SCHEDULE_MUTATION = gql`
  mutation StopSchedule($scheduleOriginId: String!) {
    stopRunningSchedule(scheduleOriginId: $scheduleOriginId) {
      __typename
      ... on ScheduleStateResult {
        scheduleState {
          __typename
          id
          status
          runningCount
        }
      }
      ... on PythonError {
        message
        stack
      }
    }
  }
`;
