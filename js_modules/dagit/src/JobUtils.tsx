import {gql} from '@apollo/client';
import * as React from 'react';
import styled from 'styled-components/macro';

import {TICK_TAG_FRAGMENT} from 'src/JobTick';
import {PythonErrorInfo} from 'src/PythonErrorInfo';
import {REPOSITORY_ORIGIN_FRAGMENT} from 'src/RepositoryInformation';
import {RunStatus} from 'src/runs/RunStatusDots';
import {titleForRun} from 'src/runs/RunUtils';
import {JobStateFragment} from 'src/types/JobStateFragment';

export const JobRunStatus: React.FC<{
  jobState: JobStateFragment;
}> = ({jobState}) => {
  if (!jobState.lastRequestedRuns.length) {
    return <div>&mdash;</div>;
  }
  return (
    <StatusTable>
      <tbody>
        {jobState.lastRequestedRuns.map((run) => (
          <tr key={run.id}>
            <td>
              <div style={{display: 'flex', alignItems: 'center', padding: 2}}>
                <RunStatus status={run.status} />
                <a
                  href={`/instance/runs/${run.runId}`}
                  style={{marginLeft: 5, color: '#a88860'}}
                  target="_blank"
                  rel="noreferrer"
                >
                  {titleForRun({runId: run.runId})}
                </a>
              </div>
            </td>
          </tr>
        ))}
      </tbody>
    </StatusTable>
  );
};

export const JOB_STATE_FRAGMENT = gql`
  fragment JobStateFragment on JobState {
    id
    name
    jobType
    status
    repositoryOrigin {
      ...RepositoryOriginFragment
    }
    jobSpecificData {
      ... on SensorJobData {
        lastRunKey
      }
      ... on ScheduleJobData {
        cronSchedule
      }
    }
    status
    lastRequestedRuns {
      id
      runId
      status
    }
    ticks(limit: 1) {
      id
      ...TickTagFragment
    }
    runningCount
  }
  ${REPOSITORY_ORIGIN_FRAGMENT}
  ${PythonErrorInfo.fragments.PythonErrorFragment}
  ${TICK_TAG_FRAGMENT}
`;

export const StatusTable = styled.table`
  width: 100%;
  padding: 0;
  margin-top: 4px;
  border-top: 1px solid #dbc5ad;
  border-left: 1px solid #dbc5ad;
  border-spacing: 0;
  background: #fffaf5;
  td:first-child {
    color: #a88860;
  }
  tbody > tr {
    margin: 0;
    padding: 0;
  }
  tbody > tr > th,
  tbody > tr > td {
    padding: 2px;
    border-bottom: 1px solid #dbc5ad;
    border-right: 1px solid #dbc5ad;
    vertical-align: top;
    box-shadow: none !important;
  }

  tbody > tr > th {
    padding: 4px 2px;
  }
`;
