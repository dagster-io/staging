import {gql, useLazyQuery} from '@apollo/client';
import {Colors} from '@blueprintjs/core';
import qs from 'qs';
import * as React from 'react';
import {Link} from 'react-router-dom';

import {useFeatureFlags} from '../app/Flags';
import {assertUnreachable} from '../app/Util';
import {StatusTable} from '../instigation/InstigationUtils';
import {PipelineRunStatus} from '../types/globalTypes';
import {ButtonLink} from '../ui/ButtonLink';
import {Group} from '../ui/Group';
import {RepoAddress} from '../workspace/types';
import {workspacePathFromAddress} from '../workspace/workspacePath';

import {
  SchedulePartitionStatusFragment,
  SchedulePartitionStatusFragment_partitionSet_partitionStatusesOrError_PartitionStatuses_results as Partition,
} from './types/SchedulePartitionStatusFragment';
import {SchedulePartitionStatusQuery} from './types/SchedulePartitionStatusQuery';

const RUN_STATUSES = ['Succeeded', 'Failed', 'Missing', 'Pending'];

const calculateDisplayStatus = (partition: Partition) => {
  switch (partition.runStatus) {
    case null:
      return 'Missing';
    case PipelineRunStatus.SUCCESS:
      return 'Succeeded';
    case PipelineRunStatus.FAILURE:
    case PipelineRunStatus.CANCELED:
    case PipelineRunStatus.CANCELING:
      return 'Failed';
    case PipelineRunStatus.MANAGED:
    case PipelineRunStatus.QUEUED:
    case PipelineRunStatus.NOT_STARTED:
    case PipelineRunStatus.STARTED:
    case PipelineRunStatus.STARTING:
      return 'Pending';
    default:
      return assertUnreachable(partition.runStatus);
  }
};

export const SchedulePartitionStatus: React.FC<{
  repoAddress: RepoAddress;
  scheduleName: string;
}> = React.memo(({repoAddress, scheduleName}) => {
  const [retrievePartitionStatus, {data, loading}] = useLazyQuery<SchedulePartitionStatusQuery>(
    SCHEDULE_PARTITION_STATUS_QUERY,
    {
      variables: {
        scheduleSelector: {
          scheduleName,
          repositoryName: repoAddress.name,
          repositoryLocationName: repoAddress.location,
        },
      },
    },
  );

  const onClick = React.useCallback(() => retrievePartitionStatus(), [retrievePartitionStatus]);

  if (loading) {
    return <div style={{color: Colors.GRAY3}}>Loading…</div>;
  }

  if (!data) {
    return <ButtonLink onClick={onClick}>Show partition set</ButtonLink>;
  }

  const schedule = data.scheduleOrError;
  if (schedule.__typename === 'Schedule') {
    return <RetrievedSchedulePartitionStatus schedule={schedule} repoAddress={repoAddress} />;
  }

  return <div style={{color: Colors.RED1}}>Partition set not found!</div>;
});

const RetrievedSchedulePartitionStatus: React.FC<{
  schedule: SchedulePartitionStatusFragment;
  repoAddress: RepoAddress;
}> = ({schedule, repoAddress}) => {
  const {flagPipelineModeTuples} = useFeatureFlags();
  const {partitionSet, pipelineName, mode} = schedule;
  const partitionSetName = partitionSet?.name;

  const partitionPath = React.useMemo(() => {
    const query = partitionSetName
      ? qs.stringify(
          {
            partitionSet: partitionSetName,
          },
          {addQueryPrefix: true},
        )
      : '';
    return `/${
      flagPipelineModeTuples ? 'jobs' : 'pipelines'
    }/${pipelineName}:${mode}/partitions${query}`;
  }, [flagPipelineModeTuples, pipelineName, mode, partitionSetName]);

  if (
    !schedule.partitionSet ||
    schedule.partitionSet.partitionStatusesOrError.__typename !== 'PartitionStatuses'
  ) {
    return <span style={{color: Colors.GRAY4}}>None</span>;
  }

  const partitions = schedule.partitionSet.partitionStatusesOrError.results;
  const partitionsByType = {};
  partitions.forEach((partition) => {
    const displayStatus = calculateDisplayStatus(partition);
    partitionsByType[displayStatus] = [...(partitionsByType[displayStatus] || []), partition];
  });

  const partitionUrl = workspacePathFromAddress(repoAddress, partitionPath);

  return (
    <Group direction="column" spacing={4}>
      <Link to={partitionUrl}>{schedule.partitionSet.name}</Link>
      <StatusTable>
        <tbody>
          {RUN_STATUSES.map((status) => {
            if (!(status in partitionsByType)) {
              return null;
            }
            return (
              <tr key={status}>
                <td style={{width: '100px'}}>{status}</td>
                <td>
                  {status === 'Failed' || status === 'Missing' ? (
                    <Link
                      to={`${partitionUrl}?showFailuresAndGapsOnly=true`}
                      style={{color: Colors.DARK_GRAY1}}
                    >
                      {partitionsByType[status].length}
                    </Link>
                  ) : (
                    partitionsByType[status].length
                  )}
                </td>
              </tr>
            );
          })}
        </tbody>
      </StatusTable>
    </Group>
  );
};

const SCHEDULE_PARTITION_STATUS_FRAGMENT = gql`
  fragment SchedulePartitionStatusFragment on Schedule {
    id
    mode
    pipelineName
    partitionSet {
      id
      name
      partitionStatusesOrError {
        ... on PartitionStatuses {
          results {
            id
            partitionName
            runStatus
          }
        }
      }
    }
  }
`;

const SCHEDULE_PARTITION_STATUS_QUERY = gql`
  query SchedulePartitionStatusQuery($scheduleSelector: ScheduleSelector!) {
    scheduleOrError(scheduleSelector: $scheduleSelector) {
      ... on Schedule {
        id
        ...SchedulePartitionStatusFragment
      }
    }
  }
  ${SCHEDULE_PARTITION_STATUS_FRAGMENT}
`;
