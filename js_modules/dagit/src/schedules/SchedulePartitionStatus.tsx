import * as React from 'react';

import {StatusTable} from 'src/JobUtils';
import {ScheduleFragment} from 'src/schedules/types/ScheduleFragment';
import {PartitionRunStatus} from 'src/types/globalTypes';

const RUN_STATUSES = [
  PartitionRunStatus.SUCCESS,
  PartitionRunStatus.FAILURE,
  PartitionRunStatus.MISSING,
  PartitionRunStatus.PENDING,
];
const STATUS_LABELS = {
  [PartitionRunStatus.SUCCESS]: 'Succeeded',
  [PartitionRunStatus.FAILURE]: 'Failed',
  [PartitionRunStatus.MISSING]: 'Missing',
  [PartitionRunStatus.PENDING]: 'Pending',
};

export const SchedulePartitionStatus: React.FC<{
  schedule: ScheduleFragment;
}> = ({schedule}) => {
  if (
    !schedule.partitionSet ||
    schedule.partitionSet.partitionsOrError.__typename !== 'Partitions'
  ) {
    return <div>&mdash;</div>;
  }

  const partitions = schedule.partitionSet.partitionsOrError.results;
  const partitionsByType = {};
  partitions.forEach((partition) => {
    partitionsByType[partition.status] = [...(partitionsByType[partition.status] || []), partition];
  });

  return (
    <StatusTable>
      <tbody>
        <tr>
          <th colSpan={2}>{schedule.partitionSet.name}</th>
        </tr>
        {RUN_STATUSES.map((status) =>
          status in partitionsByType ? (
            <tr key={status}>
              <td style={{width: 100}}>{STATUS_LABELS[status]}</td>
              <td>{partitionsByType[status].length}</td>
            </tr>
          ) : null,
        )}
      </tbody>
    </StatusTable>
  );
};
