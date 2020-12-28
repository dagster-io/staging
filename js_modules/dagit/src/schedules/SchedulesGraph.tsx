import {gql, useQuery} from '@apollo/client';
import {Colors} from '@blueprintjs/core';
import {TimeUnit} from 'chart.js';
import * as React from 'react';
import {Line, ChartComponentProps} from 'react-chartjs-2';

import {PythonErrorInfo} from 'src/PythonErrorInfo';
import {
  SchedulesGraphQuery,
  SchedulesGraphQuery_repositoryOrError_Repository_schedules_scheduleState_ticks,
} from 'src/schedules/types/SchedulesGraphQuery';
import {JobStatus, JobTickStatus} from 'src/types/globalTypes';
import {repoAddressToSelector} from 'src/workspace/repoAddressToSelector';
import {RepoAddress} from 'src/workspace/types';

type Tick = SchedulesGraphQuery_repositoryOrError_Repository_schedules_scheduleState_ticks;

const COLOR_MAP = {
  [JobTickStatus.SUCCESS]: Colors.BLUE3,
  [JobTickStatus.FAILURE]: Colors.RED3,
  [JobTickStatus.STARTED]: Colors.GRAY3,
  [JobTickStatus.SKIPPED]: Colors.GOLD3,
};
const TICK_LIMIT = 30;

export const SchedulesGraph: React.FC<{
  repoAddress: RepoAddress;
}> = ({repoAddress}) => {
  const repositorySelector = repoAddressToSelector(repoAddress);
  const {data} = useQuery<SchedulesGraphQuery>(SCHEDULES_GRAPH_QUERY, {
    variables: {repositorySelector, tickLimit: TICK_LIMIT},
    fetchPolicy: 'cache-and-network',
    pollInterval: 15 * 1000,
    partialRefetch: true,
  });

  if (data?.repositoryOrError.__typename !== 'Repository') {
    return null;
  }

  const now = Date.now();

  const schedules = data.repositoryOrError.schedules;

  const graphData: ChartComponentProps['data'] = {
    datasets: schedules.map((schedule, index) => ({
      label: schedule.name,
      data: [
        ...schedule.scheduleState.ticks.map((tick) => ({
          x: 1000 * tick.timestamp,
          y: schedules.length - index,
        })),
        ...(schedule.scheduleState.status === JobStatus.RUNNING
          ? schedule.futureTicks.results.map((tick) => ({
              x: 1000 * tick.timestamp,
              y: schedules.length - index,
            }))
          : []),
      ],
      borderColor: Colors.LIGHT_GRAY4,
      borderWidth: 0.5,
      backgroundColor: 'rgba(0,0,0,0)',
      pointBackgroundColor: [
        ...schedule.scheduleState.ticks.map((tick) => COLOR_MAP[tick.status]),
        ...schedule.futureTicks.results.map((_) => Colors.GRAY5),
      ],
      pointBorderWidth: 1,
      pointBorderColor: [
        ...schedule.scheduleState.ticks.map((tick) => COLOR_MAP[tick.status]),
        ...schedule.futureTicks.results.map((_) => Colors.GRAY5),
      ],
      pointRadius: 3,
      pointHoverBorderWidth: 1,
      pointHoverRadius: 3,
    })),
  };

  const calculateBounds = () => {
    const defaultMin = now - 1000 * 86400 * 7;
    const defaultMax = now + 1000 * 86400;
    return {
      min: Math.max(
        defaultMin,
        ...schedules.map((schedule) =>
          schedule.scheduleState.ticks.length < TICK_LIMIT
            ? defaultMin
            : Math.min(...schedule.scheduleState.ticks.map((tick) => 1000 * tick.timestamp)),
        ),
      ),
      max: Math.min(
        defaultMax,
        ...schedules.map((schedule) =>
          schedule.scheduleState.status === JobStatus.RUNNING
            ? Math.max(...schedule.futureTicks.results.map((tick) => 1000 * tick.timestamp))
            : 0,
        ),
      ),
    };
  };

  const calculateUnit: () => TimeUnit = () => {
    const {min, max} = calculateBounds();
    const range = max - min;
    const factor = 2;
    const hour = 3600000;
    const day = 24 * hour;
    const month = 30 * day;
    const year = 365 * day;

    if (range < factor * hour) {
      return 'minute';
    }
    if (range < factor * day) {
      return 'hour';
    }
    if (range < factor * month) {
      return 'day';
    }
    if (range < factor * year) {
      return 'month';
    }
    return 'year';
  };

  const statusLabelForTick = (tick: Tick) => {
    switch (tick.status) {
      case JobTickStatus.STARTED:
        return 'Started';
      case JobTickStatus.FAILURE:
        return 'Failed';
      case JobTickStatus.SUCCESS:
        return 'Requested';
      case JobTickStatus.SKIPPED:
        return 'Skipped';
    }
  };

  const options: ChartComponentProps['options'] = {
    scales: {
      yAxes: [
        {
          scaleLabel: {display: false},
          ticks: {display: false, min: 0, max: schedules.length + 1},
          gridLines: {display: false},
        },
      ],
      xAxes: [
        {
          type: 'time',
          scaleLabel: {
            display: false,
          },
          bounds: 'ticks',
          ticks: {
            ...calculateBounds(),
          },
          time: {
            minUnit: calculateUnit(),
          },
        },
      ],
    },
    legend: {
      display: false,
    },
    tooltips: {
      displayColors: false,
      intersect: false,
      callbacks: {
        title: function (tooltipItems, _) {
          if (!tooltipItems.length) {
            return '';
          }
          const tooltipItem = tooltipItems[0];
          if (tooltipItem.index === undefined || tooltipItem.datasetIndex === undefined) {
            return '';
          }

          const schedule = schedules[tooltipItem.datasetIndex];
          return schedule.name;
        },

        label: function (tooltipItem, {datasets}) {
          if (
            tooltipItem.index === undefined ||
            tooltipItem.datasetIndex === undefined ||
            !datasets
          ) {
            return '';
          }
          const schedule = schedules[tooltipItem.datasetIndex];
          if (schedule.scheduleState.ticks.length <= tooltipItem.index) {
            return `${tooltipItem.xLabel}: Upcoming`;
          }

          const tick = schedule.scheduleState.ticks[tooltipItem.index];
          return `${tooltipItem.xLabel}: ${statusLabelForTick(tick)}`;
        },
      },
    },
  };

  return <Line data={graphData} height={30} options={options} key="100%" />;
};

export const SCHEDULES_GRAPH_QUERY = gql`
  query SchedulesGraphQuery($repositorySelector: RepositorySelector!, $tickLimit: Int!) {
    repositoryOrError(repositorySelector: $repositorySelector) {
      ... on Repository {
        id
        schedules {
          id
          name
          scheduleState {
            id
            status
            ticks(limit: $tickLimit) {
              id
              status
              timestamp
              skipReason
              runIds
              error {
                ...PythonErrorFragment
              }
            }
          }
          futureTicks(limit: 10) {
            results {
              timestamp
            }
          }
        }
      }
    }
  }
  ${PythonErrorInfo.fragments.PythonErrorFragment}
`;
