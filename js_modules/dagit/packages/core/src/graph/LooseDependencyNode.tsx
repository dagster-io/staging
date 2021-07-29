import {gql} from '@apollo/client';
import {Colors, Icon} from '@blueprintjs/core';
import * as React from 'react';
import {Link} from 'react-router-dom';

import {LABEL_STEP_EXECUTION_TIME} from '../assets/AssetMaterializationMatrix';
import {extractNumericData} from '../assets/AssetMaterializations';
import {Sparkline} from '../assets/Sparkline';
import {ASSET_VIEW_FRAGMENT} from '../assets/queries';
import {useMaterializationBuckets} from '../assets/useMaterializationBuckets';
import {TickTag, TICK_TAG_FRAGMENT} from '../instigation/InstigationTick';
import {RunStatus} from '../runs/RunStatusDots';
import {titleForRun} from '../runs/RunUtils';
import {TimestampDisplay} from '../schedules/TimestampDisplay';
import {humanizeSensorInterval} from '../sensors/SensorDetails';
import {InstigationType, InstigationStatus} from '../types/globalTypes';
import {Box} from '../ui/Box';
import {Group} from '../ui/Group';
import {FontFamily} from '../ui/styles';
import {workspacePath} from '../workspace/workspacePath';

import {LooseDependencyAssetFragment} from './types/LooseDependencyAssetFragment';
import {LooseDependencyPipelineFragment} from './types/LooseDependencyPipelineFragment';
import {LooseDependencyPipelineNodeQuery_nodesOrError_Nodes_nodes} from './types/LooseDependencyPipelineNodeQuery';
import {LooseDependencyScheduleFragment} from './types/LooseDependencyScheduleFragment';
import {LooseDependencySensorFragment} from './types/LooseDependencySensorFragment';

export interface LooseDependencyGraphData {
  nodes: {[name: string]: NodeSelection};
  upstream: {[downstream: string]: {[upstream: string]: boolean}};
  downstream: {[upstream: string]: {[downstream: string]: boolean}};
}

export interface NodeSelection {
  id: string;
  nodeType: NodeType;
  name?: string;
  repositoryName?: string;
  repositoryLocationName?: string;
}
export interface ILooseDepsPipeline {
  name: string;
  sensors: ILooseDepsSensor[];
  schedules: ILooseDepsSchedule[];
  downstreamAssets: ILooseDepsAssetKey[];
}

interface ILooseDepsSensor {
  name: string;
  assets:
    | {
        key: ILooseDepsAssetKey;
      }[]
    | null;
}

export interface ILooseDepsAssetKey {
  path: string[];
}
interface ILooseDepsSchedule {
  name: string;
}

export interface ILooseDepsNode {
  id: string;
  type: NodeType;
  x: number;
  y: number;
}
interface IPoint {
  x: number;
  y: number;
}

export type IEdge = {
  from: IPoint;
  to: IPoint;
  dashed: boolean;
};

export enum NodeType {
  pipeline = 'pipeline',
  schedule = 'schedule',
  sensor = 'sensor',
  asset = 'asset',
}

export const getNodeDimensions = (nodeType: NodeType, selected = false) => {
  switch (nodeType) {
    case NodeType.pipeline:
      return {width: 300, height: selected ? 98 : 92};
    case NodeType.schedule:
      return {width: 300, height: selected ? 112 : 106};
    case NodeType.sensor:
      return {width: 350, height: selected ? 120 : 114};
    case NodeType.asset:
      return {width: 350, height: selected ? 94 : 88};
  }
};

export const LooseDependencyNode = ({
  graphNode,
  nodeData,
  isSelected,
}: {
  graphNode: NodeSelection;
  nodeData: LooseDependencyPipelineNodeQuery_nodesOrError_Nodes_nodes[];
  isSelected: boolean;
}) => {
  switch (graphNode.nodeType) {
    case NodeType.pipeline:
      const pipeline = nodeData.find(
        (x) => x.__typename === 'Pipeline' && x.name === graphNode.name,
      ) as LooseDependencyPipelineFragment;
      return <PipelineNode pipeline={pipeline} graphNode={graphNode} isSelected={isSelected} />;

    case NodeType.asset:
      const asset = nodeData.find(
        (x) => x.__typename === 'Asset' && JSON.stringify(x.key.path) === graphNode.name,
      ) as LooseDependencyAssetFragment;
      return <AssetNode asset={asset} isSelected={isSelected} />;

    case NodeType.schedule:
      const schedule = nodeData.find(
        (x) => x.__typename === 'Schedule' && x.name === graphNode.name,
      ) as LooseDependencyScheduleFragment;
      return <ScheduleNode schedule={schedule} graphNode={graphNode} isSelected={isSelected} />;

    case NodeType.sensor:
      const sensor = nodeData.find(
        (x) => x.__typename === 'Sensor' && x.name === graphNode.name,
      ) as LooseDependencySensorFragment;
      return sensor ? (
        <SensorNode sensor={sensor} graphNode={graphNode} isSelected={isSelected} />
      ) : null;

    default:
      throw new Error('unknown node type');
  }
};

const PipelineNode = ({
  pipeline,
  graphNode,
  isSelected,
}: {
  pipeline: LooseDependencyPipelineFragment;
  graphNode: NodeSelection;
  isSelected: boolean;
}) => {
  const run = pipeline.runs.length ? pipeline.runs[0] : null;
  return (
    <Box
      margin={{top: 8, horizontal: 4}}
      style={{
        border: isSelected ? `4px solid ${Colors.BLUE3}` : `1px solid #ececec`,
      }}
    >
      <Box flex={{direction: 'row', alignItems: 'center'}} padding={2}>
        <Box
          style={{width: 32, height: 32}}
          flex={{justifyContent: 'center', alignItems: 'center'}}
          margin={{right: 4}}
        >
          <Icon icon="diagram-tree" color={Colors.DARK_GRAY5} />
        </Box>
        <Box margin={2}>
          <Box margin={{vertical: 4}}>
            {graphNode.repositoryName && graphNode.repositoryLocationName ? (
              <Link
                to={workspacePath(
                  graphNode.repositoryName,
                  graphNode.repositoryLocationName,
                  `/pipelines/${pipeline.name}/overview`,
                )}
                style={{
                  fontWeight: 'bold',
                  fontSize: 14,
                }}
              >
                {pipeline.name}
              </Link>
            ) : (
              pipeline.name
            )}
          </Box>
          <Box style={{fontSize: 11, color: Colors.GRAY3}}>
            Pipeline in {graphNode.repositoryName}
          </Box>
        </Box>
      </Box>
      <Box
        style={{
          padding: 5,
          borderTop: '1px solid #ececec',
          fontSize: 12,
          backgroundColor: Colors.LIGHT_GRAY5,
        }}
      >
        {run ? (
          <Box flex={{direction: 'row', alignItems: 'center'}} style={{height: 23}} padding={4}>
            <RunStatus status={run.status} />
            <div style={{fontFamily: FontFamily.monospace, marginLeft: 4}}>
              <Link to={`/instance/runs/${run.runId}`}>{titleForRun(run)}</Link>
            </div>
          </Box>
        ) : (
          <Box style={{height: 23, color: Colors.GRAY3}} flex={{alignItems: 'center'}} padding={4}>
            No data available
          </Box>
        )}
      </Box>
    </Box>
  );
};

const SensorNode = ({
  sensor,
  graphNode,
  isSelected,
}: {
  sensor: LooseDependencySensorFragment;
  graphNode: NodeSelection;
  isSelected: boolean;
}) => {
  const latestTick = sensor.sensorState.ticks.length ? sensor.sensorState.ticks[0] : null;
  const stopped = sensor.sensorState.status === InstigationStatus.STOPPED;
  return (
    <Box
      margin={{top: 8}}
      style={{
        border: isSelected ? `4px solid ${Colors.BLUE1}` : `1px solid #ececec`,
      }}
    >
      <Box flex={{direction: 'row', alignItems: 'center'}} padding={2}>
        <Box
          style={{width: 32, height: 32}}
          flex={{justifyContent: 'center', alignItems: 'center'}}
          margin={{right: 4}}
        >
          <Icon icon="automatic-updates" color={stopped ? Colors.LIGHT_GRAY3 : Colors.GREEN4} />
        </Box>
        <Box margin={2}>
          <Box margin={{vertical: 4}}>
            {graphNode.repositoryName && graphNode.repositoryLocationName ? (
              <Link
                to={workspacePath(
                  graphNode.repositoryName,
                  graphNode.repositoryLocationName,
                  `/sensors/${sensor.name}`,
                )}
                style={{
                  fontWeight: 'bold',
                  fontSize: 14,
                }}
              >
                {sensor.name}
              </Link>
            ) : (
              sensor.name
            )}
          </Box>
          <Box style={{fontSize: 11, color: Colors.GRAY3}}>
            Sensor in {graphNode.repositoryName}
          </Box>
        </Box>
      </Box>
      <Box
        style={{
          padding: 5,
          borderTop: '1px solid #ececec',
          fontSize: 12,
          backgroundColor: Colors.LIGHT_GRAY5,
        }}
      >
        <table>
          <tbody>
            <tr>
              <td width={100}>Frequency</td>
              <td>{humanizeSensorInterval(sensor.minIntervalSeconds)}</td>
            </tr>
            <tr>
              <td width={100}>Latest tick</td>
              <td>
                {latestTick ? (
                  <Group direction="row" spacing={8} alignItems="center">
                    <TimestampDisplay timestamp={latestTick.timestamp} />
                    <TickTag tick={latestTick} instigationType={InstigationType.SENSOR} />
                  </Group>
                ) : (
                  <Box style={{height: 20}} flex={{alignItems: 'center'}}>
                    -
                  </Box>
                )}
              </td>
            </tr>
          </tbody>
        </table>
      </Box>
    </Box>
  );
};

const ScheduleNode = ({
  schedule,
  graphNode,
  isSelected,
}: {
  schedule: LooseDependencyScheduleFragment;
  graphNode: NodeSelection;
  isSelected: boolean;
}) => {
  const stopped = schedule.scheduleState.status === InstigationStatus.STOPPED;
  const latestTick = schedule.scheduleState.ticks.length ? schedule.scheduleState.ticks[0] : null;
  return (
    <Box
      style={{
        border: isSelected ? `4px solid ${Colors.BLUE1}` : `1px solid #ececec`,
      }}
    >
      <Box flex={{direction: 'row', alignItems: 'center'}} padding={2}>
        <Box
          style={{width: 32, height: 32}}
          flex={{justifyContent: 'center', alignItems: 'center'}}
          margin={{right: 4}}
        >
          <Icon icon="time" color={stopped ? Colors.LIGHT_GRAY3 : Colors.GREEN4} />
        </Box>
        <Box margin={2}>
          <Box margin={{vertical: 4}}>
            {graphNode.repositoryName && graphNode.repositoryLocationName ? (
              <Link
                to={workspacePath(
                  graphNode.repositoryName,
                  graphNode.repositoryLocationName,
                  `/schedules/${schedule.name}`,
                )}
                style={{
                  fontWeight: 'bold',
                  fontSize: 14,
                }}
              >
                {schedule.name}
              </Link>
            ) : (
              schedule.name
            )}
          </Box>
          <Box style={{fontSize: 11, color: Colors.GRAY3}}>
            Schedule in {graphNode.repositoryName}
          </Box>
        </Box>
      </Box>
      <Box
        style={{
          padding: 5,
          borderTop: '1px solid #ececec',
          fontSize: 12,
          backgroundColor: Colors.LIGHT_GRAY5,
        }}
      >
        <table>
          <tbody>
            <tr>
              <td width={100}>Schedule</td>
              <td>
                <div
                  style={{
                    fontFamily: FontFamily.monospace,
                    backgroundColor: Colors.LIGHT_GRAY3,
                    padding: '0px 6px',
                  }}
                >
                  {schedule.cronSchedule}
                </div>
              </td>
            </tr>
            <tr>
              <td width={100}>Latest tick</td>
              <td>
                <Box style={{height: 20}} flex={{alignItems: 'center'}}>
                  {latestTick ? (
                    <TickTag tick={latestTick} instigationType={InstigationType.SCHEDULE} />
                  ) : (
                    '-'
                  )}
                </Box>
              </td>
            </tr>
          </tbody>
        </table>
      </Box>
    </Box>
  );
};

const AssetNode = ({
  asset,
  isSelected,
}: {
  asset: LooseDependencyAssetFragment;
  isSelected: boolean;
}) => {
  const to = `/instance/assets/${asset.key.path.map(encodeURIComponent).join('/')}`;
  const materializations = asset.assetMaterializations || [];
  const isPartitioned = materializations.some((m) => m.partition);
  const bucketed = useMaterializationBuckets({
    materializations,
    isPartitioned,
    shouldBucketPartitions: true,
  });
  const reversed = React.useMemo(() => [...bucketed].reverse(), [bucketed]);
  const latest = reversed.map((m) => m.latest);
  const graphDataByMetadataLabel = extractNumericData(latest, isPartitioned ? 'partition' : 'time');
  const graphData = graphDataByMetadataLabel[LABEL_STEP_EXECUTION_TIME];

  return (
    <Box
      margin={{top: 8}}
      style={{
        border: isSelected ? `4px solid ${Colors.BLUE1}` : `1px solid #ececec`,
      }}
    >
      <Box flex={{direction: 'row', alignItems: 'center'}}>
        <Box
          style={{width: 44, height: 44}}
          flex={{justifyContent: 'center', alignItems: 'center'}}
          margin={{right: 4}}
          background={Colors.GREEN4}
        >
          <Icon icon="box" color={Colors.WHITE} />
        </Box>
        <Box margin={2}>
          <Box margin={{vertical: 4}}>
            <Link to={to} style={{fontWeight: 'bold', fontSize: 14}}>
              {asset.key.path.join(' > ')}
            </Link>
          </Box>
          <Box style={{fontSize: 11, color: Colors.GRAY3}}>Asset</Box>
        </Box>
      </Box>
      <Box
        style={{
          padding: 5,
          borderTop: '1px solid #ececec',
          fontSize: 12,
          backgroundColor: Colors.LIGHT_GRAY5,
        }}
      >
        {graphData ? (
          <Box
            flex={{direction: 'row', alignItems: 'center', justifyContent: 'space-between'}}
            margin={{right: 8}}
          >
            <div>{LABEL_STEP_EXECUTION_TIME}</div>
            <Sparkline data={graphData} width={150} height={23} />
          </Box>
        ) : (
          <Box style={{height: 23, color: Colors.GRAY3}} flex={{alignItems: 'center'}} padding={4}>
            No data available
          </Box>
        )}
      </Box>
    </Box>
  );
};

export const LOOSE_DEPENDENCY_ASSET_NODE_FRAGMENT = gql`
  fragment LooseDependencyAssetFragment on Asset {
    id
    key {
      path
    }
    ...AssetViewFragment
  }
  ${ASSET_VIEW_FRAGMENT}
`;

export const LOOSE_DEPENDENCY_SCHEDULE_NODE_FRAGMENT = gql`
  fragment LooseDependencyScheduleFragment on Schedule {
    id
    name
    mode
    cronSchedule
    scheduleState {
      id
      status
      ticks(limit: 1) {
        id
        ...TickTagFragment
      }
    }
  }
  ${TICK_TAG_FRAGMENT}
`;

export const LOOSE_DEPENDENCY_SENSOR_NODE_FRAGMENT = gql`
  fragment LooseDependencySensorFragment on Sensor {
    id
    name
    minIntervalSeconds
    sensorState {
      id
      status
      ticks(limit: 1) {
        id
        ...TickTagFragment
      }
    }
  }
  ${TICK_TAG_FRAGMENT}
`;

export const LOOSE_DEPENDENCY_PIPELINE_NODE_FRAGMENT = gql`
  fragment LooseDependencyPipelineFragment on Pipeline {
    id
    name
    runs(limit: 1) {
      id
      runId
      status
    }
  }
`;
