import * as React from "react";
import { uniq } from "lodash";
import { Colors } from "@blueprintjs/core";
import styled from "styled-components/macro";

import {
  PartitionRunMatrixPipelineQuery,
  PartitionRunMatrixPipelineQueryVariables
} from "./types/PartitionRunMatrixPipelineQuery";
import { PartitionLongitudinalQuery_partitionSetOrError_PartitionSet_partitionsOrError_Partitions_results } from "./types/PartitionLongitudinalQuery";
import { RUN_STATUS_COLORS } from "../runs/RunStatusDots";
import gql from "graphql-tag";
import { useQuery } from "react-apollo";
import { useRepositorySelector } from "../DagsterRepositoryContext";
import { buildLayout } from "../gaant/GaantChartLayout";
import { GaantChartMode } from "../gaant/GaantChart";
import { pipeline } from "stream";
import { formatStepKey } from "../Util";

type Partition = PartitionLongitudinalQuery_partitionSetOrError_PartitionSet_partitionsOrError_Partitions_results;

interface PartitionRunMatrixProps {
  partitions: Partition[];
}

function byStartTime(a: Partition["runs"][0], b: Partition["runs"][0]) {
  return (
    (("startTime" in a.stats && a.stats.startTime) || 0) -
    (("startTime" in b.stats && b.stats.startTime) || 0)
  );
}

export const PartitionRunMatrix: React.FunctionComponent<PartitionRunMatrixProps> = props => {
  let pipelineName = "";
  for (const p of props.partitions) {
    if (p.runs.length) {
      pipelineName = p.runs[0].pipelineName;
      break;
    }
  }

  const repositorySelector = useRepositorySelector();
  const runinfo = useQuery<
    PartitionRunMatrixPipelineQuery,
    PartitionRunMatrixPipelineQueryVariables
  >(PARTITION_RUN_MATRIX_PIPELINE_QUERY, {
    variables: { pipelineSelector: { ...repositorySelector, pipelineName } }
  });

  const graph =
    runinfo.data?.pipelineSnapshotOrError.__typename === "PipelineSnapshot" &&
    runinfo.data.pipelineSnapshotOrError.solidHandles.map(h => h.solid);

  if (!graph) {
    return <span />;
  }

  const layout = buildLayout({
    nodes: graph,
    mode: GaantChartMode.FLAT
  });

  return (
    <GridContainer>
      <GridColumn key={"steps"}>
        <LabelTilted key="spacer" />
        {layout.boxes.map(box => (
          <Label style={{ paddingLeft: box.x }} key={box.key}>
            {box.node.name}
          </Label>
        ))}
      </GridColumn>
      {props.partitions.map(p => (
        <GridColumn key={p.name}>
          <LabelTilted>
            <div>{p.name}</div>
          </LabelTilted>
          {layout.boxes.map(({ key, node }) => {
            const statuses = uniq(
              p.runs
                .sort(byStartTime)
                .map(
                  r =>
                    r.stepStats.find(stats => formatStepKey(stats.stepKey) === node.name)?.status ||
                    "missing"
                )
            );
            return <Dot key={key} className={`${statuses.join("-").toLowerCase()}`} />;
          })}
        </GridColumn>
      ))}
    </GridContainer>
  );
};

const GridContainer = styled.div`
  display: flex;
`;
const GridColumn = styled.div`
  display: flex;
  flex-direction: column;
`;
const Label = styled.div`
  height: 15px;
  margin: 4px;
`;
const LabelTilted = styled.div`
  position: relative;
  height: 70px;
  & > div {
    position: absolute;
    bottom: 0;
    transform: rotate(-41deg);
    transform-origin: top left;
    font-size: 13px;
  }
`;
const Dot = styled.div`
  width: 15px;
  height: 15px;
  margin: 4px;
  display: inline-block;

  &.success {
    background: ${RUN_STATUS_COLORS.SUCCESS};
  }
  &.failure {
    background: ${RUN_STATUS_COLORS.FAILURE};
  }
  &.failure-success {
    background: linear-gradient(
      135deg,
      ${RUN_STATUS_COLORS.FAILURE} 40%,
      ${RUN_STATUS_COLORS.SUCCESS} 41%
    );
  }
  &.skipped {
    background: ${Colors.GOLD3};
  }
  &.skipped-success {
    background: linear-gradient(135deg, ${Colors.GOLD3} 40%, ${RUN_STATUS_COLORS.SUCCESS} 41%);
  }
  &.missing {
    background: ${Colors.LIGHT_GRAY3};
  }
  &.missing-success {
    background: linear-gradient(
      135deg,
      ${Colors.LIGHT_GRAY3} 40%,
      ${RUN_STATUS_COLORS.SUCCESS} 41%
    );
  }
`;

export const PARTITION_RUN_MATRIX_PIPELINE_QUERY = gql`
  query PartitionRunMatrixPipelineQuery($pipelineSelector: PipelineSelector) {
    pipelineSnapshotOrError(activePipelineSelector: $pipelineSelector) {
      ... on PipelineSnapshot {
        name
        solidHandles {
          handleID
          solid {
            name
            definition {
              name
            }
            inputs {
              dependsOn {
                solid {
                  name
                }
              }
            }
            outputs {
              dependedBy {
                solid {
                  name
                }
              }
            }
          }
        }
      }
    }
  }
`;
