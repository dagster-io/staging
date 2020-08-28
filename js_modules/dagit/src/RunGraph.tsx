import * as React from "react";
import gql from "graphql-tag";
import { RowContainer } from "./ListComponents";

import { Line } from "react-chartjs-2";
import { colorHash } from "./Util";
import { Colors } from "@blueprintjs/core";
import { RunGraphRunFragment } from "./types/RunGraphRunFragment";

type PointValue = number | null | undefined;
type Point = { x: PointValue; y: PointValue };

export const PIPELINE_LABEL = "Total pipeline";

type Run = RunGraphRunFragment;

interface GraphProps {
  runs: Run[];
  getPipelineDataForRun: (run: Run) => PointValue;
  getStepDataForRun: (run: Run) => { [key: string]: PointValue[] };
  title?: string;
  yLabel?: string;
}

export class RunGraph extends React.Component<GraphProps> {
  static fragments = {
    RunGraphRunFragment: gql`
      fragment RunGraphRunFragment on PipelineRun {
        runId
        stats {
          ... on PipelineRunStatsSnapshot {
            startTime
            endTime
          }
          ... on PythonError {
            ...PythonErrorFragment
          }
        }
        stepStats {
          __typename
          stepKey
          startTime
          endTime
        }
      }
    `
  };
  chart = React.createRef<any>();
  getDefaultOptions = () => {
    const { title, yLabel } = this.props;
    const titleOptions = title ? { display: true, text: title } : undefined;
    const scales = yLabel
      ? {
          yAxes: [
            {
              scaleLabel: { display: true, labelString: yLabel }
            }
          ],
          xAxes: [
            {
              type: "time",
              time: {
                parser: "X",
                tooltipFormat: "ll"
              },
              scaleLabel: { display: true, labelString: "Pipeline Start Time" }
            }
          ]
        }
      : undefined;
    return {
      title: titleOptions,
      scales,
      legend: {
        display: false,
        onClick: (_e: MouseEvent, _legendItem: any) => {}
      }
    };
  };

  buildDatasetData() {
    const { runs, getPipelineDataForRun, getStepDataForRun } = this.props;

    const pipelineData: Point[] = [];
    const stepData = {};

    runs.forEach(run => {
      pipelineData.push({
        x: _getExecutionTime(run),
        y: getPipelineDataForRun(run)
      });

      const stepDataforRun = getStepDataForRun(run);
      Object.keys(stepDataforRun).forEach(stepKey => {
        stepData[stepKey] = [
          ...(stepData[stepKey] || []),
          {
            x: _getExecutionTime(run),
            y: stepDataforRun[stepKey]
          }
        ];
      });
    });

    // stepData may have holes due to missing runs or missing steps.  For these to
    // render properly, fill in the holes with `undefined` values.
    Object.keys(stepData).forEach(stepKey => {
      stepData[stepKey] = _fillPartitions(runs, stepData[stepKey]);
    });

    return { pipelineData, stepData };
  }

  render() {
    const { pipelineData, stepData } = this.buildDatasetData();
    const graphData = {
      // labels: Object.keys(runs),
      datasets: [
        {
          label: PIPELINE_LABEL,
          data: pipelineData,
          borderColor: Colors.GRAY2,
          backgroundColor: "rgba(0,0,0,0)"
        },
        ...Object.keys(stepData).map(stepKey => ({
          label: stepKey,
          data: stepData[stepKey],
          borderColor: colorHash(stepKey),
          backgroundColor: "rgba(0,0,0,0)"
        }))
      ]
    };
    const options = this.getDefaultOptions();
    return (
      <RowContainer style={{ margin: "20px 0" }}>
        <Line data={graphData} height={100} options={options} ref={this.chart} />
      </RowContainer>
    );
  }
}

const _getExecutionTime = (run: Run) => {
  if (run.stats.__typename === "PipelineRunStatsSnapshot") {
    return run.stats.startTime ? run.stats.startTime * 1000 : null;
  }
  return undefined;
};

const _fillPartitions = (runs: Run[], points: Point[]) => {
  const pointData = {};
  points.forEach(point => {
    if (point.x) {
      pointData[point.x] = point.y;
    }
  });

  return runs.map(run => {
    const x = _getExecutionTime(run);
    const y = x ? pointData[x] : undefined;
    return { x, y };
  });
};
