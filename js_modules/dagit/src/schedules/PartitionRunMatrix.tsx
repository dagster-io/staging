import * as React from "react";
import { uniq } from "lodash";
import { Colors, Checkbox, NonIdealState } from "@blueprintjs/core";
import styled from "styled-components/macro";

import {
  PartitionRunMatrixPipelineQuery,
  PartitionRunMatrixPipelineQueryVariables,
  PartitionRunMatrixPipelineQuery_pipelineSnapshotOrError_PipelineSnapshot_solidHandles
} from "./types/PartitionRunMatrixPipelineQuery";
import { PartitionLongitudinalQuery_partitionSetOrError_PartitionSet_partitionsOrError_Partitions_results } from "./types/PartitionLongitudinalQuery";
import { RUN_STATUS_COLORS } from "../runs/RunStatusDots";
import gql from "graphql-tag";
import { useQuery } from "react-apollo";
import { useRepositorySelector } from "../DagsterRepositoryContext";
import { buildLayout } from "../gaant/GaantChartLayout";
import { GaantChartMode } from "../gaant/GaantChart";
import { formatStepKey } from "../Util";
import { StepEventStatus } from "../types/globalTypes";
import { GaantChartLayout } from "../gaant/Constants";
import { GraphQueryInput } from "../GraphQueryInput";
import { OptionsDivider } from "../VizComponents";
import { RunTable } from "../runs/RunTable";
import { filterByQuery } from "../GraphQueryImpl";
import {
  tokenizedValuesFromString,
  TokenizingFieldValue,
  stringFromValue,
  TokenizingField
} from "../TokenizingField";
import { shallowCompareKeys } from "@blueprintjs/core/lib/cjs/common/utils";
import { useViewport } from "../gaant/useViewport";

type Partition = PartitionLongitudinalQuery_partitionSetOrError_PartitionSet_partitionsOrError_Partitions_results;
type SolidHandle = PartitionRunMatrixPipelineQuery_pipelineSnapshotOrError_PipelineSnapshot_solidHandles;

const TITLE_TOTAL_FAILURES = "This step failed at least once for this percent of partitions.";

const TITLE_FINAL_FAILURES = "This step failed to run successfully for this percent of partitions.";

const SUCCESS_COLOR = "#CFE6DC";

const BOX_COL_WIDTH = 23;

const OVERSCROLL = 150;

function byStartTime(a: Partition["runs"][0], b: Partition["runs"][0]) {
  return (
    (("startTime" in a.stats && a.stats.startTime) || 0) -
    (("startTime" in b.stats && b.stats.startTime) || 0)
  );
}

interface DisplayOptions {
  showSucessful: boolean;
  showPrevious: boolean;
}

function buildMatrixData(
  layout: GaantChartLayout,
  partitions: Partition[],
  options: DisplayOptions
) {
  const partitionColumns = partitions.map(p => ({
    name: p.name,
    runs: p.runs,
    steps: layout.boxes.map(({ node }) => {
      const statuses = uniq(
        p.runs
          .sort(byStartTime)
          .map(
            r =>
              r.stepStats.find(stats => formatStepKey(stats.stepKey) === node.name)?.status ||
              "missing"
          )
      );
      return {
        name: node.name,
        statuses: statuses
      };
    })
  }));

  const stepRows = layout.boxes.map((box, idx) => {
    const totalFailures = partitionColumns.filter(p =>
      p.steps[idx].statuses.includes(StepEventStatus.FAILURE)
    );
    const finalFailures = partitionColumns.filter(
      p =>
        p.steps[idx].statuses[p.steps[idx].statuses.length - 1] !== StepEventStatus.SUCCESS &&
        !(p.steps[idx].statuses.length === 0) &&
        !(p.steps[idx].statuses.length === 1 && p.steps[idx].statuses[0] === "missing")
    );
    return {
      x: box.x,
      name: box.node.name,
      totalFailurePercent: Math.round((totalFailures.length / partitionColumns.length) * 100),
      finalFailurePercent: Math.round((finalFailures.length / partitionColumns.length) * 100)
    };
  });

  if (!options.showPrevious) {
    partitionColumns.forEach(p =>
      p.steps.forEach(s => (s.statuses = s.statuses.slice(s.statuses.length - 1)))
    );
  }

  if (!options.showSucessful) {
    for (let ii = stepRows.length - 1; ii >= 0; ii--) {
      if (stepRows[ii].finalFailurePercent === 0) {
        stepRows.splice(ii, 1);
        partitionColumns.forEach(p => p.steps.splice(ii, 1));
      }
    }
  }

  return { stepRows, partitions, partitionColumns };
}

interface PartitionRunMatrixProps {
  pipelineName: string;
  partitions: Partition[];
}

interface MatrixDataInputs {
  solidHandles: SolidHandle[] | false;
  partitions: Partition[];
  stepQuery: string;
  runsFilter: TokenizingFieldValue[];
  options: DisplayOptions;
}

/**
 * This hook uses the inputs provided to filter the data displayed and calls through to buildMatrixData.
 * It uses a React ref to cache the result and avoids re-computing when all inputs are shallow-equal.
 *
 * - This could alternatively be implemented via React.memo and an outer + inner component pair, but I
 *   didn't want to split <PartitionRunMatrix />
 * - This can't be a React useEffect with an array of deps because we want the cached value to be updated
 *   synchronously when the inputs are modified to avoid a double-render caused by an effect + state var.
 *
 * @param inputs
 */
const useMatrixData = (inputs: MatrixDataInputs) => {
  const cachedMatrixData = React.useRef<{
    result: ReturnType<typeof buildMatrixData>;
    inputs: MatrixDataInputs;
  }>();
  if (!inputs.solidHandles) {
    return null;
  }
  if (cachedMatrixData.current && shallowCompareKeys(inputs, cachedMatrixData.current.inputs)) {
    return cachedMatrixData.current.result;
  }

  // Filter the runs down to the subset matching the tags input (eg: backfillId)
  const partitionsFiltered = inputs.partitions.map(p => ({
    ...p,
    runs: runsMatchingTagTokens(p.runs, inputs.runsFilter)
  }));

  // Filter the pipeline's structure and build the flat gaant layout for the left hand side
  const solidsFiltered = filterByQuery(
    inputs.solidHandles.map(h => h.solid),
    inputs.stepQuery
  );
  const layout = buildLayout({ nodes: solidsFiltered.all, mode: GaantChartMode.FLAT });

  // Build the matrix of step + partition squares - presorted to match the gaant layout
  const result = buildMatrixData(layout, partitionsFiltered, inputs.options);
  cachedMatrixData.current = { result, inputs };
  return result;
};

export const PartitionRunMatrix: React.FunctionComponent<PartitionRunMatrixProps> = props => {
  const { viewport, containerProps } = useViewport();
  const [runsFilter, setRunsFilter] = React.useState<TokenizingFieldValue[]>([]);
  const [focusedPartitionName, setFocusedPartitionName] = React.useState<string>("");
  const [hoveredStepName, setHoveredStepName] = React.useState<string>("");
  const [stepQuery, setStepQuery] = React.useState<string>("");
  const [options, setOptions] = React.useState<DisplayOptions>({
    showPrevious: false,
    showSucessful: true
  });

  // Retrieve the pipeline's structure
  const repositorySelector = useRepositorySelector();
  const pipelineSelector = { ...repositorySelector, pipelineName: props.pipelineName };
  const pipeline = useQuery<
    PartitionRunMatrixPipelineQuery,
    PartitionRunMatrixPipelineQueryVariables
  >(PARTITION_RUN_MATRIX_PIPELINE_QUERY, {
    variables: { pipelineSelector }
  });

  const solidHandles =
    pipeline.data?.pipelineSnapshotOrError.__typename === "PipelineSnapshot" &&
    pipeline.data.pipelineSnapshotOrError.solidHandles;

  const data = useMatrixData({
    partitions: props.partitions,
    solidHandles,
    stepQuery,
    runsFilter,
    options
  });
  if (!data || !solidHandles) {
    return <span />;
  }

  const { stepRows, partitionColumns, partitions } = data;
  const focusedPartition = partitions.find(p => p.name === focusedPartitionName);

  const visibleRangeStart = Math.max(0, Math.floor((viewport.left - OVERSCROLL) / BOX_COL_WIDTH));
  const visibleCount = Math.ceil((viewport.width + OVERSCROLL * 2) / BOX_COL_WIDTH);
  const visibleColumns = partitionColumns.slice(
    visibleRangeStart,
    visibleRangeStart + visibleCount
  );
  return (
    <PartitionRunMatrixContainer>
      <OptionsContainer>
        <strong>Partition Run Matrix</strong>
        <div style={{ width: 20 }} />
        <Checkbox
          label="Show Previous Run States"
          checked={options.showPrevious}
          onChange={() => setOptions({ ...options, showPrevious: !options.showPrevious })}
          style={{ marginBottom: 0, height: 20 }}
        />
        <OptionsDivider />
        <Checkbox
          label="Show Successful Steps"
          checked={options.showSucessful}
          onChange={() => setOptions({ ...options, showSucessful: !options.showSucessful })}
          style={{ marginBottom: 0, height: 20 }}
        />
        <div style={{ flex: 1 }} />
        <RunTagsTokenizingField
          runs={partitions.reduce((a, b) => [...a, ...b.runs], [])}
          onChange={setRunsFilter}
          tokens={runsFilter}
        />
      </OptionsContainer>
      <div
        style={{
          position: "relative",
          display: "flex",
          border: `1px solid ${Colors.GRAY5}`,
          borderLeft: 0
        }}
      >
        <GridFloatingContainer floating={viewport.left > 0}>
          <GridColumn disabled style={{ flexShrink: 1, overflow: "hidden" }}>
            <TopLabel>
              <GraphQueryInput
                small
                width={260}
                items={solidHandles.map(h => h.solid)}
                value={stepQuery}
                placeholder="Type a Step Subset"
                onChange={setStepQuery}
              />
            </TopLabel>
            {stepRows.map(step => (
              <LeftLabel
                style={{ paddingLeft: step.x }}
                key={step.name}
                data-tooltip={step.name}
                hovered={step.name === hoveredStepName}
              >
                {step.name}
              </LeftLabel>
            ))}
            <Divider />
            <LeftLabel style={{ paddingLeft: 5 }}>Runs</LeftLabel>
          </GridColumn>
          <GridColumn disabled>
            <TopLabel>
              <div className="square failure-blank" title={TITLE_TOTAL_FAILURES} />
            </TopLabel>
            {stepRows.map(({ totalFailurePercent, name }, idx) => (
              <LeftLabel
                key={idx}
                title={TITLE_TOTAL_FAILURES}
                hovered={name === hoveredStepName}
                redness={totalFailurePercent / 100}
              >
                {`${totalFailurePercent}%`}
              </LeftLabel>
            ))}
            <Divider />
          </GridColumn>
          <GridColumn disabled>
            <TopLabel>
              <div className="square failure" title={TITLE_FINAL_FAILURES} />
            </TopLabel>
            {stepRows.map(({ finalFailurePercent, name }, idx) => (
              <LeftLabel
                key={idx}
                title={TITLE_FINAL_FAILURES}
                hovered={name === hoveredStepName}
                redness={finalFailurePercent / 100}
              >
                {`${finalFailurePercent}%`}
              </LeftLabel>
            ))}
            <Divider />
          </GridColumn>
        </GridFloatingContainer>
        <GridScrollContainer {...containerProps}>
          <div
            style={{
              width: partitionColumns.length * BOX_COL_WIDTH,
              position: "relative",
              height: "100%"
            }}
          >
            {visibleColumns.map((p, idx) => (
              <GridColumn
                key={p.name}
                style={{
                  zIndex: visibleColumns.length - idx,
                  width: BOX_COL_WIDTH,
                  position: "absolute",
                  left: (idx + visibleRangeStart) * BOX_COL_WIDTH
                }}
                focused={p.name === focusedPartitionName}
                onClick={() => setFocusedPartitionName(p.name)}
              >
                <TopLabelTilted>
                  <div className="tilted">{p.name}</div>
                </TopLabelTilted>
                {p.steps.map(({ name, statuses }) => (
                  <div
                    key={name}
                    className={`square ${statuses.join("-").toLowerCase()}`}
                    onMouseEnter={() => setHoveredStepName(name)}
                    onMouseLeave={() => setHoveredStepName("")}
                  />
                ))}
                <Divider />
                <LeftLabel style={{ textAlign: "center" }}>{p.runs.length}</LeftLabel>
              </GridColumn>
            ))}
          </div>
        </GridScrollContainer>
      </div>
      {stepRows.length === 0 && <EmptyMessage>No data to display.</EmptyMessage>}
      <div style={{ padding: "10px 0", minHeight: 220 }}>
        <RunTable
          runs={focusedPartition ? focusedPartition.runs : []}
          onSetFilter={() => {}}
          nonIdealState={
            <NonIdealState
              description={
                focusedPartition
                  ? `No runs for ${focusedPartitionName}. Select another partition above.`
                  : `No runs to display. Select a partition above.`
              }
            />
          }
        />
      </div>
    </PartitionRunMatrixContainer>
  );
};

const EmptyMessage = styled.div`
  padding: 20px;
  text-align: center;
`;

const PartitionRunMatrixContainer = styled.div`
  display: block;
  border-bottom: 1px solid ${Colors.GRAY5};
`;

const OptionsContainer = styled.div`
  display: flex;
  align-items: center;
  padding: 6px 0;
`;

const Divider = styled.div`
  height: 1px;
  width: 100%;
  margin-top: 5px;
  border-top: 1px solid ${Colors.GRAY5};
`;

// In CSS, you can layer multiple backgrounds on top of each other by comma-separating values in
// `background`. However, this only works with gradients, not with primitive color values. To do
// hovered + red without color math (?), just stack the colors as flat gradients.
const flatGradient = (color: string) => `linear-gradient(to left, ${color} 0%, ${color} 100%)`;
const flatGradientStack = (colors: string[]) => colors.map(flatGradient).join(",");

const LeftLabel = styled.div<{ hovered?: boolean; redness?: number }>`
  height: 23px;
  line-height: 23px;
  font-size: 13px;
  overflow: hidden;
  text-overflow: ellipsis;
  background: ${({ redness, hovered }) =>
    flatGradientStack([
      redness ? `rgba(255, 0, 0, ${redness * 0.6})` : "transparent",
      hovered ? Colors.LIGHT_GRAY3 : "transparent"
    ])};
`;

const TopLabel = styled.div`
  position: relative;
  height: 70px;
  padding: 4px;
  padding-bottom: 0;
  min-width: 15px;
  align-items: flex-end;
  display: flex;
`;

const TopLabelTilted = styled.div`
  position: relative;
  height: 55px;
  padding: 4px;
  padding-bottom: 0;
  min-width: 15px;
  margin-bottom: 15px;
  align-items: end;
  display: flex;

  & > div.tilted {
    font-size: 12px;
    white-space: nowrap;
    position: absolute;
    bottom: -20px;
    left: 0;
    padding: 2px;
    padding-right: 4px;
    padding-left: 0;
    transform: rotate(-41deg);
    transform-origin: top left;
  }
`;

const GridFloatingContainer = styled.div<{ floating: boolean }>`
  display: flex;
  border-right: 1px solid ${Colors.GRAY5};
  width: 330px;
  z-index: 2;
  ${({ floating }) => (floating ? "box-shadow: 1px 0 4px rgba(0, 0, 0, 0.15)" : "")};
`;

const GridScrollContainer = styled.div`
  padding-right: 60px;
  overflow-x: scroll;
  background: ${Colors.LIGHT_GRAY5};
  flex: 1;
`;

const GridColumn = styled.div<{ disabled?: boolean; focused?: boolean }>`
  display: flex;
  flex-direction: column;
  flex-shrink: 0;

  ${({ disabled, focused }) =>
    !disabled &&
    !focused &&
    `&:hover {
    cursor: default;
    background: ${Colors.LIGHT_GRAY3};
    ${TopLabelTilted} {
      background: ${Colors.LIGHT_GRAY5};
      .tilted {
        background: ${Colors.LIGHT_GRAY3};
      }
    }
  }`}

  ${({ focused }) =>
    focused &&
    `background: ${Colors.BLUE4};
    ${LeftLabel} {
      color: white;
    }
    ${TopLabelTilted} {
      background: ${Colors.LIGHT_GRAY5};
      color: white;
      .tilted {
        background: ${Colors.BLUE4};
      }
    }
  }`}

  .square {
    width: 23px;
    height: 23px;
    display: inline-block;

    &:before {
      content: " ";
      display: inline-block;
      width: 15px;
      height: 15px;
      margin: 4px;
    }
    &.success-skipped,
    &.success-failure,
    &.success {
      &:before {
        background: ${SUCCESS_COLOR};
      }
    }
    &.failure {
      &:before {
        background: ${RUN_STATUS_COLORS.FAILURE};
      }
    }
    &.failure-success {
      &:before {
        background: linear-gradient(135deg, ${RUN_STATUS_COLORS.FAILURE} 40%, ${SUCCESS_COLOR} 41%);
      }
    }
    &.failure-blank {
      &:before {
        background: linear-gradient(
          135deg,
          ${RUN_STATUS_COLORS.FAILURE} 40%,
          rgba(150, 150, 150, 0.3) 41%
        );
      }
    }
    &.skipped {
      &:before {
        background: ${Colors.GOLD3};
      }
    }
    &.skipped-success {
      &:before {
        background: linear-gradient(135deg, ${Colors.GOLD3} 40%, ${SUCCESS_COLOR} 41%);
      }
    }
    &.missing {
      &:before {
        background: ${Colors.LIGHT_GRAY3};
      }
    }
    &.missing-success {
      &:before {
        background: linear-gradient(135deg, ${Colors.LIGHT_GRAY3} 40%, ${SUCCESS_COLOR} 41%);
      }
    }
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

interface RunTagsTokenizingFieldProps {
  runs: Partition["runs"];
  tokens: TokenizingFieldValue[];
  onChange: (tokens: TokenizingFieldValue[]) => void;
}

function runsMatchingTagTokens(runs: Partition["runs"], tokens: TokenizingFieldValue[]) {
  return runs.filter(
    run =>
      tokens.length === 0 ||
      tokens.some(({ token, value }) => {
        if (token === "tag") {
          const [tkey, tvalue] = value.split("=");
          return run.tags.some(tag => tag.key === tkey && tag.value === tvalue);
        }
        throw new Error(`Unknown token: ${token}`);
      })
  );
}

const RunTagsTokenizingField: React.FunctionComponent<RunTagsTokenizingFieldProps> = ({
  runs,
  tokens,
  onChange
}) => {
  const suggestions = [
    {
      token: "tag",
      values: () => {
        const runTags = runs.map(r => r.tags).reduce((a, b) => [...a, ...b], []);
        const runTagValues = runTags.map(t => `${t.key}=${t.value}`);
        return uniq(runTagValues).sort();
      }
    }
  ];
  const search = tokenizedValuesFromString(stringFromValue(tokens), suggestions);
  return (
    <TokenizingField
      small
      values={search}
      onChange={onChange}
      placeholder="Filter partition runs..."
      suggestionProviders={suggestions}
      loading={false}
    />
  );
};
