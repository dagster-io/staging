import * as React from "react";
import { uniq } from "lodash";
import { Colors, Checkbox } from "@blueprintjs/core";
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
import { formatStepKey } from "../Util";
import { StepEventStatus } from "../types/globalTypes";
import { GaantChartLayout } from "../gaant/Constants";
import { GraphQueryInput } from "../GraphQueryInput";
import { OptionsContainer, OptionsDivider } from "../VizComponents";
import { RunTable } from "../runs/RunTable";
import { filterByQuery } from "../GraphQueryImpl";
import {
  tokenizedValuesFromString,
  TokenizingFieldValue,
  stringFromValue,
  TokenizingField
} from "../TokenizingField";

type Partition = PartitionLongitudinalQuery_partitionSetOrError_PartitionSet_partitionsOrError_Partitions_results;

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
        !p.steps[idx].statuses.includes(StepEventStatus.SUCCESS) &&
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

  return { stepRows, partitionColumns };
}

interface PartitionRunMatrixProps {
  pipelineName: string;
  partitions: Partition[];
}

export const PartitionRunMatrix: React.FunctionComponent<PartitionRunMatrixProps> = props => {
  const [runsFilter, setRunsFilter] = React.useState<TokenizingFieldValue[]>([]);
  const [focusedPartition, setFocusedPartition] = React.useState<string>("");
  const [query, setQuery] = React.useState<string>("");
  const [options, setOptions] = React.useState<DisplayOptions>({
    showPrevious: false,
    showSucessful: true
  });

  // Filter the runs down to the subset matching the tags input (eg: backfillId)
  const partitionsFiltered = props.partitions.map(p => ({
    ...p,
    runs: runsMatchingTagTokens(p.runs, runsFilter)
  }));

  // Retrieve the pipeline's structure
  const repositorySelector = useRepositorySelector();
  const pipelineSelector = { ...repositorySelector, pipelineName: props.pipelineName };
  const pipeline = useQuery<
    PartitionRunMatrixPipelineQuery,
    PartitionRunMatrixPipelineQueryVariables
  >(PARTITION_RUN_MATRIX_PIPELINE_QUERY, {
    variables: { pipelineSelector }
  });

  const solids =
    pipeline.data?.pipelineSnapshotOrError.__typename === "PipelineSnapshot" &&
    pipeline.data.pipelineSnapshotOrError.solidHandles.map(h => h.solid);

  if (!solids) {
    return <span />;
  }

  // Filter the pipeline's structure and build the flat gaant layout for the left hand side
  const solidsFiltered = filterByQuery(solids, query);
  const layout = buildLayout({ nodes: solidsFiltered.all, mode: GaantChartMode.FLAT });

  // Build the matrix of step + partition squares - presorted to match the gaant layout
  const { stepRows, partitionColumns } = buildMatrixData(layout, partitionsFiltered, options);

  return (
    <PartitionRunMatrixContainer>
      <OptionsContainer>
        <Checkbox
          label="Show Previous Run States"
          checked={options.showPrevious}
          onChange={() => setOptions({ ...options, showPrevious: !options.showPrevious })}
        />
        <OptionsDivider />
        <Checkbox
          label="Show Successful Steps"
          checked={options.showSucessful}
          onChange={() => setOptions({ ...options, showSucessful: !options.showSucessful })}
        />
        <div style={{ flex: 1 }} />
        <RunTagsTokenizingField
          runs={partitionsFiltered.reduce((a, b) => [...a, ...b.runs], [])}
          onChange={setRunsFilter}
          tokens={runsFilter}
        />
      </OptionsContainer>
      <GridContainer>
        <GridColumn disabled>
          <LabelTilted>
            <GraphQueryInput
              small
              width={260}
              items={solids}
              value={query}
              placeholder="Type a Step Subset"
              onChange={setQuery}
            />
          </LabelTilted>
          {stepRows.map(step => (
            <Label style={{ paddingLeft: step.x }} key={step.name}>
              {step.name}
            </Label>
          ))}
          <Divider />
          <Label>Runs</Label>
        </GridColumn>
        <GridColumn disabled>
          <LabelTilted />
          {stepRows.map(({ totalFailurePercent }, idx) => (
            <Label
              key={idx}
              style={{ background: `rgba(255, 0, 0, ${(totalFailurePercent / 100) * 0.6})` }}
              title="This step failed at least once for this percent of partitions."
            >
              {`${totalFailurePercent}%`}
            </Label>
          ))}
          <Divider />
        </GridColumn>
        <GridColumn disabled>
          <LabelTilted />
          {stepRows.map(({ finalFailurePercent }, idx) => (
            <Label
              key={idx}
              style={{ background: `rgba(255, 0, 0, ${(finalFailurePercent / 100) * 0.6})` }}
              title="This step failed to run successfully for this percent of partitions."
            >
              {`${finalFailurePercent}%`}
            </Label>
          ))}
          <Divider />
        </GridColumn>
        {partitionColumns.map((p, idx) => (
          <GridColumn
            key={p.name}
            style={{ zIndex: partitionColumns.length - idx }}
            focused={p.name === focusedPartition}
            onClick={() => setFocusedPartition(p.name)}
          >
            <LabelTilted>
              <div className="tilted">{p.name}</div>
            </LabelTilted>
            {p.steps.map(({ name, statuses }) => (
              <Square key={name} className={`${statuses.join("-").toLowerCase()}`} />
            ))}
            <Divider />
            <Label style={{ textAlign: "center" }}>{p.runs.length}</Label>
          </GridColumn>
        ))}
      </GridContainer>
      {stepRows.length === 0 && <EmptyMessage>No data to display.</EmptyMessage>}
      {focusedPartition && (
        <RunTable
          runs={partitionsFiltered.find(p => p.name === focusedPartition)!.runs}
          onSetFilter={() => {}}
        />
      )}
    </PartitionRunMatrixContainer>
  );
};

const EmptyMessage = styled.div`
  padding: 20px;
  text-align: center;
`;
const PartitionRunMatrixContainer = styled.div`
  display: block;
`;
const Divider = styled.div`
  height: 1px;
  width: 100%;
  margin-top: 5px;
  border-top: 1px solid ${Colors.GRAY5};
`;
const Label = styled.div`
  height: 15px;
  margin: 4px;
  font-size: 13px;
`;

const LabelTilted = styled.div`
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

const GridContainer = styled.div`
  display: flex;
  padding-right: 60px;
  overflow-x: scroll;
`;

const GridColumn = styled.div<{ disabled?: boolean; focused?: boolean }>`
  display: flex;
  flex-direction: column;
  ${({ disabled, focused }) =>
    !disabled &&
    !focused &&
    `&:hover {
    cursor: default;
    background: ${Colors.LIGHT_GRAY4};
    ${LabelTilted} {
      background: ${Colors.WHITE};
      .tilted {
        background: ${Colors.LIGHT_GRAY4};
      }
    }
  }`}
  ${({ focused }) =>
    focused &&
    `background: ${Colors.BLUE4};
    ${Label} {
      color: white;
    }
    ${LabelTilted} {
      background: ${Colors.WHITE};
      color: white;
      .tilted {
        background: ${Colors.BLUE4};
      }
    }
  }`}
`;

const SUCCESS_COLOR = "#CFE6DC";

const Square = styled.div`
  width: 15px;
  height: 15px;
  margin: 4px;
  display: inline-block;

  &.success {
    background: ${SUCCESS_COLOR};
  }
  &.failure {
    background: ${RUN_STATUS_COLORS.FAILURE};
  }
  &.failure-success {
    background: linear-gradient(135deg, ${RUN_STATUS_COLORS.FAILURE} 40%, ${SUCCESS_COLOR} 41%);
  }
  &.skipped {
    background: ${Colors.GOLD3};
  }
  &.skipped-success {
    background: linear-gradient(135deg, ${Colors.GOLD3} 40%, ${SUCCESS_COLOR} 41%);
  }
  &.missing {
    background: ${Colors.LIGHT_GRAY3};
  }
  &.missing-success {
    background: linear-gradient(135deg, ${Colors.LIGHT_GRAY3} 40%, ${SUCCESS_COLOR} 41%);
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
