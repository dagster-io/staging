import * as React from "react";
import gql from "graphql-tag";
import { RunGraph, PIPELINE_LABEL } from "./RunGraph";
import { TriggerGraphsRunFragment } from "./types/TriggerGraphsRunFragment";
import styled from "styled-components/macro";
import { RunsFilter } from "./runs/RunsFilter";
import { colorHash } from "./Util";
import { Colors } from "@blueprintjs/core";
import { TokenizingFieldValue } from "./TokenizingField";

type Run = TriggerGraphsRunFragment;

export const TriggerGraphs: React.FunctionComponent<{
  runs: TriggerGraphsRunFragment[];
  stepKeys: string[];
}> = ({ runs, stepKeys }) => {
  const initial: { [stepKey: string]: boolean } = { [PIPELINE_LABEL]: true };
  stepKeys.forEach(stepKey => (initial[stepKey] = true));
  const [selectedStepKeys, setSelectedStepKeys] = React.useState(initial);
  const [tokens, setTokens] = React.useState<TokenizingFieldValue[]>([]);
  const durationGraph = React.useRef<any>(undefined);
  const materializationGraph = React.useRef<any>(undefined);
  const successGraph = React.useRef<any>(undefined);
  const failureGraph = React.useRef<any>(undefined);
  const rateGraph = React.useRef<any>(undefined);
  const graphs = [durationGraph, materializationGraph, successGraph, failureGraph, rateGraph];
  React.useEffect(() => {
    const initial: { [stepKey: string]: boolean } = { [PIPELINE_LABEL]: true };
    stepKeys.forEach(stepKey => (initial[stepKey] = true));
    setSelectedStepKeys(initial);
  }, [stepKeys]);

  const onStepChange = (selectedKeys: { [stepKey: string]: boolean }) => {
    setSelectedStepKeys(selectedKeys);
    graphs.forEach(graph => {
      const chart = graph?.current?.chart?.current?.chartInstance;
      const datasets = chart?.data?.datasets || [];
      datasets.forEach((dataset: any, idx: number) => {
        const meta = chart.getDatasetMeta(idx);
        meta.hidden = dataset.label in selectedKeys ? !selectedKeys[dataset.label] : false;
      });
    });
  };

  return (
    <Container>
      <div style={{ flex: 1 }}>
        <RunGraph
          runs={runs}
          title="Execution Duration"
          yLabel="Duration (secs)"
          getPipelineDataForRun={getPipelineDurationForRun}
          getStepDataForRun={getStepDurationsForRun}
          ref={durationGraph}
        />
        <RunGraph
          runs={runs}
          title="Materialization Count"
          yLabel="Number of materializations"
          getPipelineDataForRun={getPipelineMaterializationCountForRun}
          getStepDataForRun={getStepMaterializationCountForRun}
          ref={materializationGraph}
        />
        <RunGraph
          runs={runs}
          title="Expectation Successes"
          yLabel="Number of successes"
          getPipelineDataForRun={getPipelineExpectationSuccessForRun}
          getStepDataForRun={getStepExpectationSuccessForRun}
          ref={successGraph}
        />
        <RunGraph
          runs={runs}
          title="Expectation Failures"
          yLabel="Number of failures"
          getPipelineDataForRun={getPipelineExpectationFailureForRun}
          getStepDataForRun={getStepExpectationFailureForRun}
          ref={failureGraph}
        />
        <RunGraph
          runs={runs}
          title="Expectation Rate"
          yLabel="Rate of success"
          getPipelineDataForRun={getPipelineExpectationRateForRun}
          getStepDataForRun={getStepExpectationRateForRun}
          ref={rateGraph}
        />
      </div>
      <div style={{ width: 450 }}>
        <NavContainer>
          <NavSectionHeader>Run filters</NavSectionHeader>
          <NavSection>
            <RunsFilter tokens={tokens} onChange={setTokens} enabledFilters={["status", "tag"]} />
          </NavSection>
          <StepSelector selected={selectedStepKeys} onChange={onStepChange} />
        </NavContainer>
      </div>
    </Container>
  );
};

const getPipelineDurationForRun = (run: any) => {
  const { stats } = run;
  if (
    stats &&
    stats.__typename === "PipelineRunStatsSnapshot" &&
    stats.endTime &&
    stats.startTime
  ) {
    return stats.endTime - stats.startTime;
  }

  return undefined;
};

const getStepDurationsForRun = (run: any) => {
  const { stepStats } = run;

  const perStepDuration = {};
  stepStats.forEach((stepStat: any) => {
    if (stepStat.endTime && stepStat.startTime) {
      perStepDuration[stepStat.stepKey] = stepStat.endTime - stepStat.startTime;
    }
  });

  return perStepDuration;
};

const getPipelineMaterializationCountForRun = (run: Run) => {
  const { stats } = run;
  if (stats && stats.__typename === "PipelineRunStatsSnapshot") {
    return stats.materializations;
  }
  return undefined;
};

const getStepMaterializationCountForRun = (run: Run) => {
  const { stepStats } = run;
  const perStepCounts = {};
  stepStats.forEach(stepStat => {
    perStepCounts[stepStat.stepKey] = stepStat.materializations?.length || 0;
  });
  return perStepCounts;
};

const getPipelineExpectationSuccessForRun = (run: Run) => {
  const stepCounts: { [key: string]: number } = getStepExpectationSuccessForRun(run);
  return _arraySum(Object.values(stepCounts));
};

const getStepExpectationSuccessForRun = (run: Run) => {
  const { stepStats } = run;
  const perStepCounts = {};
  stepStats.forEach(stepStat => {
    perStepCounts[stepStat.stepKey] =
      stepStat.expectationResults?.filter(x => x.success).length || 0;
  });
  return perStepCounts;
};

const getPipelineExpectationFailureForRun = (run: Run) => {
  const stepCounts: { [key: string]: number } = getStepExpectationFailureForRun(run);
  return _arraySum(Object.values(stepCounts));
};

const getStepExpectationFailureForRun = (run: Run) => {
  const { stepStats } = run;
  const perStepCounts = {};
  stepStats.forEach(stepStat => {
    perStepCounts[stepStat.stepKey] =
      stepStat.expectationResults?.filter(x => !x.success).length || 0;
  });
  return perStepCounts;
};

const getPipelineExpectationRateForRun = (run: Run) => {
  const stepSuccesses: {
    [key: string]: number;
  } = getStepExpectationSuccessForRun(run);
  const stepFailures: {
    [key: string]: number;
  } = getStepExpectationFailureForRun(run);

  const pipelineSuccesses = _arraySum(Object.values(stepSuccesses));
  const pipelineFailures = _arraySum(Object.values(stepFailures));
  const pipelineTotal = pipelineSuccesses + pipelineFailures;

  return pipelineTotal ? pipelineSuccesses / pipelineTotal : 0;
};

const getStepExpectationRateForRun = (run: Run) => {
  const { stepStats } = run;
  const perStepCounts = {};
  stepStats.forEach(stepStat => {
    const results = stepStat.expectationResults || [];
    perStepCounts[stepStat.stepKey] = results.length
      ? results.filter(x => x.success).length / results.length
      : 0;
  });
  return perStepCounts;
};

const _arraySum = (arr: number[]) => {
  let sum = 0;
  arr.forEach(x => (sum += x));
  return sum;
};

const StepSelector = ({
  selected,
  onChange
}: {
  selected: { [stepKey: string]: boolean };
  onChange: (selected: { [stepKey: string]: boolean }) => void;
}) => {
  const onStepClick = (stepKey: string) => {
    return (evt: React.MouseEvent) => {
      if (evt.shiftKey) {
        // toggle on shift+click
        onChange({ ...selected, [stepKey]: !selected[stepKey] });
      } else {
        // regular click
        const newSelected = {};

        const alreadySelected = Object.keys(selected).every(key => {
          return key === stepKey ? selected[key] : !selected[key];
        });

        Object.keys(selected).forEach(key => {
          newSelected[key] = alreadySelected || key === stepKey;
        });

        onChange(newSelected);
      }
    };
  };

  return (
    <>
      <NavSectionHeader>
        Run steps
        <div style={{ flex: 1 }} />
        <span style={{ fontSize: 13, opacity: 0.5 }}>Tip: Shift-click to multi-select</span>
      </NavSectionHeader>
      <NavSection>
        {Object.keys(selected).map(stepKey => (
          <Item
            key={stepKey}
            shown={selected[stepKey]}
            onClick={onStepClick(stepKey)}
            color={stepKey === PIPELINE_LABEL ? Colors.GRAY2 : colorHash(stepKey)}
          >
            <div
              style={{
                display: "inline-block",
                marginRight: 5,
                borderRadius: 5,
                height: 10,
                width: 10,
                backgroundColor: selected[stepKey]
                  ? stepKey === PIPELINE_LABEL
                    ? Colors.GRAY2
                    : colorHash(stepKey)
                  : "#aaaaaa"
              }}
            />
            {stepKey}
          </Item>
        ))}
      </NavSection>
    </>
  );
};

export const TRIGGER_GRAPHS_RUN_FRAGMENT = gql`
  fragment TriggerGraphsRunFragment on PipelineRun {
    runId
    stats {
      ... on PipelineRunStatsSnapshot {
        startTime
        endTime
        materializations
      }
    }
    stepStats {
      __typename
      stepKey
      startTime
      endTime
      status
      materializations {
        __typename
      }
      expectationResults {
        success
      }
    }
    ...RunGraphRunFragment
  }
  ${RunGraph.fragments.RunGraphRunFragment}
`;

const Container = styled.div`
  display: flex;
  flex-direction: row;
  position: relative;
  max-width: 1600px;
  margin: 0 auto;
`;

const Item = styled.div`
  list-style-type: none;
  padding: 5px 2px;
  cursor: pointer;
  text-decoration: ${({ shown }: { shown: boolean }) => (shown ? "none" : "line-through")};
  user-select: none;
  font-size: 12px;
  color: ${props => (props.shown ? props.color : "#aaaaaa")};
  white-space: nowrap;
`;
const NavSectionHeader = styled.div`
  border-bottom: 1px solid ${Colors.GRAY5};
  margin-bottom: 10px;
  padding-bottom: 5px;
  display: flex;
`;
const NavSection = styled.div`
  margin-bottom: 30px;
`;
const NavContainer = styled.div`
  margin: 20px 0 0 10px;
  padding: 10px;
  background-color: #fff;
  border: 1px solid ${Colors.GRAY5};
  overflow: auto;
`;
