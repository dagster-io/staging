import {Meta} from '@storybook/react/types-6-0';
import * as React from 'react';

import {ApolloTestProvider} from '../testing/ApolloTestProvider';
import {StepEventStatus} from '../types/globalTypes';
import {TokenizingFieldValue} from '../ui/TokenizingField';

import {PartitionRunMatrix} from './PartitionRunMatrix';
import {PartitionRunMatrixRunFragment} from './types/PartitionRunMatrixRunFragment';

// eslint-disable-next-line import/no-default-export
export default {
  title: 'PartitionRunMatrix',
  component: PartitionRunMatrix,
} as Meta;

const PipelineMocks = {
  PipelineSnapshotOrError: () => ({
    __typename: 'PipelineSnapshot',
  }),
  PipelineSnapshot: () => ({
    name: () => 'TestPipeline',
    solidHandles: () => [
      {
        __typename: 'SolidHandle',
        handleID: 'a',
        solid: {
          name: 'a',
          definition: {
            __typename: 'SolidDefinition',
            name: 'a',
          },
          inputs: [],
          outputs: [
            {
              dependedBy: [
                {
                  solid: {
                    name: 'b',
                  },
                },
                {
                  solid: {
                    name: 'c',
                  },
                },
              ],
            },
          ],
        },
      },
      {
        __typename: 'SolidHandle',
        handleID: 'b',
        solid: {
          name: 'b',
          definition: {
            __typename: 'SolidDefinition',
            name: 'b',
          },
          inputs: [{dependsOn: [{solid: {name: 'a'}}]}],
          outputs: [
            {
              dependedBy: [
                {
                  solid: {
                    name: 'c',
                  },
                },
              ],
            },
          ],
        },
      },
      {
        __typename: 'SolidHandle',
        handleID: 'c',
        solid: {
          name: 'c',
          definition: {
            __typename: 'SolidDefinition',
            name: 'c',
          },
          inputs: [
            {
              dependsOn: [
                {
                  solid: {name: 'a'},
                },
              ],
            },
            {
              dependsOn: [
                {
                  solid: {name: 'b'},
                },
              ],
            },
          ],
          outputs: [],
        },
      },
    ],
  }),
};

export const FewParents = () => {
  const [runTags, setRunTags] = React.useState<TokenizingFieldValue[]>([]);
  const [stepQuery, setStepQuery] = React.useState('');
  const partitions: {name: string; runs: PartitionRunMatrixRunFragment[]}[] = [];

  partitions.push({
    name: '2021-01-01',
    runs: [
      {
        __typename: 'PipelineRun',
        id: '00001',
        runId: '00001',
        stats: {
          id: '00001',
          startTime: Date.now(),
          __typename: 'PipelineRunStatsSnapshot',
        },
        stepStats: [
          {
            __typename: 'PipelineRunStepStats',
            stepKey: 'a',
            status: StepEventStatus.SUCCESS,
            materializations: [],
            expectationResults: [],
          },
        ],
        tags: [],
      },
    ],
  });

  return (
    <ApolloTestProvider mocks={PipelineMocks}>
      <PartitionRunMatrix
        pipelineName="TestPipeline"
        partitions={partitions}
        repoAddress={{name: 'Test', location: 'TestLocation'}}
        runTags={runTags}
        setRunTags={setRunTags}
        stepQuery={stepQuery}
        setStepQuery={setStepQuery}
      />
    </ApolloTestProvider>
  );
};
