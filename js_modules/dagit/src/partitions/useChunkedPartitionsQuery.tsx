import gql from 'graphql-tag';
import * as React from 'react';
import {useApolloClient} from 'react-apollo';

import {useRepositorySelector} from 'src/DagsterRepositoryContext';
import {PythonErrorInfo} from 'src/PythonErrorInfo';
import {
  PartitionLongitudinalQuery,
  PartitionLongitudinalQueryVariables,
  PartitionLongitudinalQuery_partitionSetOrError_PartitionSet_partitionsOrError_Partitions_results,
} from 'src/partitions/types/PartitionLongitudinalQuery';
import {RunTable} from 'src/runs/RunTable';

type Partition = PartitionLongitudinalQuery_partitionSetOrError_PartitionSet_partitionsOrError_Partitions_results;

/**
 * This React hook mirrors `useCursorPaginatedQuery` but collects each page of partitions
 * in slices that are smaller than pageSize and cause the results to load incrementally.
 */
export function useChunkedPartitionsQuery(partitionSetName: string, pageSize: number) {
  const {repositoryName, repositoryLocationName} = useRepositorySelector();
  const client = useApolloClient();

  const version = React.useRef(0);
  const [results, setResults] = React.useState<Partition[]>([]);
  const [loading, setLoading] = React.useState(false);

  const [cursorStack, setCursorStack] = React.useState<string[]>([]);
  const [cursor, setCursor] = React.useState<string | undefined>();

  React.useEffect(() => {
    const v = version.current + 1;
    version.current = v;

    setResults([]);
    setLoading(true);

    let c = cursor;
    let accumulated: Partition[] = [];
    const fetchOne = async () => {
      const result = await client.query<
        PartitionLongitudinalQuery,
        PartitionLongitudinalQueryVariables
      >({
        fetchPolicy: 'network-only',
        query: PARTITION_SET_QUERY,
        variables: {
          partitionSetName,
          repositorySelector: {repositoryName, repositoryLocationName},
          reverse: true,
          cursor: c,
          limit: Math.min(2, pageSize - accumulated.length),
        },
      });
      if (version.current !== v) {
        return;
      }
      const fetched = partitionsFromResult(result.data);
      accumulated = [...fetched, ...accumulated];
      if (accumulated.length < pageSize && fetched.length > 0) {
        c = accumulated[0].name;
        fetchOne();
      } else {
        setLoading(false);
      }
      setResults(accumulated);
    };

    fetchOne();
  }, [pageSize, cursor, client, partitionSetName, repositoryName, repositoryLocationName]);

  return {
    loading,
    partitions: [...buildEmptyPartitions(pageSize - results.length), ...results],
    paginationProps: {
      hasPrevPage: cursor !== undefined,
      hasNextPage: results.length === pageSize,
      onPrevPage: () => {
        setResults([]);
        setCursor(cursorStack.pop());
        setCursorStack(cursorStack.slice(0, cursorStack.length - 1));
      },
      onNextPage: () => {
        if (cursor) {
          setCursorStack([...cursorStack, cursor]);
        }
        setResults([]);
        setCursor(results[0].name);
      },
      onReset: () => {
        setResults([]);
        setCursor(undefined);
        setCursorStack([]);
      },
    },
  };
}

function buildEmptyPartitions(count: number) {
  // Note: Partitions don't have any unique keys beside their names, so we use names
  // extensively in our display layer as React keys. To create unique empty partitions
  // we use different numbers of zero-width space characters
  const empty: Partition[] = [];
  for (let ii = 0; ii < count; ii++) {
    empty.push({
      __typename: 'Partition',
      name: `\u200b`.repeat(ii + 1),
      runs: [],
    });
  }
  return empty;
}

function partitionsFromResult(result?: PartitionLongitudinalQuery) {
  if (result?.partitionSetOrError.__typename !== 'PartitionSet') {
    return [];
  }
  if (result.partitionSetOrError.partitionsOrError.__typename !== 'Partitions') {
    return [];
  }
  return result.partitionSetOrError.partitionsOrError.results;
}

const PARTITION_SET_QUERY = gql`
  query PartitionLongitudinalQuery(
    $partitionSetName: String!
    $repositorySelector: RepositorySelector!
    $limit: Int
    $cursor: String
    $reverse: Boolean
  ) {
    partitionSetOrError(
      repositorySelector: $repositorySelector
      partitionSetName: $partitionSetName
    ) {
      ... on PartitionSet {
        name
        partitionsOrError(cursor: $cursor, limit: $limit, reverse: $reverse) {
          ... on Partitions {
            results {
              name
              runs {
                runId
                pipelineName
                tags {
                  key
                  value
                }
                stats {
                  __typename
                  ... on PipelineRunStatsSnapshot {
                    startTime
                    endTime
                    materializations
                  }
                }
                status
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
                ...RunTableRunFragment
              }
            }
          }
          ... on PythonError {
            ...PythonErrorFragment
          }
        }
      }
    }
  }
  ${PythonErrorInfo.fragments.PythonErrorFragment}
  ${RunTable.fragments.RunTableRunFragment}
`;
