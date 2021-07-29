import {gql} from '@apollo/client';

import {METADATA_ENTRY_FRAGMENT} from '../runs/MetadataEntry';

export const ASSET_VIEW_FRAGMENT = gql`
  fragment AssetViewFragment on Asset {
    id
    key {
      path
    }
    mostRecentMaterialization: assetMaterializations(limit: 1) {
      materializationEvent {
        timestamp
      }
    }
    assetMaterializations(limit: $limit, beforeTimestampMillis: $before) {
      partition
      runOrError {
        ... on PipelineRun {
          id
          runId
          mode
          status
          pipelineName
          pipelineSnapshotId
        }
      }
      materializationEvent {
        runId
        timestamp
        stepKey
        stepStats {
          endTime
          startTime
        }
        materialization {
          label
          description
          metadataEntries {
            ...MetadataEntryFragment
          }
        }
        assetLineage {
          assetKey {
            path
          }
          partitions
        }
      }
    }
  }
  ${METADATA_ENTRY_FRAGMENT}
`;

export const ASSET_QUERY = gql`
  query AssetQuery($assetKey: AssetKeyInput!, $limit: Int!, $before: String) {
    assetOrError(assetKey: $assetKey) {
      ... on Asset {
        ...AssetViewFragment
      }
    }
  }
  ${ASSET_VIEW_FRAGMENT}
  ${METADATA_ENTRY_FRAGMENT}
`;
