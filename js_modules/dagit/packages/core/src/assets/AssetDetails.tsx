import {useQuery} from '@apollo/client';
import {Button, Colors, Icon, Dialog} from '@blueprintjs/core';
import qs from 'qs';
import * as React from 'react';
import {Link, useHistory} from 'react-router-dom';

import {Timestamp} from '../app/time/Timestamp';
import {PartitionsBackfillPartitionSelector} from '../partitions/PartitionsBackfill';
import {PipelineReference} from '../pipelines/PipelineReference';
import {MetadataEntry} from '../runs/MetadataEntry';
import {DagsterTag} from '../runs/RunTag';
import {titleForRun} from '../runs/RunUtils';
import {Box} from '../ui/Box';
import {Group} from '../ui/Group';
import {MetadataTable} from '../ui/MetadataTable';
import {Spinner} from '../ui/Spinner';
import {Subheading} from '../ui/Text';
import {FontFamily} from '../ui/styles';
import {workspacePathFromAddress} from '../workspace/workspacePath';

import {AssetLineageElements} from './AssetLineageElements';
import {ASSET_QUERY} from './queries';
import {AssetKey} from './types';
import {
  AssetQuery,
  AssetQueryVariables,
  AssetQuery_assetOrError_Asset_assetMaterializations_runOrError_PipelineRun,
} from './types/AssetQuery';

interface Props {
  assetKey: AssetKey;
  asOf: string | null;
}

export const AssetDetails: React.FC<Props> = ({assetKey, asOf}) => {
  const {data, loading} = useQuery<AssetQuery, AssetQueryVariables>(ASSET_QUERY, {
    variables: {
      assetKey: {path: assetKey.path},
      limit: 1,
      before: asOf,
    },
  });
  const history = useHistory();
  const [showBackfillSetup, setShowBackfillSetup] = React.useState(false);

  if (loading) {
    return (
      <Group direction="column" spacing={8}>
        <Subheading>{'Details'}</Subheading>
        <Box padding={{vertical: 20}}>
          <Spinner purpose="section" />
        </Box>
      </Group>
    );
  }

  const assetOrError = data?.assetOrError;

  if (!assetOrError || assetOrError.__typename !== 'Asset') {
    return null;
  }

  const latest = assetOrError.assetMaterializations[0];
  const latestEvent = latest && latest.materializationEvent;
  const latestAssetLineage = latestEvent && latestEvent.assetLineage;
  const latestRun =
    latest && latest.runOrError.__typename === 'PipelineRun' ? latest.runOrError : null;
  const backfillContext = backfillContextFromRun(latestRun);

  const isPartitioned = !!(
    data?.assetOrError?.__typename === 'Asset' &&
    data.assetOrError.assetMaterializations[0].partition
  );

  return (
    <Group direction="column" spacing={8}>
      <Subheading>{isPartitioned ? 'Latest Materialized Partition' : 'Details'}</Subheading>
      {backfillContext && (
        <Dialog
          canEscapeKeyClose={false}
          canOutsideClickClose={false}
          onClose={() => setShowBackfillSetup(false)}
          style={{width: 800, background: Colors.WHITE}}
          title={`Launch ${backfillContext.partitionSet} backfill`}
          isOpen={showBackfillSetup}
        >
          {showBackfillSetup && (
            <PartitionsBackfillPartitionSelector
              repoAddress={backfillContext.repoAddress}
              partitionSetName={backfillContext.partitionSet}
              pipelineName={backfillContext.pipelineName}
              onSubmit={() => {}}
              onLaunch={(backfillId) => {
                history.push(
                  workspacePathFromAddress(
                    backfillContext.repoAddress,
                    `/pipelines/${backfillContext.pipelineName}/partitions?${qs.stringify({
                      partitionSet: backfillContext.partitionSet,
                      q: `${DagsterTag.Backfill}=${backfillId}`,
                    })}`,
                  ),
                );
              }}
            />
          )}
        </Dialog>
      )}
      <MetadataTable
        rows={[
          {
            key: 'Latest materialization from',
            value: latestRun ? (
              <div style={{lineHeight: '22px'}}>
                <div>
                  {'Run '}
                  <Link
                    style={{fontFamily: FontFamily.monospace}}
                    to={`/instance/runs/${latestEvent.runId}?timestamp=${latestEvent.timestamp}`}
                  >
                    {titleForRun({runId: latestEvent.runId})}
                  </Link>
                </div>
                <div style={{paddingLeft: 10, alignItems: 'flex-start'}}>
                  <Icon
                    icon="diagram-tree"
                    color={Colors.GRAY2}
                    iconSize={12}
                    style={{position: 'relative', top: -2, paddingRight: 5}}
                  />
                  <PipelineReference
                    pipelineName={latestRun.pipelineName}
                    pipelineHrefContext="repo-unknown"
                    snapshotId={latestRun.pipelineSnapshotId}
                    mode={latestRun.mode}
                  />
                  {backfillContext && (
                    <Button
                      small
                      onClick={() => setShowBackfillSetup(true)}
                      style={{marginLeft: 7, marginTop: -3}}
                    >
                      Launch Backfill
                    </Button>
                  )}
                </div>
                <div style={{paddingLeft: 10}}>
                  <Icon
                    icon="git-commit"
                    color={Colors.GRAY2}
                    iconSize={12}
                    style={{position: 'relative', top: -2, paddingRight: 5}}
                  />
                  <Link
                    to={`/instance/runs/${latestRun.runId}?${qs.stringify({
                      selection: latest.materializationEvent.stepKey,
                      logs: `step:${latest.materializationEvent.stepKey}`,
                    })}`}
                  >
                    {latest.materializationEvent.stepKey}
                  </Link>
                </div>
              </div>
            ) : (
              'No materialization events'
            ),
          },
          latest.partition
            ? {
                key: 'Latest partition',
                value: latest ? latest.partition : 'No materialization events',
              }
            : undefined,
          {
            key: 'Latest timestamp',
            value: latestEvent ? (
              <Timestamp timestamp={{ms: Number(latestEvent.timestamp)}} />
            ) : (
              'No materialization events'
            ),
          },
          latestAssetLineage.length > 0
            ? {
                key: 'Latest parent assets',
                value: (
                  <AssetLineageElements
                    elements={latestAssetLineage}
                    timestamp={latestEvent.timestamp}
                  />
                ),
              }
            : undefined,
          ...latestEvent?.materialization.metadataEntries.map((entry) => ({
            key: entry.label,
            value: <MetadataEntry entry={entry} expandSmallValues={true} />,
          })),
        ].filter(Boolean)}
      />
    </Group>
  );
};

function backfillContextFromRun(
  run: AssetQuery_assetOrError_Asset_assetMaterializations_runOrError_PipelineRun | null,
) {
  if (!run || !run.repositoryOrigin) {
    return null;
  }
  const partitionSet = run.tags.find((t) => t.key === DagsterTag.PartitionSet)?.value;
  if (!partitionSet) {
    return null;
  }

  const repoAddress = {
    name: run.repositoryOrigin.repositoryName,
    location: run.repositoryOrigin.repositoryLocationName,
  };

  return {
    run,
    partitionSet,
    pipelineName: run.pipelineName,
    repoAddress,
  };
}
