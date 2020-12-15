import {gql} from '@apollo/client';
import {Button, Classes, Colors, Dialog} from '@blueprintjs/core';
import * as React from 'react';
import {Link} from 'react-router-dom';

import {HighlightedCodeBlock} from 'src/HighlightedCodeBlock';
import {browserTimezone, timestampToString} from 'src/TimeComponents';
import {RunTags} from 'src/runs/RunTags';
import {TimeElapsed} from 'src/runs/TimeElapsed';
import {RunFragment} from 'src/runs/types/RunFragment';
import {Box} from 'src/ui/Box';
import {Group} from 'src/ui/Group';
import {MetadataTable} from 'src/ui/MetadataTable';
import {FontFamily} from 'src/ui/styles';

interface Props {
  loading: boolean;
  run: RunFragment | undefined;
}

export const RunDetails = (props: Props) => {
  const {loading, run} = props;
  const [showDialog, setShowDialog] = React.useState(false);

  // todo dish: Make sure these values make sense.
  const startedValue = () => {
    if (loading) {
      return <div style={{color: Colors.GRAY3}}>Loading…</div>;
    }
    if (run?.stats.__typename === 'PipelineRunStatsSnapshot' && run.stats.startTime) {
      return timestampToString(
        {unix: run.stats.startTime, format: 'MMM D, YYYY, h:mm:ss A'},
        browserTimezone(),
      );
    }
    if (run?.status === 'QUEUED') {
      return 'Queued';
    }
    return 'None';
  };

  const endedValue = () => {
    if (loading) {
      return <div style={{color: Colors.GRAY3}}>Loading…</div>;
    }
    if (run?.stats.__typename === 'PipelineRunStatsSnapshot' && run.stats.endTime) {
      return timestampToString(
        {unix: run.stats.endTime, format: 'MMM D, YYYY, h:mm:ss A'},
        browserTimezone(),
      );
    }
    return 'None';
  };

  const elapsed = () => {
    if (loading) {
      return <div style={{color: Colors.GRAY3}}>Loading…</div>;
    }
    if (run?.stats.__typename === 'PipelineRunStatsSnapshot') {
      return <TimeElapsed startUnix={run.stats.startTime} endUnix={run.stats.endTime} />;
    }
    return 'None';
  };

  return (
    <Box
      padding={{vertical: 8, horizontal: 12}}
      border={{side: 'bottom', width: 1, color: Colors.GRAY5}}
      flex={{justifyContent: 'space-between'}}
    >
      <Group direction="row" spacing={12}>
        <Box padding={{right: 12}} border={{side: 'right', width: 1, color: Colors.LIGHT_GRAY3}}>
          <MetadataTable
            spacing={0}
            rows={[
              {
                key: 'Pipeline',
                value: (
                  <Group direction="row" spacing={8}>
                    <div>{run?.pipeline.name}</div>
                    <div style={{fontFamily: FontFamily.monospace}}>
                      (
                      <Link
                        to={`/instance/snapshots/${run?.pipeline.name}@${run?.pipelineSnapshotId}`}
                      >
                        {run?.pipelineSnapshotId?.slice(0, 8)}
                      </Link>
                      )
                    </div>
                  </Group>
                ),
              },
              {
                key: 'Duration',
                value: elapsed(),
              },
            ]}
          />
        </Box>
        <Box padding={{right: 12}}>
          <MetadataTable
            spacing={0}
            rows={[
              {
                key: 'Started',
                value: startedValue(),
              },
              {
                key: 'Ended',
                value: endedValue(),
              },
            ]}
          />
        </Box>
      </Group>
      {run ? (
        <div>
          <Button onClick={() => setShowDialog(true)}>View tags and configuration</Button>
          <Dialog
            isOpen={showDialog}
            onClose={() => setShowDialog(false)}
            style={{width: '800px'}}
            title="Run configuration"
          >
            <div className={Classes.DIALOG_BODY}>
              <Group direction="column" spacing={20}>
                <Group direction="column" spacing={12}>
                  <div style={{fontSize: '16px', fontWeight: 600}}>Tags</div>
                  <div>
                    <RunTags tags={run.tags} />
                  </div>
                </Group>
                <Group direction="column" spacing={12}>
                  <div style={{fontSize: '16px', fontWeight: 600}}>Config</div>
                  <HighlightedCodeBlock value={run?.runConfigYaml || ''} language="yaml" />
                </Group>
              </Group>
            </div>
            <div className={Classes.DIALOG_FOOTER}>
              <div className={Classes.DIALOG_FOOTER_ACTIONS}>
                <Button onClick={() => setShowDialog(false)}>OK</Button>
              </div>
            </div>
          </Dialog>
        </div>
      ) : null}
    </Box>
  );
};

export const RUN_DETAILS_FRAGMENT = gql`
  fragment RunDetailsFragment on PipelineRun {
    id
    stats {
      ... on PipelineRunStatsSnapshot {
        id
        endTime
        startTime
      }
    }
    status
  }
`;
