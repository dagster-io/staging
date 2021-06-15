import React from 'react';
import {Link, RouteComponentProps} from 'react-router-dom';

import {RepositoryLink} from '../nav/RepositoryLink';
import {PipelineExplorerRegexRoot} from '../pipelines/PipelineExplorerRoot';
import {explorerPathFromString} from '../pipelines/PipelinePathUtils';
import {Group} from '../ui/Group';
import {PageHeader} from '../ui/PageHeader';
import {Heading} from '../ui/Text';

import {RepoAddress} from './types';
import {workspacePathFromAddress} from './workspacePath';

interface Props extends RouteComponentProps {
  repoAddress: RepoAddress;
}

export const GraphRoot: React.FC<Props> = (props) => {
  const {repoAddress} = props;
  const path = explorerPathFromString(props.match.params[0]);
  return (
    <div style={{height: '100%', display: 'flex', flexDirection: 'column'}}>
      <div style={{padding: 20, borderBottom: '1px solid #ccc'}}>
        <Group direction="column" spacing={20}>
          <PageHeader
            title={<Heading>{path.pipelineName.replace('_pipeline', '')}</Heading>}
            description={
              <>
                <Link to={workspacePathFromAddress(repoAddress, '/graphs')}>Graph</Link> in{' '}
                <RepositoryLink repoAddress={repoAddress} />
              </>
            }
            icon="th"
          />
        </Group>
      </div>
      <div style={{position: 'relative', minHeight: 0, flex: 1}}>
        <PipelineExplorerRegexRoot {...props} repoAddress={repoAddress} />
      </div>
    </div>
  );
};
