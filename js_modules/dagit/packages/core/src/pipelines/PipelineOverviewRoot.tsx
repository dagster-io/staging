import * as React from 'react';
import {RouteComponentProps, useHistory, useLocation} from 'react-router-dom';

import {useFeatureFlags} from '../app/Flags';
import {useDocumentTitle} from '../hooks/useDocumentTitle';
import {usePipelineSelector} from '../workspace/WorkspaceContext';
import {RepoAddress} from '../workspace/types';
import {workspacePathFromAddress} from '../workspace/workspacePath';

import {PipelineExplorerJobContext} from './PipelineExplorerJobContext';
import {PipelineExplorerContainer} from './PipelineExplorerRoot';
import {
  explorerPathFromString,
  explorerPathToString,
  useStripSnapshotFromPath,
} from './PipelinePathUtils';
import {SidebarJobOverview} from './SidebarJobOverview';

type Props = RouteComponentProps<{0: string}> & {repoAddress: RepoAddress};

export const PipelineOverviewRoot: React.FC<Props> = (props) => {
  const {match, repoAddress} = props;
  const history = useHistory();
  const location = useLocation();
  const explorerPath = explorerPathFromString(match.params['0']);
  const pipelineSelector = usePipelineSelector(repoAddress, explorerPath.pipelineName);
  const {flagPipelineModeTuples} = useFeatureFlags();

  useDocumentTitle(`${flagPipelineModeTuples ? 'Job' : 'Pipeline'}: ${explorerPath.pipelineName}`);
  useStripSnapshotFromPath({pipelinePath: explorerPathToString(explorerPath)});

  return (
    <PipelineExplorerJobContext.Provider
      value={{
        sidebarTab: (
          <SidebarJobOverview repoAddress={repoAddress} pipelineSelector={pipelineSelector} />
        ),
      }}
    >
      <PipelineExplorerContainer
        repoAddress={repoAddress}
        explorerPath={explorerPath}
        onChangeExplorerPath={(path, action) => {
          history[action]({
            search: location.search,
            pathname: workspacePathFromAddress(
              repoAddress,
              `/${flagPipelineModeTuples ? 'jobs' : 'pipelines'}/${explorerPathToString(path)}`,
            ),
          });
        }}
      />
    </PipelineExplorerJobContext.Provider>
  );
};
