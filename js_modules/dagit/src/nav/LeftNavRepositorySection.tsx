import {ApolloConsumer} from '@apollo/client';
import {Colors} from '@blueprintjs/core';
import * as React from 'react';
import {useRouteMatch} from 'react-router';

import {JobsList} from 'src/nav/JobsList';
import {RepositoryContentList} from 'src/nav/RepositoryContentList';
import {RepositoryLocationStateObserver} from 'src/nav/RepositoryLocationStateObserver';
import {RepositoryPicker} from 'src/nav/RepositoryPicker';
import {
  DagsterRepoOption,
  getRepositoryOptionHash,
  WorkspaceContext,
} from 'src/workspace/WorkspaceContext';

export const LAST_REPO_KEY = 'dagit.last-repo';
export const REPO_KEYS = 'dagit.repo-keys';

/**
 * useLocalStorageState vends `[repo, setRepo]` and internally mirrors the current
 * selection into localStorage so that the default selection in new browser windows
 * is the repo currently active in your session.
 */
const useNavVisibleRepos = (options: DagsterRepoOption[]) => {
  const [repoKeys, setRepoKeys] = React.useState<Set<string> | null>(() => {
    const keys = window.localStorage.getItem(REPO_KEYS);
    if (keys) {
      const parsed: string[] = JSON.parse(keys);
      return new Set(parsed);
    }
    const key = window.localStorage.getItem(LAST_REPO_KEY);
    if (key) {
      return new Set([key]);
    }

    return null;
  });

  const toggleRepo = React.useCallback((option: DagsterRepoOption) => {
    const key = getRepositoryOptionHash(option);

    // todo dish: Only allow one repo to be toggled until the UI afforances are in place
    // for multiple selection. After that, start using the `Set` as intended.
    const copy = new Set([key]);
    window.localStorage.setItem(REPO_KEYS, JSON.stringify(Array.from(copy)));
    setRepoKeys(copy);
  }, []);

  const reposForKeys = React.useMemo(() => {
    if (!repoKeys) {
      return options;
    }
    return options.filter((o) => repoKeys.has(getRepositoryOptionHash(o)));
  }, [options, repoKeys]);

  return [reposForKeys, toggleRepo] as [typeof reposForKeys, typeof toggleRepo];
};

export const LeftNavRepositorySection = () => {
  const match = useRouteMatch<
    | {repoPath: string; selector: string; tab: string; rootTab: undefined}
    | {selector: undefined; tab: undefined; rootTab: string}
  >([
    '/workspace/:repoPath/pipelines/:selector/:tab?',
    '/workspace/:repoPath/solids/:selector',
    '/workspace/:repoPath/schedules/:selector',
    '/:rootTab?',
  ]);

  const {allRepos, loading} = React.useContext(WorkspaceContext);
  const [visibleRepos, toggleRepo] = useNavVisibleRepos(allRepos);

  return (
    <div
      className="bp3-dark"
      style={{
        background: `rgba(0,0,0,0.3)`,
        color: Colors.WHITE,
        display: 'flex',
        flex: 1,
        overflow: 'none',
        flexDirection: 'column',
        minHeight: 0,
      }}
    >
      <RepositoryPicker
        loading={loading}
        options={allRepos}
        selected={visibleRepos}
        toggleRepo={toggleRepo}
      />
      <ApolloConsumer>
        {(client) => <RepositoryLocationStateObserver client={client} />}
      </ApolloConsumer>
      {visibleRepos.length ? (
        <div style={{display: 'flex', flex: 1, flexDirection: 'column', minHeight: 0}}>
          <RepositoryContentList {...match?.params} repos={visibleRepos} />
          <JobsList {...match?.params} repos={visibleRepos} />
        </div>
      ) : null}
    </div>
  );
};
