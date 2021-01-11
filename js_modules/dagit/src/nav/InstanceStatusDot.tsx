import {gql, useQuery} from '@apollo/client';
import {Colors} from '@blueprintjs/core';
import * as React from 'react';
import styled from 'styled-components';

import {INSTANCE_HEALTH_FRAGMENT} from 'src/instance/InstanceHealthFragment';
import {InstanceStatusDotQuery} from 'src/nav/types/InstanceStatusDotQuery';
import {WorkspaceContext} from 'src/workspace/WorkspaceContext';

export const InstanceStatusDot = React.memo(() => {
  const {loading, locations} = React.useContext(WorkspaceContext);

  const {loading: healthLoading, data: healthData} = useQuery<InstanceStatusDotQuery>(
    INSTANCE_STATUS_DOT_QUERY,
    {
      fetchPolicy: 'cache-and-network',
      pollInterval: 15 * 1000,
    },
  );

  const repoErrors = locations.some((node) => node.__typename === 'RepositoryLocationLoadFailure');
  const daemonErrors = healthData?.instance.daemonHealth.allDaemonStatuses.some(
    (daemon) => !daemon.healthy && daemon.required,
  );

  const values = () => {
    if (loading || (healthLoading && !healthData)) {
      return {color: Colors.GRAY5, title: 'Loading…'};
    }

    if (repoErrors || daemonErrors) {
      return {color: Colors.GOLD4, title: 'Warnings found'};
    }

    return {color: Colors.GREEN4, title: 'No issues found'};
  };

  const {color, title} = values();

  return <StatusDot color={color} title={title} />;
});

const INSTANCE_STATUS_DOT_QUERY = gql`
  query InstanceStatusDotQuery {
    instance {
      ...InstanceHealthFragment
    }
  }
  ${INSTANCE_HEALTH_FRAGMENT}
`;

const StatusDot = styled(({color, ...rest}) => <div {...rest} />)<{color: string}>`
  width: 9px;
  height: 9px;
  border-radius: 4.5px;
  background-color: ${({color}) => color};
`;
