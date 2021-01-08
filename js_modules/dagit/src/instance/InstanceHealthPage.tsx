import {QueryResult} from '@apollo/client';
import {Spinner} from '@blueprintjs/core';
import * as React from 'react';

import {DaemonList} from 'src/instance/DaemonList';
import {InstanceHealthQuery} from 'src/instance/types/InstanceHealthQuery';
import {Box} from 'src/ui/Box';
import {Group} from 'src/ui/Group';
import {Subheading} from 'src/ui/Text';
import {RepositoryLocationsList} from 'src/workspace/RepositoryLocationsList';

interface Props {
  queryData: QueryResult<InstanceHealthQuery>;
}

export const InstanceHealthPage = (props: Props) => {
  const {queryData} = props;
  const {data, loading} = queryData;

  const content = () => {
    if (loading && !data?.instance) {
      return (
        <Box padding={24}>
          <Spinner size={16} />
        </Box>
      );
    }

    return data?.instance ? <DaemonList daemonHealth={data.instance.daemonHealth} /> : null;
  };

  return (
    <Group direction="column" spacing={32}>
      <Group direction="column" spacing={16}>
        <Subheading id="repository-locations">Repository locations</Subheading>
        <RepositoryLocationsList />
      </Group>
      <Group direction="column" spacing={16}>
        <Subheading>Daemon statuses</Subheading>
        {content()}
      </Group>
    </Group>
  );
};
