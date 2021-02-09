import {gql, useQuery} from '@apollo/client';
import {Colors} from '@blueprintjs/core';
import * as React from 'react';
import styled from 'styled-components';

import {InstanceDetailSummaryQuery} from 'src/nav/types/InstanceDetailSummaryQuery';

export const VersionNumber = () => {
  const {data} = useQuery<InstanceDetailSummaryQuery>(VERSION_NUMBER_QUERY, {
    fetchPolicy: 'cache-and-network',
  });

  return <Version>{data?.version}</Version>;
};

const Version = styled.div`
  color: ${Colors.GRAY3};
  font-size: 11px;
`;

const VERSION_NUMBER_QUERY = gql`
  query VersionNumberQuery {
    version
  }
`;
