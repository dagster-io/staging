import qs from 'qs';
import * as React from 'react';
import {Link} from 'react-router-dom';
import styled from 'styled-components/macro';

import {AssetKey} from './types';

export const AssetLink = ({
  assetKey,
  asOfTimestamp,
}: {
  assetKey: AssetKey;
  asOfTimestamp?: string;
}) => {
  const asOfQuery = asOfTimestamp ? `?${qs.stringify({asOf: asOfTimestamp})}` : '';
  const to = `/instance/assets/${assetKey.path.map(encodeURIComponent).join('/')}${asOfQuery}`;
  return <StyledLink to={to}>{assetKey.path.join(' > ')}</StyledLink>;
};

const StyledLink = styled(Link)`
  text-decoration: underline;
  color: inherit;
  &:hover {
    color: inherit;
  }
`;
