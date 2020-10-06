import {Button, ButtonGroup} from '@blueprintjs/core';
import * as React from 'react';
import styled from 'styled-components/macro';

import {CursorHistoryControls, CursorPaginationProps} from 'src/CursorControls';

interface PartitionPageControlsProps {
  pageSize: number | undefined;
  paginationProps: CursorPaginationProps;
  setPageSize: React.Dispatch<React.SetStateAction<number | undefined>>;
}

export const PartitionPageControls: React.FunctionComponent<PartitionPageControlsProps> = ({
  pageSize,
  setPageSize,
  paginationProps,
}) => (
  <PartitionPagerContainer>
    <ButtonGroup>
      {[7, 30, 120].map((size) => (
        <Button
          key={size}
          active={!paginationProps.hasPrevPage && pageSize === size}
          onClick={() => setPageSize(size)}
        >
          Last {size}
        </Button>
      ))}
      <Button active={pageSize === undefined} onClick={() => setPageSize(undefined)}>
        All
      </Button>
    </ButtonGroup>
    <CursorHistoryControls {...paginationProps} />
  </PartitionPagerContainer>
);

const PartitionPagerContainer = styled.div`
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin: 10px 0;
`;
