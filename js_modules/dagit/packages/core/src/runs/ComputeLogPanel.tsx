import * as React from 'react';
import styled from 'styled-components/macro';

import {AppContext} from '../app/AppContext';
import {Spinner} from '../ui/Spinner';

import {ComputeLogContent} from './ComputeLogContent';
import {ComputeLogsProvider} from './ComputeLogModal';
import {IRunMetadataDict} from './RunMetadataProvider';

interface RunComputeLogs {
  runId: string;
  metadata: IRunMetadataDict;
  selectedStepKey?: string;
  ioType: 'stdout' | 'stderr';
}

export const ComputeLogPanel: React.FC<RunComputeLogs> = ({
  runId,
  metadata,
  selectedStepKey,
  ioType,
}) => {
  const {rootServerURI, websocketURI} = React.useContext(AppContext);
  const stepKeys = Object.keys(metadata.steps);

  if (!stepKeys.length || !selectedStepKey) {
    return <Spinner purpose="section" />;
  }

  return (
    <div style={{flex: 1, minHeight: 0, display: 'flex', flexDirection: 'column'}}>
      <ComputeLogsProvider websocketURI={websocketURI} runId={runId} stepKey={selectedStepKey}>
        {({isLoading, stdout, stderr}) => {
          if (isLoading) {
            return (
              <LoadingContainer>
                <Spinner purpose="section" />
              </LoadingContainer>
            );
          }

          const logData = ioType === 'stdout' ? stdout : stderr;
          return <ComputeLogContent rootServerURI={rootServerURI} logData={logData} />;
        }}
      </ComputeLogsProvider>
    </div>
  );
};

const LoadingContainer = styled.div`
  height: 100%;
  display: flex;
  justify-content: center;
  align-items: center;
`;
