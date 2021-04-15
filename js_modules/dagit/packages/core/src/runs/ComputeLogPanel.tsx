import * as React from 'react';

import {AppContext} from '../app/AppContext';
import {Spinner} from '../ui/Spinner';

import {ComputeLogContent} from './ComputeLogContent';
import {ComputeLogsProvider} from './ComputeLogProvider';
import {IRunMetadataDict} from './RunMetadataProvider';
import {ComputeLogContentFileFragment} from './types/ComputeLogContentFileFragment';

interface RunComputeLogs {
  runId: string;
  metadata: IRunMetadataDict;
  selectedStepKey?: string;
  ioType: string;
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
          return (
            <ContentWrapper
              rootServerURI={rootServerURI}
              logData={ioType === 'stderr' ? stderr : stdout}
              isLoading={isLoading}
            />
          );
        }}
      </ComputeLogsProvider>
    </div>
  );
};

const ContentWrapper = ({
  rootServerURI,
  isLoading,
  logData,
}: {
  isLoading: boolean;
  logData: ComputeLogContentFileFragment | null;
  rootServerURI: string;
}) => {
  const [data, setData] = React.useState<ComputeLogContentFileFragment | null>(null);
  React.useEffect(() => {
    if (logData && !isLoading) {
      setData(logData);
    }
  }, [logData, isLoading]);
  return <ComputeLogContent rootServerURI={rootServerURI} logData={data} isLoading={isLoading} />;
};
