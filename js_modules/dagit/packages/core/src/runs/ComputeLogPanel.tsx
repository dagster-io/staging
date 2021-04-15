import * as React from 'react';

import {AppContext} from '../app/AppContext';
import {Box} from '../ui/Box';
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
  setComputeLogUrl: (url: string | null) => void;
}

const getComputeLogDownloadURL = (
  rootServerURI: string,
  logData: ComputeLogContentFileFragment | null,
) => {
  const downloadUrl = logData?.downloadUrl;
  if (!downloadUrl) {
    return null;
  }
  const isRelativeUrl = (x?: string) => x && x.startsWith('/');
  return isRelativeUrl(downloadUrl) ? rootServerURI + downloadUrl : downloadUrl;
};

export const ComputeLogPanel: React.FC<RunComputeLogs> = ({
  runId,
  metadata,
  selectedStepKey,
  ioType,
  setComputeLogUrl,
}) => {
  const {rootServerURI, websocketURI} = React.useContext(AppContext);
  const stepKeys = Object.keys(metadata.steps);

  if (!stepKeys.length || !selectedStepKey) {
    return (
      <Box
        flex={{justifyContent: 'center', alignItems: 'center'}}
        style={{flex: 1, height: '100%'}}
      >
        <Spinner purpose="section" />
      </Box>
    );
  }

  return (
    <div style={{flex: 1, minHeight: 0, display: 'flex', flexDirection: 'column'}}>
      <ComputeLogsProvider websocketURI={websocketURI} runId={runId} stepKey={selectedStepKey}>
        {({isLoading, stdout, stderr}) => {
          const logData = ioType === 'stderr' ? stderr : stdout;
          const downloadUrl = getComputeLogDownloadURL(rootServerURI, logData);
          setComputeLogUrl(downloadUrl);
          return (
            <ContentWrapper logData={logData} isLoading={isLoading} downloadUrl={downloadUrl} />
          );
        }}
      </ComputeLogsProvider>
    </div>
  );
};

const ContentWrapper = ({
  isLoading,
  logData,
  downloadUrl,
}: {
  isLoading: boolean;
  logData: ComputeLogContentFileFragment | null;
  downloadUrl: string | null;
}) => {
  const [data, setData] = React.useState<ComputeLogContentFileFragment | null>(null);
  React.useEffect(() => {
    if (!isLoading) {
      setData(logData);
    }
  }, [logData, isLoading]);
  return <ComputeLogContent logData={data} isLoading={isLoading} downloadUrl={downloadUrl} />;
};
