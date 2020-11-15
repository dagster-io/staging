import {Colors, Button} from '@blueprintjs/core';
import {uniq, flatMap} from 'lodash';
import * as React from 'react';
import {Link} from 'react-router-dom';
import styled from 'styled-components/macro';

import {Timestamp} from 'src/TimeComponents';
import {AssetNumericHistoricalData} from 'src/assets/AssetView';
import {Sparkline} from 'src/assets/Sparkline';
import {
  AssetQuery_assetOrError_Asset_assetMaterializations,
  AssetQuery_assetOrError_Asset_assetMaterializations_materializationEvent_materialization_metadataEntries,
} from 'src/assets/types/AssetQuery';
import {useViewport} from 'src/gaant/useViewport';
import {
  GridColumn,
  GridScrollContainer,
  GridFloatingContainer,
  LeftLabel,
} from 'src/partitions/RunMatrixUtils';
import {MetadataEntry} from 'src/runs/MetadataEntry';
import {titleForRun} from 'src/runs/RunUtils';
import {FontFamily} from 'src/ui/styles';

const COL_WIDTH = 120;

const OVERSCROLL = 150;

interface AssetMaterializationMatrixProps {
  materializations: AssetQuery_assetOrError_Asset_assetMaterializations[];
  isPartitioned: boolean;
  xAxis: 'time' | 'partition';
  xHover: number | string | null;
  onHoverX: (x: number | string | null) => void;
  graphDataByMetadataLabel: AssetNumericHistoricalData;
  graphedLabels: string[];
  setGraphedLabels: (labels: string[]) => void;
}

function xForAssetMaterialization(
  am: AssetQuery_assetOrError_Asset_assetMaterializations,
  xAxis: 'time' | 'partition',
) {
  return xAxis === 'time' ? Number(am.materializationEvent.timestamp) : am.partition;
}

export const AssetMaterializationMatrix: React.FunctionComponent<AssetMaterializationMatrixProps> = ({
  materializations,
  isPartitioned,
  xAxis,
  xHover,
  onHoverX,
  graphDataByMetadataLabel,
  graphedLabels,
  setGraphedLabels,
}) => {
  const {viewport, containerProps, onMoveToViewport} = useViewport();
  const [hoveredLabel, setHoveredLabel] = React.useState<string>('');

  const visibleRangeStart = Math.max(0, Math.floor((viewport.left - OVERSCROLL) / COL_WIDTH));
  const visibleCount = Math.ceil((viewport.width + OVERSCROLL * 2) / COL_WIDTH);
  const visible = materializations.slice(visibleRangeStart, visibleRangeStart + visibleCount);

  const lastXHover = React.useRef<string | number | null>(null);
  React.useEffect(() => {
    if (lastXHover.current === xHover) {
      return;
    }
    if (xHover !== null) {
      const idx = materializations.findIndex((m) => xForAssetMaterialization(m, xAxis) === xHover);
      if ((idx !== -1 && idx < visibleRangeStart) || idx > visibleRangeStart + visibleCount) {
        onMoveToViewport({left: idx * COL_WIDTH - (viewport.width - COL_WIDTH) / 2, top: 0}, false);
      }
    }
    lastXHover.current = xHover;
  });

  const metadataLabels = uniq(
    flatMap(materializations, (m) =>
      m.materializationEvent.materialization.metadataEntries.map((e) => e.label),
    ),
  );

  return (
    <PartitionRunMatrixContainer>
      <div style={{position: 'relative', display: 'flex', border: `1px solid ${Colors.GRAY5}`}}>
        <GridFloatingContainer floating={true} style={{width: 300}}>
          <GridColumn disabled style={{width: 300, overflow: 'hidden'}}>
            <LeftLabel style={{paddingLeft: 6, color: Colors.GRAY2}}>Run</LeftLabel>
            {isPartitioned && (
              <LeftLabel style={{paddingLeft: 6, color: Colors.GRAY2}}>Partition</LeftLabel>
            )}
            <LeftLabel
              style={{
                paddingLeft: 6,
                color: Colors.GRAY2,
                borderBottom: `1px solid ${Colors.LIGHT_GRAY1}`,
              }}
            >
              Timestamp
            </LeftLabel>
            {metadataLabels.map((label) => (
              <LeftLabel
                key={label}
                hovered={label === hoveredLabel}
                data-tooltip={label}
                style={{display: 'flex'}}
              >
                <div style={{width: 149}}>
                  <Button
                    minimal
                    small
                    disabled={!graphDataByMetadataLabel[label]}
                    onClick={() =>
                      setGraphedLabels(
                        graphedLabels.includes(label)
                          ? graphedLabels.filter((l) => l !== label)
                          : [...graphedLabels, label],
                      )
                    }
                    icon={
                      graphDataByMetadataLabel[label]
                        ? graphedLabels.includes(label)
                          ? 'eye-open'
                          : 'eye-off'
                        : 'blank'
                    }
                  />
                  {label}
                </div>
                {graphDataByMetadataLabel[label] && (
                  <Sparkline data={graphDataByMetadataLabel[label]} width={150} height={23} />
                )}
              </LeftLabel>
            ))}
          </GridColumn>
        </GridFloatingContainer>
        <GridScrollContainer {...containerProps}>
          <div
            style={{
              width: materializations.length * COL_WIDTH,
              position: 'relative',
              height: 80,
            }}
          >
            {visible.map((assetMaterialization, idx) => {
              const {materializationEvent, partition} = assetMaterialization;
              const x = xForAssetMaterialization(assetMaterialization, xAxis);
              const runId =
                assetMaterialization.runOrError.__typename === 'PipelineRun'
                  ? assetMaterialization.runOrError.runId
                  : '';

              return (
                <GridColumn
                  key={materializationEvent.timestamp}
                  onMouseEnter={() => onHoverX(x)}
                  hovered={xHover === x}
                  style={{
                    width: COL_WIDTH,
                    position: 'absolute',
                    zIndex: visible.length - idx,
                    left: (idx + visibleRangeStart) * COL_WIDTH,
                  }}
                >
                  <div className={`cell`} style={{fontFamily: FontFamily.monospace}}>
                    <Link
                      style={{marginRight: 5}}
                      to={`/instance/runs/${runId}?timestamp=${materializationEvent.timestamp}`}
                    >
                      {titleForRun({runId})}
                    </Link>
                  </div>
                  {isPartitioned && <div className={`cell`}>{partition}</div>}
                  <div className={`cell`} style={{borderBottom: `1px solid ${Colors.LIGHT_GRAY1}`}}>
                    <Timestamp ms={Number(materializationEvent.timestamp)} />
                  </div>
                  {metadataLabels.map((label) => {
                    const entry = materializationEvent.materialization.metadataEntries.find(
                      (m) => m.label === label,
                    );
                    return (
                      <div
                        key={label}
                        className={`cell`}
                        style={{width: COL_WIDTH}}
                        data-tooltip={plaintextFor(entry) || undefined}
                        onMouseEnter={() => setHoveredLabel(label)}
                        onMouseLeave={() => setHoveredLabel('')}
                      >
                        {entry ? (
                          <MetadataEntry entry={entry} />
                        ) : (
                          <span style={{opacity: 0.3}}> - </span>
                        )}
                      </div>
                    );
                  })}
                </GridColumn>
              );
            })}
          </div>
        </GridScrollContainer>
      </div>
      {visible.length === 0 && <EmptyMessage>No data to display.</EmptyMessage>}
    </PartitionRunMatrixContainer>
  );
};

const plaintextFor = (
  entry:
    | AssetQuery_assetOrError_Asset_assetMaterializations_materializationEvent_materialization_metadataEntries
    | undefined,
) => {
  if (!entry) {
    return '';
  }
  if (entry.__typename === 'EventFloatMetadataEntry') {
    return entry.floatValue;
  } else if (entry.__typename === 'EventIntMetadataEntry') {
    return entry.intValue.toLocaleString();
  } else if (entry.__typename === 'EventPathMetadataEntry') {
    return entry.path;
  } else if (entry.__typename === 'EventTextMetadataEntry') {
    return entry.text;
  } else if (entry.__typename === 'EventUrlMetadataEntry') {
    return entry.url;
  } else {
    return '';
  }
};

const EmptyMessage = styled.div`
  padding: 20px;
  text-align: center;
`;

const PartitionRunMatrixContainer = styled.div`
  display: block;
  margin-bottom: 20px;
`;
