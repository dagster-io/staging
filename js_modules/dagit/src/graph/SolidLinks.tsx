import {Colors} from '@blueprintjs/core';
import {pathVerticalDiagonal} from '@vx/shape';
import * as React from 'react';
import styled from 'styled-components/macro';

import {weakmapMemoize} from 'src/app/Util';
import {
  IFullPipelineLayout,
  IFullSolidLayout,
  ILayoutConnection,
} from 'src/graph/getFullSolidLayout';
import {PipelineGraphSolidFragment} from 'src/graph/types/PipelineGraphSolidFragment';

export type Edge = {a: string; b: string};

const buildSVGPath = pathVerticalDiagonal({
  source: (s: any) => s.source,
  target: (s: any) => s.target,
  x: (s: any) => s.x,
  y: (s: any) => s.y,
});

const buildSVGPaths = weakmapMemoize(
  (connections: ILayoutConnection[], solids: {[name: string]: IFullSolidLayout}) =>
    connections.map(({from, to}) => {
      const sourceOutput = solids[from.solidName].outputs[from.edgeName];
      const targetInput = solids[to.solidName].inputs[to.edgeName];

      return {
        // can also use from.point for the "Dagre" closest point on node
        path: buildSVGPath({
          source: sourceOutput.port,
          target: targetInput.port,
        }),
        sourceOutput,
        targetInput,
        from,
        to,
      };
    }),
);

const outputIsDynamic = (
  solids: PipelineGraphSolidFragment[],
  from: {solidName: string; edgeName: string},
) => {
  const solid = solids.find((s) => s.name === from.solidName);
  const outDef = solid?.definition.outputDefinitions.find((o) => o.name === from.edgeName);
  return outDef?.isDynamic || false;
};

const inputIsDynamicCollect = (
  solids: PipelineGraphSolidFragment[],
  to: {solidName: string; edgeName: string},
) => {
  const solid = solids.find((s) => s.name === to.solidName);
  const inputDef = solid?.inputs.find((o) => o.definition.name === to.edgeName);
  return inputDef?.isDynamicCollect || false;
};

export const SolidLinks = React.memo(
  (props: {
    opacity: number;
    solids: PipelineGraphSolidFragment[];
    layout: IFullPipelineLayout;
    connections: ILayoutConnection[];
    onHighlight: (arr: Edge[]) => void;
  }) => (
    <g opacity={props.opacity}>
      {buildSVGPaths(props.connections, props.layout.solids).map(
        ({path, from, sourceOutput, targetInput, to}, idx) => (
          <g
            key={idx}
            onMouseLeave={() => props.onHighlight([])}
            onMouseEnter={() => props.onHighlight([{a: from.solidName, b: to.solidName}])}
          >
            <StyledPath d={path} />
            {outputIsDynamic(props.solids, from) && (
              <DynamicMarker x={targetInput.layout.x} y={targetInput.layout.y} />
            )}
            {inputIsDynamicCollect(props.solids, to) && (
              <CollectMarker
                x={(targetInput.port.x + sourceOutput.port.x) / 2}
                y={(targetInput.port.y + sourceOutput.port.y) / 2}
              />
            )}
          </g>
        ),
      )}
    </g>
  ),
);

SolidLinks.displayName = 'SolidLinks';

const CollectMarker: React.FunctionComponent<{x: number; y: number}> = ({x, y}) => (
  <g>
    <rect x={x - 35} y={y - 15} width={100} height={30} fill={'#fff'}></rect>
    <text x={x - 35} y={y + 6} style={{fontSize: 20, fontWeight: 600}}>
      collect(✱)
    </text>
  </g>
);

const DynamicMarker: React.FunctionComponent<{x: number; y: number}> = ({x, y}) => (
  <text x={x - 35} y={y + 30} width="40" height="60" style={{fontSize: 30}}>
    ✱
  </text>
);

const StyledPath = styled('path')`
  stroke-width: 6;
  stroke: ${Colors.BLACK};
  fill: none;
`;
