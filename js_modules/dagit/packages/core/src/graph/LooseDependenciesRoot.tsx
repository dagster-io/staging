import {gql, useQuery} from '@apollo/client';
import {NonIdealState, Menu, Button, Checkbox, Colors, Icon, Popover} from '@blueprintjs/core';
import {IconNames} from '@blueprintjs/icons';
import {pathVerticalDiagonal} from '@vx/shape';
import * as dagre from 'dagre';
import Fuse from 'fuse.js';
import * as React from 'react';
import styled from 'styled-components/macro';

import {PYTHON_ERROR_FRAGMENT} from '../app/PythonErrorInfo';
import {SearchResults} from '../search/SearchResults';
import {SearchResult, SearchResultType} from '../search/types';
import {useRepoSearch} from '../search/useRepoSearch';
import {Box} from '../ui/Box';
import {Loading} from '../ui/Loading';
import {MainContent} from '../ui/MainContent';
import {Spinner} from '../ui/Spinner';
import {FontFamily} from '../ui/styles';

import {
  IEdge,
  getNodeDimensions,
  NodeType,
  ILooseDepsNode,
  NodeSelection,
  LooseDependencyGraphData,
  LooseDependencyNode,
  LOOSE_DEPENDENCY_ASSET_NODE_FRAGMENT,
  LOOSE_DEPENDENCY_SENSOR_NODE_FRAGMENT,
  LOOSE_DEPENDENCY_SCHEDULE_NODE_FRAGMENT,
  LOOSE_DEPENDENCY_PIPELINE_NODE_FRAGMENT,
} from './LooseDependencyNode';
import {SVGViewport} from './SVGViewport';
import {
  LooseDependencyPipelineEdgeQuery,
  LooseDependencyPipelineEdgeQuery_repositoriesOrError_RepositoryConnection_nodes,
} from './types/LooseDependencyPipelineEdgeQuery';
import {LooseDependencyPipelineNodeQuery} from './types/LooseDependencyPipelineNodeQuery';

type Repository = LooseDependencyPipelineEdgeQuery_repositoriesOrError_RepositoryConnection_nodes;

const SEARCH_TYPES = {
  [SearchResultType.Asset]: true,
  [SearchResultType.Pipeline]: true,
  [SearchResultType.Schedule]: true,
  [SearchResultType.Sensor]: true,
};

interface LooseDependencyGraphLayout {
  nodes: ILooseDepsNode[];
  edges: IEdge[];
  width: number;
  height: number;
}

const buildLooseDependencyGraphData = (repositories: Repository[], includeHistorical: boolean) => {
  const nodes = {};
  const downstream = {};
  const upstream = {};

  repositories.forEach((repository) => {
    repository.pipelines.forEach((pipeline) => {
      const pipeline_id = `${repository.name}:${pipeline.name}`;
      nodes[pipeline_id] = {
        id: pipeline_id,
        nodeType: NodeType.pipeline,
        name: pipeline.name,
        repositoryName: repository.name,
        repositoryLocationName: repository.location.name,
      };

      if (includeHistorical && pipeline.runs) {
        pipeline.runs.forEach((run) => {
          run.assets.forEach((asset) => {
            const assetKeyJson = JSON.stringify(asset.key.path);
            nodes[assetKeyJson] = {
              id: assetKeyJson,
              nodeType: NodeType.asset,
              name: assetKeyJson,
            };
            downstream[pipeline_id] = {
              ...(downstream[pipeline_id] || {}),
              [assetKeyJson]: false, // historical mapping
            };
            upstream[assetKeyJson] = {
              ...(upstream[assetKeyJson] || {}),
              [pipeline_id]: false, // historical mapping
            };
          });
        });
      }

      pipeline.solids.forEach((solid) => {
        solid.outputs.forEach((output) => {
          if (output.definition.asset) {
            const assetKeyJson = JSON.stringify(output.definition.asset.key.path);
            nodes[assetKeyJson] = {
              id: assetKeyJson,
              nodeType: NodeType.asset,
              name: assetKeyJson,
            };
            downstream[pipeline_id] = {
              ...(downstream[pipeline_id] || {}),
              [assetKeyJson]: true,
            };
            upstream[assetKeyJson] = {
              ...(upstream[assetKeyJson] || {}),
              [pipeline_id]: true,
            };
          }
        });
      });

      pipeline.sensors.forEach((sensor) => {
        const sensor_id = `${repository.name}:${sensor.name}`;
        nodes[sensor_id] = {
          id: sensor_id,
          nodeType: NodeType.sensor,
          name: sensor.name,
          repositoryName: repository.name,
          repositoryLocationName: repository.location.name,
        };
        downstream[sensor_id] = {...(downstream[sensor_id] || {}), [pipeline_id]: true};
        upstream[pipeline_id] = {...(upstream[pipeline_id] || {}), [sensor_id]: true};
        sensor.assets &&
          sensor.assets.forEach((asset) => {
            const assetKeyJson = JSON.stringify(asset.key.path);
            nodes[assetKeyJson] = {
              id: assetKeyJson,
              nodeType: NodeType.asset,
              name: assetKeyJson,
            };
            downstream[assetKeyJson] = {...(downstream[assetKeyJson] || {}), [sensor_id]: true};
            upstream[sensor_id] = {...(upstream[sensor_id] || {}), [assetKeyJson]: true};
          });
      });

      pipeline.schedules.forEach((schedule) => {
        const schedule_id = `${repository.name}:${schedule.name}`;
        nodes[schedule_id] = {
          id: schedule_id,
          nodeType: NodeType.schedule,
          name: schedule.name,
          repositoryName: repository.name,
          repositoryLocationName: repository.location.name,
        };
        downstream[schedule_id] = {...(downstream[schedule_id] || {}), [pipeline_id]: true};
        upstream[pipeline_id] = {...(upstream[pipeline_id] || {}), [schedule_id]: true};
      });
    });
  });

  return {nodes, downstream, upstream};
};

const graphHasCycles = (_graphData: LooseDependencyGraphData) => {
  const nodes = new Set(Object.keys(_graphData.nodes));
  const search = (stack: string[], node: string): boolean => {
    if (stack.indexOf(node) !== -1) {
      return true;
    }
    if (nodes.delete(node) === true) {
      const nextStack = stack.concat(node);
      return Object.keys(_graphData.downstream[node] || {}).some((nextNode) =>
        search(nextStack, nextNode),
      );
    }
    return false;
  };
  let hasCycles = false;
  while (nodes.size !== 0) {
    hasCycles = hasCycles || search([], nodes.values().next().value);
  }
  return hasCycles;
};

export const LooseDependenciesRoot = () => {
  const [nodeSelection, setSelectedNode] = React.useState<NodeSelection | undefined>();
  const [includeHistorical, setIncludeHistorical] = React.useState<boolean>(false);
  const {data, loading} = useQuery<LooseDependencyPipelineEdgeQuery>(
    LOOSE_DEPENDENCY_PIPELINE_EDGE_QUERY,
    {variables: {includeHistorical}},
  );

  if (!data || loading) {
    return (
      <MainContent style={{display: 'flex', alignItems: 'center', justifyContent: 'center'}}>
        <Spinner purpose="page" />
      </MainContent>
    );
  }

  const {repositoriesOrError} = data;

  if (repositoriesOrError.__typename !== 'RepositoryConnection') {
    return <NonIdealState icon={IconNames.ERROR} title="GraphQL Error - see console for details" />;
  }

  const items: NodeSelection[] = [];
  repositoriesOrError.nodes.forEach((repository) => {
    repository.pipelines.forEach((pipeline) => {
      items.push({
        nodeType: NodeType.pipeline,
        id: `${repository.name}:${pipeline.name}`,
      });
    });
  });

  const graphData = buildLooseDependencyGraphData(repositoriesOrError.nodes, includeHistorical);
  const hasCycles = graphHasCycles(graphData);
  const layout = nodeSelection
    ? layoutGraphDependencies(graphData, nodeSelection)
    : hasCycles
    ? null
    : layoutFullGraph(graphData);

  const _searchItemToNode = (item: SearchResult) => {
    switch (item.type) {
      case SearchResultType.Asset:
        return {
          id: item.name,
          nodeType: NodeType.asset,
          name: item.name,
        };
      case SearchResultType.Pipeline:
        return {
          id: `${item.repositoryName}:${item.name}`,
          nodeType: NodeType.pipeline,
          name: item.name,
          repositoryName: item.repositoryName,
          repositoryLocationName: item.repositoryLocationName,
        };
      case SearchResultType.Schedule:
        return {
          id: `${item.repositoryName}:${item.name}`,
          nodeType: NodeType.schedule,
          name: item.name,
          repositoryName: item.repositoryName,
          repositoryLocationName: item.repositoryLocationName,
        };
      case SearchResultType.Sensor:
        return {
          id: `${item.repositoryName}:${item.name}`,
          nodeType: NodeType.sensor,
          name: item.name,
          repositoryName: item.repositoryName,
          repositoryLocationName: item.repositoryLocationName,
        };
      default:
        throw Error('no error');
    }
  };
  return (
    <MainContent
      style={{display: 'flex', flexDirection: 'column', justifyContent: 'space-between'}}
    >
      <SearchBar
        onSelectItem={(item: SearchResult) => {
          setSelectedNode(_searchItemToNode(item));
        }}
      />
      {layout ? (
        <LooseDependencyPipelineGraph
          graphData={graphData}
          layout={layout}
          nodeSelection={nodeSelection}
          onSelectNode={setSelectedNode}
        />
      ) : null}
      <Box padding={24} style={{borderTop: `1px solid ${Colors.LIGHT_GRAY4}`}}>
        <Checkbox
          inline
          checked={includeHistorical}
          onChange={(event) => setIncludeHistorical(event.currentTarget.checked)}
        >
          Include historical assets
        </Checkbox>
      </Box>
    </MainContent>
  );
};

type State = {
  queryString: string;
  searching: boolean;
  results: Fuse.FuseResult<SearchResult>[];
  highlight: number;
  loaded: boolean;
};
type Action =
  | {type: 'show-dialog'}
  | {type: 'hide-dialog'}
  | {type: 'highlight'; highlight: number}
  | {type: 'change-query'; queryString: string}
  | {type: 'complete-search'; results: Fuse.FuseResult<SearchResult>[]};

const reducer = (state: State, action: Action) => {
  switch (action.type) {
    case 'show-dialog':
      return {...state, shown: true, loaded: true};
    case 'hide-dialog':
      return {...state, shown: false, queryString: ''};
    case 'highlight':
      return {...state, highlight: action.highlight};
    case 'change-query':
      return {...state, queryString: action.queryString, searching: true, highlight: 0};
    case 'complete-search':
      return {...state, results: action.results, searching: false};
    default:
      return state;
  }
};

const initialState: State = {
  queryString: '',
  searching: false,
  results: [],
  highlight: 0,
  loaded: false,
};

const SearchBar = ({onSelectItem}: {onSelectItem: (item: SearchResult) => void}) => {
  const [state, dispatch] = React.useReducer(reducer, initialState);
  const {queryString, results, highlight, loaded} = state;
  const {loading, performSearch} = useRepoSearch();
  const onChange = React.useCallback((e) => {
    dispatch({type: 'change-query', queryString: e.target.value});
  }, []);
  React.useEffect(() => {
    const results = performSearch(queryString, loaded).filter((x) => SEARCH_TYPES[x.item.type]);
    dispatch({type: 'complete-search', results});
  }, [queryString, performSearch, loaded]);
  const highlightedResult = results[highlight] || null;
  const onClickResult = (result: Fuse.FuseResult<SearchResult>) => {
    dispatch({type: 'hide-dialog'});
    onSelectItem(result.item);
  };
  const onKeyDown = (e: React.KeyboardEvent) => {
    const {key} = e;
    if (key === 'Escape') {
      dispatch({type: 'hide-dialog'});
      return;
    }

    if (!results.length) {
      return;
    }

    const lastResult = results.length - 1;

    switch (key) {
      case 'ArrowUp':
        e.preventDefault();
        dispatch({
          type: 'highlight',
          highlight: highlight === 0 ? lastResult : highlight - 1,
        });
        break;
      case 'ArrowDown':
        e.preventDefault();
        dispatch({
          type: 'highlight',
          highlight: highlight === lastResult ? 0 : highlight + 1,
        });
        break;
      case 'Enter':
        e.preventDefault();
        if (highlightedResult) {
          dispatch({type: 'hide-dialog'});
          onSelectItem(highlightedResult.item);
        }
    }
  };
  return (
    <Box
      flex={{alignItems: 'center', direction: 'column'}}
      padding={{top: 32, bottom: 24}}
      style={{borderBottom: `1px solid ${Colors.LIGHT_GRAY4}`}}
    >
      <div style={{width: 750}}>
        <Popover
          minimal
          fill={true}
          isOpen={!!results.length}
          usePortal={false}
          content={
            <SearchResults
              highlight={highlight}
              queryString={queryString}
              results={results}
              onClickResult={onClickResult}
              style={{width: 750}}
            />
          }
        >
          <SearchBox hasQueryString={!!queryString.length}>
            <Icon icon="search" iconSize={18} color={Colors.LIGHT_GRAY3} />
            {loading ? <Spinner purpose="body-text" /> : null}
            <SearchInput
              autoFocus
              spellCheck={false}
              onChange={onChange}
              onKeyDown={onKeyDown}
              placeholder="Search pipelines, schedules, sensors…"
              type="text"
              value={queryString}
            />
          </SearchBox>
        </Popover>
      </div>
    </Box>
  );
};

const layoutFullGraph = (graphData: LooseDependencyGraphData) => {
  const g = new dagre.graphlib.Graph();
  const marginBase = 100;
  const marginy = marginBase;
  const marginx = marginBase;
  g.setGraph({rankdir: 'TB', marginx, marginy});
  g.setDefaultEdgeLabel(() => ({}));
  Object.values(graphData.nodes).forEach((node) => {
    g.setNode(node.id, getNodeDimensions(node.nodeType, false));
  });
  Object.keys(graphData.downstream).forEach((upstreamId) => {
    const downstreamIds = Object.keys(graphData.downstream[upstreamId]);
    downstreamIds.forEach((downstreamId) => {
      g.setEdge({v: upstreamId, w: downstreamId}, {weight: 1});
    });
  });
  dagre.layout(g);
  const {nodes, edges, width, height} = _getLayoutDataFromDagre(g, graphData);
  return {
    nodes,
    edges,
    width,
    height: height + marginBase,
  };
};

const _getLayoutDataFromDagre = (g: dagre.graphlib.Graph, graphData: LooseDependencyGraphData) => {
  const dagreNodesById: {[id: string]: dagre.Node} = {};
  g.nodes().forEach((id) => {
    const node = g.node(id);
    if (!node) {
      return;
    }
    dagreNodesById[id] = node;
  });

  let maxWidth = 0;
  let maxHeight = 0;
  const nodes: ILooseDepsNode[] = [];
  Object.keys(dagreNodesById).forEach((id) => {
    const dagreNode = dagreNodesById[id];
    nodes.push({
      id,
      type: graphData.nodes[id].nodeType,
      x: dagreNode.x - dagreNode.width / 2,
      y: dagreNode.y - dagreNode.height / 2,
    });
    maxWidth = Math.max(maxWidth, dagreNode.x + dagreNode.width);
    maxHeight = Math.max(maxHeight, dagreNode.y + dagreNode.height);
  });

  const edges: IEdge[] = [];
  g.edges().forEach((e) => {
    const points = g.edge(e).points;
    edges.push({
      from: points[0],
      to: points[points.length - 1],
      dashed: !graphData.downstream[e.v][e.w],
    });
  });

  return {
    nodes,
    edges,
    width: maxWidth,
    height: maxHeight,
  };
};

const layoutGraphDependencies = (graphData: LooseDependencyGraphData, selected: NodeSelection) => {
  const g = new dagre.graphlib.Graph();
  const marginBase = 100;
  const marginy = marginBase;
  const marginx = marginBase;
  g.setGraph({rankdir: 'TB', marginx, marginy});
  g.setDefaultEdgeLabel(() => ({}));

  // add selection
  g.setNode(selected.id, getNodeDimensions(selected.nodeType, true));

  // add downstream
  const downstreamEdges = Object.keys(graphData.downstream[selected.id] || {}).map(
    (downstreamId) => ({
      downstreamId,
      upstreamId: selected.id,
    }),
  );

  while (downstreamEdges.length) {
    const edge = downstreamEdges.pop();
    if (!edge) {
      continue;
    }
    const {downstreamId, upstreamId} = edge;
    const downstreamNode = graphData.nodes[downstreamId];
    if (downstreamNode) {
      g.setNode(downstreamNode.id, getNodeDimensions(downstreamNode.nodeType));
      g.setEdge({v: upstreamId, w: downstreamId}, {weight: 1});
      if (downstreamNode.nodeType === NodeType.sensor) {
        // add all the transitive downstream pipelines
        const sensor_id = downstreamNode.id;
        Object.keys(graphData.downstream[sensor_id] || {}).forEach((pipeline_id) => {
          downstreamEdges.push({
            downstreamId: pipeline_id,
            upstreamId: sensor_id,
          });
        });
      }
    }
  }

  // add upstream
  const upstreamEdges = Object.keys(graphData.upstream[selected.id] || {}).map((upstreamId) => ({
    upstreamId,
    downstreamId: selected.id,
  }));

  while (upstreamEdges.length) {
    const edge = upstreamEdges.pop();
    if (!edge) {
      continue;
    }
    const {upstreamId, downstreamId} = edge;
    const upstreamNode = graphData.nodes[upstreamId];
    if (upstreamNode) {
      g.setNode(upstreamNode.id, getNodeDimensions(upstreamNode.nodeType));
      g.setEdge({v: upstreamId, w: downstreamId}, {weight: 1});
      if (upstreamNode.nodeType === NodeType.sensor) {
        // add all the transitive upstream assets
        const sensor_id = upstreamNode.id;
        Object.keys(graphData.upstream[sensor_id] || {}).forEach((asset_id) => {
          upstreamEdges.push({
            upstreamId: asset_id,
            downstreamId: sensor_id,
          });
        });
      }
    }
  }

  // perform layout
  dagre.layout(g);

  const {nodes, edges, width, height} = _getLayoutDataFromDagre(g, graphData);
  return {
    nodes,
    edges,
    width,
    height: height + marginBase,
  };
};

const buildSVGPath = pathVerticalDiagonal({
  source: (s: any) => s.source,
  target: (s: any) => s.target,
  x: (s: any) => s.x,
  y: (s: any) => s.y,
});

export const LooseDependencyPipelineGraph = ({
  graphData,
  layout,
  nodeSelection,
  onSelectNode,
}: {
  graphData: LooseDependencyGraphData;
  layout: LooseDependencyGraphLayout;
  nodeSelection?: NodeSelection;
  onSelectNode: (node: NodeSelection) => void;
}) => {
  const queryData = useQuery<LooseDependencyPipelineNodeQuery>(
    LOOSE_DEPENDENCY_PIPELINE_NODE_QUERY,
    {
      variables: {
        params: layout.nodes
          .map((layoutNode) => graphData.nodes[layoutNode.id])
          .map((node) => ({
            nodeType: node.nodeType,
            name: node.name,
            repositoryName: node.repositoryName,
            repositoryLocationName: node.repositoryLocationName,
          })),
      },
    },
  );

  return (
    <Loading queryResult={queryData} allowStaleData={true}>
      {({nodesOrError}) =>
        nodesOrError.__typename === 'Nodes' ? (
          <SVGViewport
            interactor={SVGViewport.Interactors.PanAndZoom}
            graphWidth={layout.width}
            graphHeight={layout.height}
            onKeyDown={() => {}}
            onDoubleClick={() => {}}
            maxZoom={1.2}
            maxAutocenterZoom={1.0}
          >
            {({scale: _scale}: any) => (
              <SVGContainer width={layout.width} height={layout.height}>
                <defs>
                  <marker
                    id="arrow"
                    viewBox="0 0 10 10"
                    refX="1"
                    refY="5"
                    markerUnits="strokeWidth"
                    markerWidth="2"
                    markerHeight="4"
                    orient="auto"
                  >
                    <path d="M 0 0 L 10 5 L 0 10 z" fill={Colors.LIGHT_GRAY1} />
                  </marker>
                </defs>
                <g opacity={0.8}>
                  {layout.edges.map((edge, idx) => (
                    <StyledPath
                      key={idx}
                      d={buildSVGPath({source: edge.from, target: edge.to})}
                      dashed={edge.dashed}
                      markerEnd="url(#arrow)"
                    />
                  ))}
                </g>
                {layout.nodes.map((layoutNode) => {
                  const isSelected = !!nodeSelection && nodeSelection.id === layoutNode.id;
                  const {width, height} = getNodeDimensions(layoutNode.type, isSelected);
                  const graphNode = graphData.nodes[layoutNode.id];
                  return (
                    <foreignObject
                      key={layoutNode.id}
                      width={width}
                      height={height}
                      x={layoutNode.x}
                      y={layoutNode.y}
                      onClick={() => onSelectNode(graphNode)}
                    >
                      <LooseDependencyNode
                        graphNode={graphNode}
                        nodeData={nodesOrError.nodes}
                        isSelected={isSelected}
                      />
                    </foreignObject>
                  );
                })}
              </SVGContainer>
            )}
          </SVGViewport>
        ) : (
          <NonIdealState icon={IconNames.ERROR} title="GraphQL Error - see console for details" />
        )
      }
    </Loading>
  );
};

const LOOSE_DEPENDENCY_PIPELINE_EDGE_QUERY = gql`
  query LooseDependencyPipelineEdgeQuery($includeHistorical: Boolean!) {
    repositoriesOrError {
      ... on RepositoryConnection {
        nodes {
          __typename
          id
          ... on Repository {
            id
            name
            location {
              id
              name
            }
            pipelines {
              id
              name
              runs(limit: 5) @include(if: $includeHistorical) {
                id
                assets {
                  id
                  key {
                    path
                  }
                }
              }
              schedules {
                id
                name
              }
              sensors {
                id
                name
                isAssetSensor
                assets {
                  id
                  key {
                    path
                  }
                }
              }
              solids {
                name
                outputs {
                  definition {
                    name
                    asset {
                      id
                      key {
                        path
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  }
`;

const LOOSE_DEPENDENCY_PIPELINE_NODE_QUERY = gql`
  query LooseDependencyPipelineNodeQuery(
    $params: [NodeParam!]!
    $limit: Int = 50
    $before: String
  ) {
    nodesOrError(params: $params) {
      ... on PythonError {
        ...PythonErrorFragment
      }
      ... on Nodes {
        nodes {
          ... on Pipeline {
            id
            ...LooseDependencyPipelineFragment
          }
          ... on Schedule {
            id
            ...LooseDependencyScheduleFragment
          }
          ... on Asset {
            id
            ...LooseDependencyAssetFragment
          }
          ... on Sensor {
            id
            ...LooseDependencySensorFragment
          }
        }
      }
    }
  }
  ${PYTHON_ERROR_FRAGMENT}
  ${LOOSE_DEPENDENCY_ASSET_NODE_FRAGMENT}
  ${LOOSE_DEPENDENCY_PIPELINE_NODE_FRAGMENT}
  ${LOOSE_DEPENDENCY_SCHEDULE_NODE_FRAGMENT}
  ${LOOSE_DEPENDENCY_SENSOR_NODE_FRAGMENT}
`;

const SVGContainer = styled.svg`
  overflow: visible;
  border-radius: 0;
`;
const StyledPath = styled('path')<{dashed: boolean}>`
  stroke-width: 4;
  stroke: ${Colors.LIGHT_GRAY1};
  ${({dashed}) => (dashed ? `stroke-dasharray: 8 2;` : '')}
  fill: none;
`;

// Imported via React.lazy, which requires a default export.
// eslint-disable-next-line import/no-default-export
export default LooseDependenciesRoot;

interface SearchBoxProps {
  readonly hasQueryString: boolean;
}

const SearchBox = styled.div<SearchBoxProps>`
  align-items: center;
  border: 1px solid ${Colors.LIGHT_GRAY2};
  border-radius: 5px;
  display: flex;
  padding: 12px 20px 12px 12px;
  ${({hasQueryString}) =>
    hasQueryString
      ? `
  border-bottom-left-radius: 0;
  border-bottom-right-radius: 0;
  `
      : ''}
`;

// border-bottom: ${({hasQueryString}) =>
//   hasQueryString ? `1px solid ${Colors.LIGHT_GRAY2}` : 'none'};
const SearchInput = styled.input`
  border: none;
  color: ${Colors.GRAY1};
  font-family: ${FontFamily.default};
  font-size: 18px;
  margin-left: 8px;
  outline: none;
  width: 100%;

  &::placeholder {
    color: ${Colors.GRAY5};
  }
`;
