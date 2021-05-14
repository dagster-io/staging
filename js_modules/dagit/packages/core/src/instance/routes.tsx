import React from 'react';
import {Redirect} from 'react-router-dom';

import {AssetEntryRoot} from '../assets/AssetEntryRoot';
import {AssetsCatalogRoot} from '../assets/AssetsCatalogRoot';
import {RunRoot} from '../runs/RunRoot';
import {RunsRoot} from '../runs/RunsRoot';
import {SnapshotRoot} from '../snapshots/SnapshotRoot';

import {InstanceStatusRoot} from './InstanceStatusRoot';

export const instanceRoutes = [
  {path: '/instance/assets', exact: true, component: AssetsCatalogRoot},
  {path: '/instance/assets/(/?.*)', component: AssetEntryRoot},
  {path: '/instance/runs', exact: true, component: RunsRoot},
  {path: '/instance/runs/:runId', exact: true, component: RunRoot},
  {path: '/instance/snapshots/:pipelinePath/:tab?', component: SnapshotRoot},
  {path: '/instance/:tab', component: InstanceStatusRoot},
  {path: '/instance', render: () => <Redirect to="/instance/health" />},
];
