import * as React from 'react';

// import {Permissions} from './Permissions';
// import {RouteConfig} from './types';

const FeatureFlagsRoot = React.lazy(() => import('./FeatureFlagsRoot'));

export const routes = [
  {path: '/flags', render: () => <FeatureFlagsRoot />},
  // {
  //   path: '/instance',
  //   routes: instanceRoutes,
  // },
  // {path: '/workspace', component: WorkspaceRoot},
  {path: '*', render: () => <div />},
];
