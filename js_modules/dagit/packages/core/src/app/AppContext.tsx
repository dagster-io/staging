import * as React from 'react';

import {PermissionsFromJSON} from './Permissions';

export type AppContextValue = {
  basePath: string;
  permissions: PermissionsFromJSON;
  rootServerURI: string;
  websocketURI: string;
  authentication: {
    organizationId: number | null;
    deploymentId: number | null;
    sessionToken: string | null;
  };
};

export const AppContext = React.createContext<AppContextValue>({
  basePath: '',
  permissions: {},
  rootServerURI: '',
  websocketURI: '',
  authentication: {
    organizationId: null,
    deploymentId: null,
    sessionToken: null,
  },
});
