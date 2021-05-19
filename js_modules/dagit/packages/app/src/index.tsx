// Before anything else, set the webpack public path.
import './publicPath';

import {App} from '@dagit/core/app/App';
import {AppCache} from '@dagit/core/app/AppCache';
import {AppProvider} from '@dagit/core/app/AppProvider';
import {AppTopNav} from '@dagit/core/app/AppTopNav';
import {Permissions} from '@dagit/core/app/Permissions';
import * as React from 'react';
import ReactDOM from 'react-dom';

import {extractPathPrefix} from './extractPathPrefix';

const pathPrefix = extractPathPrefix();

const persmissionElement = document.getElementById('permissions');

export interface PermissionsJson {
  permissions: Permissions;
}

const identity: PermissionsJson = persmissionElement
  ? JSON.parse(persmissionElement.textContent || '')
  : {
      permissions: {},
    };

const permissions = identity.permissions === '[permissions here]' ? {} : identity.permissions;

const config = {
  basePath: pathPrefix,
  graphqlURI: process.env.REACT_APP_GRAPHQL_URI || '',
  permissions,
};

ReactDOM.render(
  <AppProvider appCache={AppCache} config={config}>
    <AppTopNav searchPlaceholder="Search…" />
    <App />
  </AppProvider>,
  document.getElementById('root'),
);
