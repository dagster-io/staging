import {App, AppProvider, AppTopNav} from '@dagit/core';
import * as React from 'react';
import ReactDOM from 'react-dom';

const UNSET_APP_PATH_PREFIX_VALUE = '{{ app_path_prefix }}';
const META_APP_PATH =
  document.querySelector('meta[name=dagit-path-prefix]')?.getAttribute('content') || '';
const APP_PATH_PREFIX = META_APP_PATH !== UNSET_APP_PATH_PREFIX_VALUE ? META_APP_PATH : '';

const config = {
  basePath: APP_PATH_PREFIX,
  graphqlURI: process.env.REACT_APP_GRAPHQL_URI || '',
};

ReactDOM.render(
  <AppProvider config={config}>
    <AppTopNav searchPlaceholder="Search…" />
    <App />
  </AppProvider>,
  document.getElementById('root'),
);
