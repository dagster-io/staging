/* eslint-disable no-restricted-globals */

import {layoutPipeline} from '../graph/layout';
const ctx: Worker = self as any;

ctx.addEventListener('message', (event) => {
  const {solids, parentSolid} = event.data;
  const layout = layoutPipeline(solids, parentSolid);
  ctx.postMessage(layout);
});
