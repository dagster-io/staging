import React from 'react';
import {Link} from 'react-router-dom';

import {Group} from '../main';
import {ButtonLink} from '../ui/ButtonLink';

import {AssetQuery_assetOrError_Asset_assetMaterializations_materializationEvent_assetLineage} from './types/AssetQuery';

export const AssetLineageInfoElement: React.FunctionComponent<{
  lineage_info: AssetQuery_assetOrError_Asset_assetMaterializations_materializationEvent_assetLineage;
}> = ({lineage_info}) => {
  const partition_list_label = lineage_info.partitions.length == 1 ? 'Partition' : 'Partitions';
  const partition_list_str = lineage_info.partitions
    .map((partition) => `"${partition}"`)
    .join(', ');
  const parent_partition_phrase = (
    <>
      {partition_list_label} {partition_list_str} of{' '}
    </>
  );
  return (
    <>
      {lineage_info.partitions.length > 0 ? parent_partition_phrase : ''}
      <Link to={`/instance/assets/${lineage_info.assetKey.path.map(encodeURIComponent).join('/')}`}>
        {lineage_info.assetKey.path.join(' > ')}
      </Link>
    </>
  );
};

const MAX_COLLAPSED = 5;

export const AssetLineageElements: React.FunctionComponent<{
  elements: AssetQuery_assetOrError_Asset_assetMaterializations_materializationEvent_assetLineage[];
}> = ({elements}) => {
  const [collapsed, setCollapsed] = React.useState(true);

  return (
    <Group direction={'column'} spacing={0}>
      {(collapsed ? elements.slice(0, MAX_COLLAPSED) : elements).map((info, idx) => (
        <AssetLineageInfoElement key={idx} lineage_info={info} />
      ))}
      {elements.length > MAX_COLLAPSED && (
        <ButtonLink onClick={() => setCollapsed(!collapsed)}>
          {collapsed ? 'Show More' : 'Show Less'}
        </ButtonLink>
      )}
    </Group>
  );
};
