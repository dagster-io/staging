import {useQuery, gql} from '@apollo/client';
import * as React from 'react';

const QUERY = gql`
  query sidQuery {
    auditLogs {
      __typename
      timestamp
      user {
        email
      }
    }
  }
`;

export const SidRoot = () => {
  const {data, loading} = useQuery(QUERY);
  console.log(data, loading);
  if (loading) {
    return <div>LOADING</div>;
  }

  return (
    <div>
      <pre>{JSON.stringify(data)}</pre>
    </div>
  );
};

// eslint-disable-next-line import/no-default-export
export default SidRoot;
