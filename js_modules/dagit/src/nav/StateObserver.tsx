import {ApolloClient, gql} from '@apollo/client';
import {Icon, Colors, Spinner} from '@blueprintjs/core';
import * as React from 'react';
import {useEffect, useState} from 'react';
// import styled from 'styled-components';

import {DirectGraphQLSubscription} from 'src/DirectGraphQLSubscription';
import {GrpcServerStateChangeSubscription} from 'src/nav/types/GrpcServerStateChangeSubscription';
import {GrpcServerState, GrpcServerStateChangeEventType} from 'src/types/globalTypes';
import {Group} from 'src/ui/Group';
import {Caption} from 'src/ui/Text';
import {useRepositoryLocations} from 'src/workspace/WorkspaceContext';

const GRPC_SERVER_STATE_CHANGE_SUBSCRIPTION = gql`
  subscription GrpcServerStateChangeSubscription {
    grpcServerStateChangeEvents {
      event {
        message
        locationName
        eventType
        serverId
      }
    }
  }
`;

interface StateObserverProps {
  client: ApolloClient<any>;
  onReload: () => void;
}

export const StateObserver = ({}: StateObserverProps) => {
  const {nodes, refetch} = useRepositoryLocations();
  const [updatedLocations, setUpdatedLocations] = useState<string[]>([]);
  const reconnectingLocations = nodes
    .filter(
      (node) =>
        node &&
        node?.__typename === 'RepositoryLocation' &&
        node.serverState == GrpcServerState.SERVER_RECONNECTING,
    )
    .map((node) => node.name)
    .filter((locationName) => !updatedLocations.includes(locationName));

  const totalMessages = reconnectingLocations.length + updatedLocations.length;

  useEffect(() => {
    const onHandleMessages = (
      messages: GrpcServerStateChangeSubscription[], //   isFirstResponse: boolean,
    ) => {
      const {
        grpcServerStateChangeEvents: {event},
      } = messages[0];
      const {locationName, eventType, serverId} = event;

      switch (eventType) {
        case GrpcServerStateChangeEventType.SERVER_UPDATED:
          const matchingRepositoryLocation = nodes.find((n) => n.name == locationName);
          if (
            matchingRepositoryLocation &&
            matchingRepositoryLocation?.__typename === 'RepositoryLocation' &&
            matchingRepositoryLocation.serverId !== serverId
          ) {
            setUpdatedLocations((s) => [...s, locationName]);
          }
          return;
        default:
          if (refetch) {
            refetch();
          }
      }
    };

    const subscriptionToken = new DirectGraphQLSubscription<GrpcServerStateChangeSubscription>(
      GRPC_SERVER_STATE_CHANGE_SUBSCRIPTION,
      {},
      onHandleMessages,
      () => {}, // https://github.com/dagster-io/dagster/issues/2151
    );

    return () => {
      subscriptionToken.close();
    };
  }, [nodes, refetch]);

  return totalMessages || true ? (
    <Group background={Colors.RED5} direction="vertical" spacing={0}>
      {reconnectingLocations.map((locationName) => (
        <Group
          key={locationName}
          padding={{vertical: 8, horizontal: 12}}
          direction="horizontal"
          spacing={8}
        >
          <div style={{marginTop: 4}}>
            <Spinner size={14} />
          </div>
          <Caption color={Colors.DARK_GRAY3}>
            {locationName} has been disconnected. Attempting to automatically reconnect...
          </Caption>
        </Group>
      ))}

      {updatedLocations.map((locationName) => (
        <Group
          key={locationName}
          padding={{vertical: 8, horizontal: 12}}
          direction="horizontal"
          spacing={8}
        >
          <Icon icon="warning-sign" color={Colors.DARK_GRAY3} iconSize={14} />
          <Caption color={Colors.DARK_GRAY3}>
            Repository location {locationName} has been updated. Click{' '}
            <button
              onClick={() => {
                refetch();
                setUpdatedLocations([]);
              }}
            >
              here
            </button>{' '}
            to update data.
          </Caption>
        </Group>
      ))}
    </Group>
  ) : null;
};

// const LocationError = styled.div`
//   align-items: center;
//   background-color: ${Colors.RED3};
//   border: 0;
//   color: ${Colors.LIGHT_GRAY5};
//   display: flex;
//   flex-direction: row;
//   align-items: flex-start;
//   padding: 8px 12px;
//   text-align: left;

//   &:active {
//     outline: none;
//   }

//   & a,
//   a:hover,
//   a:active {
//     color: ${Colors.LIGHT_GRAY5};
//     text-decoration: underline;
//     white-space: nowrap;
//   }
// `;
