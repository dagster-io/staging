import {ApolloClient, gql} from '@apollo/client';
import {Icon, Colors} from '@blueprintjs/core';
import * as React from 'react';
import {useEffect, useState} from 'react';
import {Link} from 'react-router-dom';
import styled from 'styled-components';

import {ButtonLink} from 'src/ButtonLink';
import {DirectGraphQLSubscription} from 'src/DirectGraphQLSubscription';
import {GrpcServerStateChangeSubscription} from 'src/nav/types/GrpcServerStateChangeSubscription';
import {GrpcServerStateChangeEventType} from 'src/types/globalTypes';
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

export const StateObserver = ({client}: StateObserverProps) => {
  const {nodes, refetch} = useRepositoryLocations();
  const [updatedLocations, setUpdatedLocations] = useState<string[]>([]);
  const [erroredLocations, setErroredLocations] = useState<string[]>([]);
  const totalMessages = updatedLocations.length + erroredLocations.length;

  useEffect(() => {
    const onHandleMessages = (
      messages: GrpcServerStateChangeSubscription[], //   isFirstResponse: boolean,
    ) => {
      const {
        grpcServerStateChangeEvents: {event},
      } = messages[0];
      const {locationName, eventType, serverId} = event;

      switch (eventType) {
        case GrpcServerStateChangeEventType.SERVER_ERROR:
          setUpdatedLocations((s) => s.filter((name) => name !== locationName));
          setErroredLocations((s) => [...s, locationName]);
          return;
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

  return totalMessages > 0 ? (
    <Group background={Colors.GRAY5} direction="vertical" spacing={0}>
      {updatedLocations.length > 0 ? (
        <Group padding={{vertical: 8, horizontal: 12}} direction="horizontal" spacing={8}>
          <Icon icon="warning-sign" color={Colors.DARK_GRAY3} iconSize={14} />
          <Caption color={Colors.DARK_GRAY3}>
            {updatedLocations.length == 1
              ? `Repository location ${updatedLocations[0]} has been updated,` // Be specific when there's only one repository location updated
              : 'One or more repository locations have been updated,'}
            and new data is available.
            <SmallButtonLink
              onClick={() => {
                setUpdatedLocations([]);
                setErroredLocations([]);
                refetch();
                client.resetStore();
              }}
            >
              Update data
            </SmallButtonLink>
          </Caption>
        </Group>
      ) : null}

      {erroredLocations.length > 0 ? (
        <Group padding={{vertical: 8, horizontal: 12}} direction="horizontal" spacing={8}>
          <Icon icon="warning-sign" color={Colors.DARK_GRAY3} iconSize={14} />
          <Caption color={Colors.DARK_GRAY3}>
            An error occurred in a repository location.{' '}
            <DetailLink to="/workspace/repository-locations">View details</DetailLink>
          </Caption>
        </Group>
      ) : null}
    </Group>
  ) : null;
};

const DetailLink = styled(Link)`
  color: ${Colors.DARK_GRAY3};
  text-decoration: underline;

  && :hover,
  :active,
  :visited {
    color: ${Colors.DARK_GRAY1};
  }
`;

const SmallButtonLink = styled(ButtonLink)`
  font-size: 12px;
  padding: 0;
  margin: 0;
  color: ${Colors.DARK_GRAY3};
  text-decoration: underline;

  && :hover,
  :active,
  :visited {
    color: ${Colors.DARK_GRAY1};
  }
`;
