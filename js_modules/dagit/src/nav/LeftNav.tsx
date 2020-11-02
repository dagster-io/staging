import {ApolloClient, ApolloConsumer, gql} from '@apollo/client';
import {Colors, Icon} from '@blueprintjs/core';
import * as React from 'react';
import {useHistory, useRouteMatch} from 'react-router';
import {Link} from 'react-router-dom';
import styled from 'styled-components/macro';

import {DagsterRepoOption} from 'src/DagsterRepositoryContext';
import {ShortcutHandler} from 'src/ShortcutHandler';
import {TimezonePicker} from 'src/TimeComponents';
import {WebsocketStatus} from 'src/WebsocketStatus';
import navBarImage from 'src/images/nav-logo-icon.png';
import navTitleImage from 'src/images/nav-title.png';
import {InstanceDetailsLink} from 'src/nav/InstanceDetailsLink';
import {RepositoryContentList} from 'src/nav/RepositoryContentList';
import {RepositoryPicker} from 'src/nav/RepositoryPicker';
import {SchedulesList} from 'src/nav/SchedulesList';
import {
  HandleStateChangeSubscription,
  HandleStateChangeSubscription_handleStateChangeEvents_event,
} from 'src/nav/types/HandleStateChangeSubscription';
import {useRepositoryLocations} from 'src/workspace/useRepositoryLocations';
import {DirectGraphQLSubscription} from 'src/DirectGraphQLSubscription';
import {HandleStateChangeEventType} from 'src/types/globalTypes';

const KEYCODE_FOR_1 = 49;

const INSTANCE_TABS = [
  {
    to: `/instance/runs`,
    tab: `runs`,
    icon: <Icon icon="history" iconSize={18} />,
    label: 'Runs',
  },
  {
    to: `/instance/assets`,
    tab: `assets`,
    icon: <Icon icon="panel-table" iconSize={18} />,
    label: 'Assets',
  },
  {
    to: `/instance/scheduler`,
    tab: `scheduler`,
    icon: <Icon icon="time" iconSize={18} />,
    label: 'Scheduler',
  },
];

const HANDLE_STATE_CHANGE_SUBSCRIPTION = gql`
  subscription HandleStateChangeSubscription {
    handleStateChangeEvents {
      event {
        eventType
        message
        locationName
      }
    }
  }
`;

interface LeftNavProps {
  loading: boolean;
  options: DagsterRepoOption[];
  repo: DagsterRepoOption | null;
  setRepo: (repo: DagsterRepoOption) => void;
}

interface StateObserverProps {
  client: ApolloClient<any>;
  onReload: () => void;
}

interface StateObserverState {
  latestEvent: HandleStateChangeSubscription_handleStateChangeEvents_event | null;
}

class StateObserver extends React.Component<StateObserverProps, StateObserverState> {
  state: StateObserverState = {
    latestEvent: null,
  };

  _subscription: DirectGraphQLSubscription<HandleStateChangeSubscription>;

  componentDidMount() {
    this.subscribeToMessages();
  }

  componentWillUnmount() {
    this.unsubscribeFromMessages();
  }

  subscribeToMessages() {
    if (this._subscription) {
      this.unsubscribeFromMessages();
      this.setState({latestEvent: null});
    }

    this._subscription = new DirectGraphQLSubscription<HandleStateChangeSubscription>(
      HANDLE_STATE_CHANGE_SUBSCRIPTION,
      {},
      this.onHandleMessages,
      () => {}, // https://github.com/dagster-io/dagster/issues/2151
    );
  }

  onHandleMessages = (messages: HandleStateChangeSubscription[], isFirstResponse: boolean) => {
    console.log(messages);
    for (const msg of messages) {
      const {
        handleStateChangeEvents: {event},
      } = msg;

      console.log(event);

      if (event.eventType === HandleStateChangeEventType.SERVER_UPDATED) {
        this.props.onReload();
        this.props.client.resetStore();
        this.setState({latestEvent: null});
      } else if (event.eventType === HandleStateChangeEventType.SERVER_ERROR) {
        this.props.onReload();
        this.props.client.resetStore();
        this.setState({latestEvent: null});
      } else {
        this.setState({latestEvent: event});
      }
    }
  };

  unsubscribeFromMessages() {
    if (this._subscription) {
      this._subscription.close();
    }
  }

  render() {
    return (
      this.state.latestEvent && (
        <LocationError>
          <Icon icon="warning-sign" color={Colors.LIGHT_GRAY5} iconSize={14} />
          <div style={{fontSize: '12px', margin: '0 8px'}}>{this.state.latestEvent.message}</div>
        </LocationError>
      )
    );
  }
}

export const LeftNav: React.FunctionComponent<LeftNavProps> = ({
  loading,
  options,
  repo,
  setRepo,
}) => {
  const history = useHistory();
  const match = useRouteMatch<
    | {selector: string; tab: string; rootTab: undefined}
    | {selector: undefined; tab: undefined; rootTab: string}
  >(['/pipeline/:selector/:tab?', '/solid/:selector', '/schedules/:selector', '/:rootTab?']);
  const {nodes: locations, refetch} = useRepositoryLocations();

  const anyErrors = locations.some((node) => node.__typename === 'RepositoryLocationLoadFailure');

  // const {data} = useSubscription<ConnectionMessageSubscription>(CONNECTION_MESSAGE_SUBSCRIPTION);
  // const {connectionMessages} = data || {};

  const onReload = () => {
    refetch();
  };

  return (
    <LeftNavContainer>
      <div>
        <LogoContainer>
          <img
            alt="logo"
            src={navBarImage}
            style={{height: 40}}
            onClick={() => history.push('/')}
          />
          <LogoMetaContainer>
            <img src={navTitleImage} style={{height: 10}} alt="title" />
            <InstanceDetailsLink />
          </LogoMetaContainer>
          <LogoWebsocketStatus />
        </LogoContainer>
        {INSTANCE_TABS.map((t, i) => (
          <ShortcutHandler
            key={t.tab}
            onShortcut={() => history.push(t.to)}
            shortcutLabel={`⌥${i + 1}`}
            shortcutFilter={(e) => e.keyCode === KEYCODE_FOR_1 + i && e.altKey}
          >
            <Tab to={t.to} className={match?.params.rootTab === t.tab ? 'selected' : ''}>
              {t.icon}
              <TabLabel>{t.label}</TabLabel>
            </Tab>
          </ShortcutHandler>
        ))}
      </div>
      <div style={{height: 20}} />
      <div
        className="bp3-dark"
        style={{
          background: `rgba(0,0,0,0.3)`,
          color: Colors.WHITE,
          display: 'flex',
          flex: 1,
          overflow: 'none',
          flexDirection: 'column',
          minHeight: 0,
        }}
      >
        <RepositoryPicker
          loading={loading}
          options={options}
          repo={repo}
          setRepo={setRepo}
          onReload={onReload}
        />
        {anyErrors ? (
          <LoadingError>
            <Icon icon="warning-sign" color={Colors.DARK_GRAY3} iconSize={14} />
            <div style={{fontSize: '12px', margin: '0 8px'}}>
              An error occurred while loading a repository.{' '}
              <Link to="/workspace/repository-locations">View details</Link>
            </div>
          </LoadingError>
        ) : null}
        <ApolloConsumer>
          {(client) => <StateObserver onReload={onReload} client={client} />}
        </ApolloConsumer>
        {/*
        {connectionMessages ? (
          <LocationError>
            <Icon icon="warning-sign" color={Colors.LIGHT_GRAY5} iconSize={14} />
            <div style={{fontSize: '12px', margin: '0 8px'}}>
              The server has been disconnected. Attempting to automatically reconnect.{' '}
              <Link to="/workspace/repository-locations">View details</Link>
            </div>
          </LocationError>
        ) : null} */}
        {repo && (
          <div style={{display: 'flex', flex: 1, flexDirection: 'column', minHeight: 0}}>
            <ItemHeader>Pipelines & Solids:</ItemHeader>
            <RepositoryContentList {...match?.params} repo={repo} />
            <ItemHeader>Schedules:</ItemHeader>
            <SchedulesList {...match?.params} repo={repo} />
          </div>
        )}
      </div>
      <TimezonePicker />
    </LeftNavContainer>
  );
};

const LogoWebsocketStatus = styled(WebsocketStatus)`
  position: absolute;
  top: 28px;
  left: 42px;
`;

const ItemHeader = styled.div`
  font-size: 15px;
  text-overflow: ellipsis;
  overflow: hidden;
  padding: 8px 12px;
  padding-left: 8px;
  margin-top: 10px;
  border-left: 4px solid transparent;
  border-bottom: 1px solid transparent;
  display: block;
  font-weight: bold;
  color: ${Colors.LIGHT_GRAY3} !important;
`;

const LeftNavContainer = styled.div`
  width: 235px;
  height: 100%;
  display: flex;
  flex-shrink: 0;
  flex-direction: column;
  justify-content: start;
  background: ${Colors.DARK_GRAY2};
  border-right: 1px solid ${Colors.DARK_GRAY5};
  padding-top: 14px;
`;

const Tab = styled(Link)`
  color: ${Colors.LIGHT_GRAY1} !important;
  border-left: 4px solid transparent;
  border-right: 4px solid transparent;
  display: flex;
  padding: 8px 12px;
  margin: 0px 0;
  align-items: center;
  outline: 0;
  &:hover {
    color: ${Colors.WHITE} !important;
    text-decoration: none;
  }
  &:focus {
    outline: 0;
  }
  &.selected {
    color: ${Colors.WHITE} !important;
    border-left: 4px solid ${Colors.COBALT3};
    font-weight: 600;
  }
`;

const TabLabel = styled.div`
  font-size: 13px;
  margin-left: 6px;
  text-decoration: none;
  white-space: nowrap;
  text-decoration: none;
`;

const LogoContainer = styled.div`
  width: 100%;
  padding: 0 10px;
  margin-bottom: 10px;
  position: relative;
  cursor: pointer;
  &:hover {
    img {
      filter: brightness(110%);
    }
  }
`;

const LogoMetaContainer = styled.div`
  position: absolute;
  left: 56px;
  top: -3px;
  height: 42px;
  padding-left: 4px;
  right: 0;
  z-index: 1;
  border-bottom: 1px solid ${Colors.DARK_GRAY4};
`;

const LoadingError = styled.div`
  align-items: center;
  background-color: ${Colors.GOLD5};
  border: 0;
  color: ${Colors.DARK_GRAY3};
  display: flex;
  flex-direction: row;
  align-items: flex-start;
  padding: 8px 12px;
  text-align: left;

  &:active {
    outline: none;
  }

  & a,
  a:hover,
  a:active {
    color: ${Colors.DARK_GRAY3};
    text-decoration: underline;
    white-space: nowrap;
  }
`;

const LocationError = styled.div`
  align-items: center;
  background-color: ${Colors.RED3};
  border: 0;
  color: ${Colors.LIGHT_GRAY5};
  display: flex;
  flex-direction: row;
  align-items: flex-start;
  padding: 8px 12px;
  text-align: left;

  &:active {
    outline: none;
  }

  & a,
  a:hover,
  a:active {
    color: ${Colors.LIGHT_GRAY5};
    text-decoration: underline;
    white-space: nowrap;
  }
`;
