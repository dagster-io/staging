import {gql} from '@apollo/client';
import {Callout, Colors} from '@blueprintjs/core';
import * as React from 'react';

import {showCustomAlert} from 'src/CustomAlertProvider';
import {PythonErrorInfo} from 'src/PythonErrorInfo';
import {ScheduledTicksFragment} from 'src/runs/types/ScheduledTicksFragment';
import {REPOSITORY_SCHEDULES_FRAGMENT} from 'src/schedules/ScheduleUtils';
import {SchedulesNextTicks} from 'src/schedules/SchedulesNextTicks';
import {ButtonLink} from 'src/ui/ButtonLink';
import {Group} from 'src/ui/Group';

export const AllScheduledTicks: React.FC<{repos: ScheduledTicksFragment}> = ({repos}) => {
  if (repos.__typename === 'PythonError') {
    const viewError = () => {
      if (repos.__typename === 'PythonError') {
        const message = repos.message;
        return (
          <ButtonLink
            color={Colors.BLUE3}
            underline="always"
            onClick={() => {
              showCustomAlert({
                title: 'Python error',
                body: message,
              });
            }}
          >
            View error
          </ButtonLink>
        );
      }
      return null;
    };

    return (
      <Callout intent="warning">
        <Group direction="row" spacing={4}>
          <div>Could not load scheduled ticks.</div>
          {viewError()}
        </Group>
      </Callout>
    );
  }

  return <SchedulesNextTicks repos={repos.nodes} />;
};

export const SCHEDULED_TICKS_FRAGMENT = gql`
  fragment ScheduledTicksFragment on RepositoriesOrError {
    ... on RepositoryConnection {
      nodes {
        __typename
        id
        ... on Repository {
          ...RepositorySchedulesFragment
        }
      }
    }
    ...PythonErrorFragment
  }
  ${REPOSITORY_SCHEDULES_FRAGMENT}
  ${PythonErrorInfo.fragments.PythonErrorFragment}
`;
