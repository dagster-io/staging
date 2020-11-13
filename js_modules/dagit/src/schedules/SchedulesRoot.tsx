import {useQuery} from '@apollo/client';
import {
  Code,
  IBreadcrumbProps,
  NonIdealState,
  PopoverInteractionKind,
  Tooltip,
} from '@blueprintjs/core';
import {IconNames} from '@blueprintjs/icons';
import * as React from 'react';

import {Header, ScrollContainer} from 'src/ListComponents';
import {Loading} from 'src/Loading';
import {PythonErrorInfo} from 'src/PythonErrorInfo';
import {RepositoryInformation} from 'src/RepositoryInformation';
import {useDocumentTitle} from 'src/hooks/useDocumentTitle';
import {TopNav} from 'src/nav/TopNav';
import {ScheduleRow} from 'src/schedules/ScheduleRow';
import {SCHEDULES_ROOT_QUERY, SchedulerTimezoneNote} from 'src/schedules/ScheduleUtils';
import {SchedulerInfo} from 'src/schedules/SchedulerInfo';
import {
  SchedulesRootQuery,
  SchedulesRootQuery_scheduler,
  SchedulesRootQuery_repositoryOrError_Repository,
  SchedulesRootQuery_repositoryOrError_Repository_scheduleDefinitions,
} from 'src/schedules/types/SchedulesRootQuery';
import {Table} from 'src/ui/Table';
import {repoAddressToSelector} from 'src/workspace/repoAddressToSelector';
import {RepoAddress} from 'src/workspace/types';

interface Props {
  repoAddress: RepoAddress;
}

export const SchedulesRoot: React.FC<Props> = (props) => {
  const {repoAddress} = props;
  useDocumentTitle('Schedules');
  const repositorySelector = repoAddressToSelector(repoAddress);

  // This may be null and that's ok

  const queryResult = useQuery<SchedulesRootQuery>(SCHEDULES_ROOT_QUERY, {
    variables: {
      repositorySelector: repositorySelector,
    },
    fetchPolicy: 'cache-and-network',
    pollInterval: 50 * 1000,
    partialRefetch: true,
  });

  const breadcrumbs: IBreadcrumbProps[] = [{icon: 'time', text: 'Schedules'}];

  return (
    <ScrollContainer>
      <TopNav breadcrumbs={breadcrumbs} />
      <Loading queryResult={queryResult} allowStaleData={true}>
        {(result) => {
          const {repositoryOrError, scheduler} = result;
          let scheduleDefinitionsSection = null;

          if (repositoryOrError.__typename === 'PythonError') {
            scheduleDefinitionsSection = <PythonErrorInfo error={repositoryOrError} />;
          } else if (repositoryOrError.__typename === 'RepositoryNotFoundError') {
            scheduleDefinitionsSection = (
              <NonIdealState
                icon={IconNames.ERROR}
                title="Repository not found"
                description="Could not load this repository."
              />
            );
          } else {
            const scheduleDefinitions = repositoryOrError.scheduleDefinitions;
            if (!scheduleDefinitions.length) {
              scheduleDefinitionsSection = (
                <NonIdealState
                  icon={IconNames.ERROR}
                  title="No Schedules Found"
                  description={
                    <p>
                      This repository does not have any schedules defined. Visit the{' '}
                      <a href="https://docs.dagster.io/overview/scheduling-partitions/schedules">
                        scheduler documentation
                      </a>{' '}
                      for more information about scheduling pipeline runs in Dagster. .
                    </p>
                  }
                />
              );
            } else {
              scheduleDefinitionsSection = (
                <RepositorySchedules
                  schedules={scheduleDefinitions}
                  repository={repositoryOrError}
                  scheduler={scheduler}
                />
              );
            }
          }

          return (
            <div style={{padding: '16px'}}>
              <SchedulerInfo schedulerOrError={scheduler} />
              {scheduleDefinitionsSection}
            </div>
          );
        }}
      </Loading>
    </ScrollContainer>
  );
};

export interface SchedulesTableProps {
  repository: SchedulesRootQuery_repositoryOrError_Repository;
}

export const SchedulesTable: React.FunctionComponent<SchedulesTableProps> = (props) => {
  const {repository} = props;

  const repoAddress = {
    name: repository.name,
    location: repository.location.name,
  };
  const schedules = repository.scheduleDefinitions;

  return (
    <>
      <div>
        {`${schedules.length} loaded from `}
        <Tooltip
          interactionKind={PopoverInteractionKind.HOVER}
          content={
            <pre>
              <RepositoryInformation repository={repository} />
              <div style={{fontSize: 11}}>
                <span style={{marginRight: 5}}>id:</span>
                <span style={{opacity: 0.5}}>{repository.id}</span>
              </div>
            </pre>
          }
        >
          <Code>{repository.name}</Code>
        </Tooltip>
      </div>
      <Table striped style={{width: '100%'}}>
        <thead>
          <tr>
            <th style={{maxWidth: '60px'}}></th>
            <th>Schedule Name</th>
            <th>Pipeline</th>
            <th style={{width: '150px'}}>Schedule</th>
            <th style={{width: '100px'}}>Last Tick</th>
            <th>Latest Runs</th>
            <th>Execution Params</th>
          </tr>
        </thead>
        <tbody>
          {schedules.map((schedule) => (
            <ScheduleRow repoAddress={repoAddress} schedule={schedule} key={schedule.name} />
          ))}
        </tbody>
      </Table>{' '}
    </>
  );
};

export interface RepositorySchedulesProps {
  schedules: SchedulesRootQuery_repositoryOrError_Repository_scheduleDefinitions[];
  repository: SchedulesRootQuery_repositoryOrError_Repository;
  scheduler: SchedulesRootQuery_scheduler;
}

export const RepositorySchedules: React.FunctionComponent<RepositorySchedulesProps> = (props) => {
  const {repository, schedules, scheduler} = props;
  if (schedules.length === 0) {
    return null;
  }

  return (
    <div>
      <div style={{display: 'flex'}}>
        <Header>Schedules</Header>
        <div style={{flex: 1}} />
        <SchedulerTimezoneNote schedulerOrError={scheduler} />
      </div>
      <SchedulesTable repository={repository} />
    </div>
  );
};
