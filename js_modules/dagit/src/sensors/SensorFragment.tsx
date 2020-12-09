import {gql} from '@apollo/client';

import {JOB_STATE_FRAGMENT} from 'src/JobUtils';
import {REPOSITORY_INFO_FRAGMENT} from 'src/RepositoryInformation';

export const SENSOR_FRAGMENT = gql`
  fragment SensorFragment on Sensor {
    id
    jobOriginId
    name
    pipelineName
    solidSelection
    mode
    sensorState {
      id
      ...JobStateFragment
    }
  }
  ${JOB_STATE_FRAGMENT}
`;

export const REPOSITORY_SENSORS_FRAGMENT = gql`
  fragment RepositorySensorsFragment on Repository {
    name
    id
    sensors {
      id
      ...SensorFragment
    }
    ...RepositoryInfoFragment
  }
  ${REPOSITORY_INFO_FRAGMENT}
  ${SENSOR_FRAGMENT}
`;
