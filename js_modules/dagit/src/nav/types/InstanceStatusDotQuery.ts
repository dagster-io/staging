// @generated
/* tslint:disable */
/* eslint-disable */
// @generated
// This file was automatically generated and should not be edited.

// ====================================================
// GraphQL query operation: InstanceStatusDotQuery
// ====================================================

export interface InstanceStatusDotQuery_instance_daemonHealth_allDaemonStatuses_lastHeartbeatErrors_cause {
  __typename: "PythonError";
  message: string;
  stack: string[];
}

export interface InstanceStatusDotQuery_instance_daemonHealth_allDaemonStatuses_lastHeartbeatErrors {
  __typename: "PythonError";
  message: string;
  stack: string[];
  cause: InstanceStatusDotQuery_instance_daemonHealth_allDaemonStatuses_lastHeartbeatErrors_cause | null;
}

export interface InstanceStatusDotQuery_instance_daemonHealth_allDaemonStatuses {
  __typename: "DaemonStatus";
  id: string;
  daemonType: string | null;
  required: boolean;
  healthy: boolean | null;
  lastHeartbeatErrors: InstanceStatusDotQuery_instance_daemonHealth_allDaemonStatuses_lastHeartbeatErrors[];
  lastHeartbeatTime: number | null;
}

export interface InstanceStatusDotQuery_instance_daemonHealth {
  __typename: "DaemonHealth";
  allDaemonStatuses: InstanceStatusDotQuery_instance_daemonHealth_allDaemonStatuses[];
}

export interface InstanceStatusDotQuery_instance {
  __typename: "Instance";
  daemonHealth: InstanceStatusDotQuery_instance_daemonHealth;
}

export interface InstanceStatusDotQuery {
  instance: InstanceStatusDotQuery_instance;
}
