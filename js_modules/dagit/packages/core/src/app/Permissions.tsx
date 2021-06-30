import * as React from 'react';

import {AppContext} from '../app/AppContext';

export type PermissionsFromJSON = {
  LAUNCH_PIPELINE_EXECUTION?: boolean;
  LAUNCH_PIPELINE_REEXECUTION?: boolean;
  RECONCILE_SCHEDULER_STATE?: boolean;
  START_SCHEDULE?: boolean;
  STOP_RUNNING_SCHEDULE?: boolean;
  START_SENSOR?: boolean;
  STOP_SENSOR?: boolean;
  TERMINATE_PIPELINE_EXECUTION?: boolean;
  DELETE_PIPELINE_RUN?: boolean;
  RELOAD_REPOSITORY_LOCATION?: boolean;
  RELOAD_WORKSPACE?: boolean;
  WIPE_ASSETS?: boolean;
  LAUNCH_PARTITION_BACKFILL?: boolean;
  CANCEL_PARTITION_BACKFILL?: boolean;
};

export const PERMISSIONS_ALLOW_ALL: PermissionsFromJSON = {
  LAUNCH_PIPELINE_EXECUTION: true,
  LAUNCH_PIPELINE_REEXECUTION: true,
  RECONCILE_SCHEDULER_STATE: true,
  START_SCHEDULE: true,
  STOP_RUNNING_SCHEDULE: true,
  START_SENSOR: true,
  STOP_SENSOR: true,
  TERMINATE_PIPELINE_EXECUTION: true,
  DELETE_PIPELINE_RUN: true,
  RELOAD_REPOSITORY_LOCATION: true,
  RELOAD_WORKSPACE: true,
  WIPE_ASSETS: true,
  LAUNCH_PARTITION_BACKFILL: true,
  CANCEL_PARTITION_BACKFILL: true,
};
export const DISABLED_MESSAGE = 'Disabled by your administrator';

export const usePermissions = () => {
  const appContext = React.useContext(AppContext);
  const {permissions} = appContext;

  return React.useMemo(
    () => ({
      canLaunchPipelineExecution: !!permissions.LAUNCH_PIPELINE_EXECUTION,
      canLaunchPipelineReexecution: !!permissions.LAUNCH_PIPELINE_REEXECUTION,
      canReconcileSchedulerState: !!permissions.RECONCILE_SCHEDULER_STATE,
      canStartSchedule: !!permissions.START_SCHEDULE,
      canStopRunningSchedule: !!permissions.STOP_RUNNING_SCHEDULE,
      canStartSensor: !!permissions.START_SENSOR,
      canStopSensor: !!permissions.STOP_SENSOR,
      canTerminatePipelineExecution: !!permissions.TERMINATE_PIPELINE_EXECUTION,
      canDeletePipelineRun: !!permissions.DELETE_PIPELINE_RUN,
      canReloadRepositoryLocation: !!permissions.RELOAD_REPOSITORY_LOCATION,
      canReloadWorkspace: !!permissions.RELOAD_WORKSPACE,
      canWipeAssets: !!permissions.WIPE_ASSETS,
      canLaunchPartitionBackfill: !!permissions.LAUNCH_PARTITION_BACKFILL,
      canCancelPartitionBackfill: !!permissions.CANCEL_PARTITION_BACKFILL,
    }),
    [permissions],
  );
};

export type PermissionSet = ReturnType<typeof usePermissions>;
