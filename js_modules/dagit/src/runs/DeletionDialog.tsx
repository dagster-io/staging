import {useMutation} from '@apollo/client';
import {Button, Classes, Colors, Dialog, Icon, ProgressBar} from '@blueprintjs/core';
import * as React from 'react';

import {DELETE_MUTATION} from 'src/runs/RunUtils';
import {
  Delete,
  Delete_deletePipelineRun_PipelineRunNotFoundError,
  Delete_deletePipelineRun_PythonError,
} from 'src/runs/types/Delete';
import {Group} from 'src/ui/Group';
import {FontFamily} from 'src/ui/styles';

export interface Props {
  isOpen: boolean;
  onClose: () => void;
  onComplete: () => void;
  onTerminateInstead: () => void;
  selectedIDs: string[];
  terminatableIDs: string[];
}

type Error =
  | Delete_deletePipelineRun_PythonError
  | Delete_deletePipelineRun_PipelineRunNotFoundError
  | undefined;

type DeletionDialogState = {
  step: 'initial' | 'deleting' | 'completed';
  deletion: {completed: number; errors: {[id: string]: Error}};
};

type DeletionDialogAction =
  | {type: 'start'}
  | {type: 'deletion-success'}
  | {type: 'deletion-error'; id: string; error: Error}
  | {type: 'complete'};

type Reducer = (
  prevState: DeletionDialogState,
  action: DeletionDialogAction,
) => DeletionDialogState;

const deletionDialogReducer = (
  prevState: DeletionDialogState,
  action: DeletionDialogAction,
): DeletionDialogState => {
  switch (action.type) {
    case 'start':
      return {...prevState, step: 'deleting'};
    case 'deletion-success': {
      const {deletion} = prevState;
      return {
        ...prevState,
        step: 'deleting',
        deletion: {...deletion, completed: deletion.completed + 1},
      };
    }
    case 'deletion-error': {
      const {deletion} = prevState;
      return {
        ...prevState,
        step: 'deleting',
        deletion: {
          ...deletion,
          completed: deletion.completed + 1,
          errors: {...deletion.errors, [action.id]: action.error},
        },
      };
    }
    case 'complete':
      return {...prevState, step: 'completed'};
  }
};

export const DeletionDialog = (props: Props) => {
  const {isOpen, onClose, onComplete, onTerminateInstead, selectedIDs} = props;
  const [state, dispatch] = React.useReducer<Reducer>(deletionDialogReducer, {
    step: 'initial',
    deletion: {completed: 0, errors: {}},
  });

  const count = selectedIDs.length;
  const terminatableCount = props.terminatableIDs.length;

  const [destroy] = useMutation<Delete>(DELETE_MUTATION);

  const mutate = async () => {
    // Warn against window being closed.

    // Begin deletion.
    dispatch({type: 'start'});

    for (let ii = 0; ii < selectedIDs.length; ii++) {
      const runId = selectedIDs[ii];
      const {data} = await destroy({variables: {runId}});

      // Testing:
      if (data?.deletePipelineRun.__typename === 'DeletePipelineRunSuccess') {
        dispatch({type: 'deletion-success'});
      } else {
        dispatch({type: 'deletion-error', id: runId, error: data?.deletePipelineRun});
      }
    }

    dispatch({type: 'complete'});
    onComplete();
  };

  const progressContent = () => {
    switch (state.step) {
      case 'initial':
        return (
          <Group direction="vertical" spacing={8}>
            <div>{`${count} ${count === 1 ? 'run' : 'runs'} will be deleted.`}</div>
            <div>
              Deleting runs will not prevent them from continuing to execute, and may result in
              unexpected behavior.
            </div>
            {terminatableCount ? (
              <div>
                {`${terminatableCount} of these runs can be terminated.`}
                <strong>We encourage you to terminate these runs instead of deleting them.</strong>
              </div>
            ) : null}
            <div>Do you wish to continue with deletion?</div>
          </Group>
        );
      case 'deleting':
      case 'completed':
        const value = count > 0 ? state.deletion.completed / count : 1;
        return (
          <Group direction="vertical" spacing={8}>
            <div>Deleting…</div>
            <ProgressBar intent="primary" value={value} animate={value < 1} />
          </Group>
        );
      default:
        return null;
    }
  };

  const buttons = () => {
    switch (state.step) {
      case 'initial':
        return (
          <>
            <Button intent="none" onClick={onClose}>
              Cancel
            </Button>
            <Button intent="danger" onClick={mutate}>
              {`Yes, delete ${`${count} ${count === 1 ? 'run' : 'runs'}`}`}
            </Button>
            {terminatableCount ? (
              <Button intent="primary" onClick={onTerminateInstead}>
                {`Terminate ${`${terminatableCount} ${
                  terminatableCount === 1 ? 'run' : 'runs'
                }`} instead`}
              </Button>
            ) : null}
          </>
        );
      case 'deleting':
        return (
          <Button intent="danger" disabled>
            Deleting…
          </Button>
        );
      case 'completed':
        return (
          <Button intent="primary" onClick={onClose}>
            Done
          </Button>
        );
    }
  };

  const completionContent = () => {
    if (state.step !== 'completed') {
      return null;
    }

    const errors = state.deletion.errors;
    const errorCount = Object.keys(errors).length;
    const successCount = state.deletion.completed - errorCount;

    return (
      <Group direction="vertical" spacing={8}>
        {successCount ? (
          <Group direction="horizontal" spacing={8} alignItems="flex-start">
            <Icon icon="tick-circle" iconSize={16} color={Colors.GREEN3} />
            <div>{`Successfully deleted ${successCount} ${
              successCount === 1 ? 'run' : 'runs'
            }.`}</div>
          </Group>
        ) : null}
        {errorCount ? (
          <Group direction="vertical" spacing={8}>
            <Group direction="horizontal" spacing={8} alignItems="flex-start">
              <Icon icon="warning-sign" iconSize={16} color={Colors.GOLD3} />
              <div>{`Could not delete ${errorCount} ${errorCount === 1 ? 'run' : 'runs'}.`}</div>
            </Group>
            <ul>
              {Object.keys(errors).map((runId) => (
                <li key={runId}>
                  <Group direction="horizontal" spacing={8}>
                    <span style={{fontFamily: FontFamily.monospace}}>{runId.slice(0, 8)}</span>
                    {errors[runId] ? <div>{errors[runId]?.message}</div> : null}
                  </Group>
                </li>
              ))}
            </ul>
          </Group>
        ) : null}
      </Group>
    );
  };

  return (
    <Dialog isOpen={isOpen} title="Delete runs">
      <div className={Classes.DIALOG_BODY}>
        <Group direction="vertical" spacing={24}>
          {progressContent()}
          {completionContent()}
        </Group>
      </div>
      <div className={Classes.DIALOG_FOOTER}>
        <div className={Classes.DIALOG_FOOTER_ACTIONS}>{buttons()}</div>
      </div>
    </Dialog>
  );
};
