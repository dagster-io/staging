import {useMutation} from '@apollo/client';
import {Checkbox, Button, Classes, Colors, Dialog, Icon, ProgressBar} from '@blueprintjs/core';
import * as React from 'react';

import {NavigationBlock} from 'src/runs/NavitationBlock';
import {CANCEL_MUTATION} from 'src/runs/RunUtils';
import {
  Cancel,
  Cancel_terminatePipelineExecution_PipelineRunNotFoundError,
  Cancel_terminatePipelineExecution_PythonError,
  Cancel_terminatePipelineExecution_TerminatePipelineExecutionFailure,
} from 'src/runs/types/Cancel';
import {TerminatePipelinePolicy} from 'src/types/globalTypes';
import {Box} from 'src/ui/Box';
import {Group} from 'src/ui/Group';
import {FontFamily} from 'src/ui/styles';

export interface Props {
  isOpen: boolean;
  onClose: () => void;
  onComplete: (terminationState: TerminationState) => void;
  selectedIDs: string[];
}

type Error =
  | Cancel_terminatePipelineExecution_TerminatePipelineExecutionFailure
  | Cancel_terminatePipelineExecution_PipelineRunNotFoundError
  | Cancel_terminatePipelineExecution_PythonError
  | undefined;

export type TerminationState = {completed: number; errors: {[id: string]: Error}};

type TerminationDialogState = {
  mustForce: boolean;
  frozenIDs: string[];
  step: 'initial' | 'terminating' | 'completed';
  termination: TerminationState;
};

const initializeState = (selectedIDs: string[]): TerminationDialogState => {
  return {
    mustForce: false,
    frozenIDs: selectedIDs,
    step: 'initial',
    termination: {completed: 0, errors: {}},
  };
};

type TerminationDialogAction =
  | {type: 'reset'; frozenIDs: string[]}
  | {type: 'toggle-force-terminate'; checked: boolean}
  | {type: 'start'}
  | {type: 'termination-success'}
  | {type: 'termination-error'; id: string; error: Error}
  | {type: 'complete'};

const terminationDialogReducer = (
  prevState: TerminationDialogState,
  action: TerminationDialogAction,
): TerminationDialogState => {
  switch (action.type) {
    case 'reset':
      return initializeState(action.frozenIDs);
    case 'toggle-force-terminate':
      return {...prevState, mustForce: action.checked};
    case 'start':
      return {...prevState, step: 'terminating'};
    case 'termination-success': {
      const {termination} = prevState;
      return {
        ...prevState,
        step: 'terminating',
        termination: {...termination, completed: termination.completed + 1},
      };
    }
    case 'termination-error': {
      const {termination} = prevState;
      return {
        ...prevState,
        step: 'terminating',
        termination: {
          ...termination,
          completed: termination.completed + 1,
          errors: {...termination.errors, [action.id]: action.error},
        },
      };
    }
    case 'complete':
      return {...prevState, step: 'completed'};
  }
};

export const TerminationDialog = (props: Props) => {
  const {isOpen, onClose, onComplete, selectedIDs} = props;

  // Freeze the selected IDs, since the list may change as runs continue processing and
  // terminating. We want to preserve the list we're given.
  const frozenIDs = React.useRef<string[]>(selectedIDs);

  const [state, dispatch] = React.useReducer(
    terminationDialogReducer,
    frozenIDs.current,
    initializeState,
  );

  const count = state.frozenIDs.length;

  // If the dialog is newly open, update state to match the frozen list.
  React.useEffect(() => {
    if (isOpen) {
      dispatch({type: 'reset', frozenIDs: frozenIDs.current});
    }
  }, [isOpen]);

  // If the dialog is not open, update the ref so that the frozen list will be entered
  // into state the next time the dialog opens.
  React.useEffect(() => {
    if (!isOpen) {
      frozenIDs.current = selectedIDs;
    }
  }, [isOpen, selectedIDs]);

  const [terminate] = useMutation<Cancel>(CANCEL_MUTATION);
  const policy = state.mustForce
    ? TerminatePipelinePolicy.MARK_AS_CANCELED_IMMEDIATELY
    : TerminatePipelinePolicy.SAFE_TERMINATE;

  const mutate = async () => {
    dispatch({type: 'start'});

    for (let ii = 0; ii < count; ii++) {
      const runId = state.frozenIDs[ii];
      const {data} = await terminate({variables: {runId, terminatePolicy: policy}});

      if (data?.terminatePipelineExecution.__typename === 'TerminatePipelineExecutionSuccess') {
        dispatch({type: 'termination-success'});
      } else {
        dispatch({type: 'termination-error', id: runId, error: data?.terminatePipelineExecution});
      }
    }

    dispatch({type: 'complete'});
    onComplete(state.termination);
  };

  const onToggleForce = (event: React.ChangeEvent<HTMLInputElement>) => {
    dispatch({type: 'toggle-force-terminate', checked: event.target.checked});
  };

  const progressContent = () => {
    switch (state.step) {
      case 'initial':
        return (
          <Group direction="column" spacing={16}>
            <div>
              {state.mustForce
                ? `${count} ${
                    count === 1 ? 'run' : 'runs'
                  } will be force terminated. Do you wish to continue?`
                : `${count} ${
                    count === 1 ? 'run' : 'runs'
                  } will be terminated. Do you wish to continue?`}
            </div>
            <div>
              <Checkbox
                checked={state.mustForce}
                labelElement={
                  <Box flex={{display: 'inline-flex'}} style={{position: 'relative', top: '-2px'}}>
                    <Group direction="row" spacing={8}>
                      <div style={{position: 'relative', top: '-1px'}}>
                        <Icon icon="warning-sign" iconSize={13} color={Colors.GOLD3} />
                      </div>
                      <div>
                        Force termination immediately. <strong>Warning:</strong> computational
                        resources created by runs may not be fully cleaned up.
                      </div>
                    </Group>
                  </Box>
                }
                onChange={onToggleForce}
              />
            </div>
          </Group>
        );
      case 'terminating':
      case 'completed':
        const value = count > 0 ? state.termination.completed / count : 1;
        return (
          <Group direction="column" spacing={8}>
            <div>{state.mustForce ? 'Forcing termination…' : 'Terminating…'}</div>
            <ProgressBar intent="primary" value={Math.max(0.1, value)} animate={value < 1} />
            {state.step === 'terminating' ? (
              <NavigationBlock message="Termination in progress, please do not navigate away yet." />
            ) : null}
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
              {`${state.mustForce ? 'Force terminate' : 'Terminate'} ${`${count} ${
                count === 1 ? 'run' : 'runs'
              }`}`}
            </Button>
          </>
        );
      case 'terminating':
        return (
          <Button intent="danger" disabled>
            {state.mustForce
              ? `Forcing termination for ${`${count} ${count === 1 ? 'run' : 'runs'}...`}`
              : `Terminating ${`${count} ${count === 1 ? 'run' : 'runs'}...`}`}
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
    if (state.step === 'initial') {
      return null;
    }

    if (state.step === 'terminating') {
      return <div>Please do not close the window or navigate away during termination.</div>;
    }

    const errors = state.termination.errors;
    const errorCount = Object.keys(errors).length;
    const successCount = state.termination.completed - errorCount;

    return (
      <Group direction="column" spacing={8}>
        {successCount ? (
          <Group direction="row" spacing={8} alignItems="flex-start">
            <Icon icon="tick-circle" iconSize={16} color={Colors.GREEN3} />
            <div>
              {state.mustForce
                ? `Successfully forced termination for ${successCount}
                ${successCount === 1 ? 'run' : `runs`}.`
                : `Successfully requested termination for ${successCount}
              ${successCount === 1 ? 'run' : `runs`}.`}
            </div>
          </Group>
        ) : null}
        {errorCount ? (
          <Group direction="column" spacing={8}>
            <Group direction="row" spacing={8} alignItems="flex-start">
              <Icon icon="warning-sign" iconSize={16} color={Colors.GOLD3} />
              <div>
                {state.mustForce
                  ? `Could not force termination for ${errorCount} ${
                      errorCount === 1 ? 'run' : 'runs'
                    }:`
                  : `Could not request termination for ${errorCount} ${
                      errorCount === 1 ? 'run' : 'runs'
                    }:`}
              </div>
            </Group>
            <ul>
              {Object.keys(errors).map((runId) => (
                <li key={runId}>
                  <Group direction="row" spacing={8}>
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

  const canQuicklyClose = state.step !== 'terminating';

  return (
    <Dialog
      isOpen={isOpen}
      title="Terminate runs"
      canEscapeKeyClose={canQuicklyClose}
      canOutsideClickClose={canQuicklyClose}
      isCloseButtonShown={canQuicklyClose}
      onClose={onClose}
    >
      <div className={Classes.DIALOG_BODY}>
        <Group direction="column" spacing={24}>
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
