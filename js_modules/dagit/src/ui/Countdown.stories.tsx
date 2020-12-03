import {Button} from '@blueprintjs/core';
import {Meta} from '@storybook/react/types-6-0';
import * as React from 'react';

import {Countdown} from 'src/ui/Countdown';
import {Group} from 'src/ui/Group';

// eslint-disable-next-line import/no-default-export
export default {
  title: 'Countdown',
  component: Countdown,
} as Meta;

export const FiveSeconds = () => {
  const [status, setStatus] = React.useState<'counting' | 'idle'>('idle');

  const onComplete = React.useCallback(() => setStatus('idle'), []);

  const message = (timeRemaining: number) => {
    if (status === 'idle') {
      return <div>Waiting for refresh…</div>;
    }
    const seconds = Math.floor(timeRemaining / 1000);
    return <div>{`Refresh in 0:${seconds < 10 ? `0${seconds}` : seconds}`}</div>;
  };

  return (
    <Group direction="vertical" spacing={12}>
      <Group direction="horizontal" spacing={8}>
        <Button onClick={() => setStatus('counting')}>Set counting</Button>
        <Button onClick={() => setStatus('idle')}>Set idle</Button>
      </Group>
      <Countdown duration={5000} message={message} onComplete={onComplete} status={status} />
    </Group>
  );
};
