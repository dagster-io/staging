import * as React from 'react';

export type CountdownStatus = 'counting' | 'idle';

export interface Props {
  duration: number;
  interval: number;
  message: (timeRemaining: number) => React.ReactNode;
  onComplete?: () => void;
  status: CountdownStatus;
}

/**
 * A controlled component that displays a countdown for a specified duration and interval.
 *
 * - duration
 *      The length of the countdown
 * - interval
 *      The interval at which the countdown will tick down
 * - message
 *      A function to render the state of the countdown
 * - onComplete
 *      A function to indicate when the countdown has reached zero, signaling to the
 *      parent that `status` may be updated
 * - status
 *      Whether the countdown should be in progress ("counting") or idle. An effect
 *      triggers the countdown to begin when this value changes to "counting".
 */
export const Countdown = (props: Props) => {
  const {duration, interval, message, onComplete, status} = props;

  const [remainingTime, setRemainingTime] = React.useState<number>(duration);
  const token = React.useRef<ReturnType<typeof setInterval> | null>(null);

  const clearToken = React.useCallback(() => {
    token.current && clearInterval(token.current);
  }, []);

  React.useEffect(() => {
    clearToken();
    if (status === 'counting') {
      setRemainingTime(duration);
      token.current = setInterval(() => {
        setRemainingTime((current) => Math.max(0, current - interval));
      }, interval);
    }
  }, [clearToken, duration, interval, status]);

  React.useEffect(() => {
    if (remainingTime === 0) {
      clearToken();
      onComplete && onComplete();
    }
  }, [clearToken, onComplete, remainingTime]);

  return <>{message(remainingTime)}</>;
};

Countdown.defaultProps = {
  interval: 1000,
};
