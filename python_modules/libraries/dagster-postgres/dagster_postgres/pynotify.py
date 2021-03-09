# copied from https://github.com/djrobstep/pgnotify/blob/43bbe7bd3cedfb99700e4ab370cb6f5d7426bea3/pgnotify/notify.py

# This is free and unencumbered software released into the public domain.

# Anyone is free to copy, modify, publish, use, compile, sell, or
# distribute this software, either in source code form or as a compiled
# binary, for any purpose, commercial or non-commercial, and by any
# means.

# In jurisdictions that recognize copyright laws, the author or authors
# of this software dedicate any and all copyright interest in the
# software to the public domain. We make this dedication for the benefit
# of the public at large and to the detriment of our heirs and
# successors. We intend this dedication to be an overt act of
# relinquishment in perpetuity of all present and future rights to this
# software under copyright law.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.

# For more information, please refer to <http://unlicense.org>

import errno
import os
import select
import signal
import sys
from threading import Event
from typing import Iterator, List, Optional, Union

from dagster import check
from psycopg2.extensions import Notify

from .utils import get_conn


def get_wakeup_fd():
    pipe_r, pipe_w = os.pipe()
    if "win" not in sys.platform:
        import fcntl

        flags = fcntl.fcntl(pipe_w, fcntl.F_GETFL, 0)
        flags = os.O_NONBLOCK
        flags = fcntl.fcntl(pipe_w, fcntl.F_SETFL, flags)
    signal.set_wakeup_fd(pipe_w)
    return pipe_r


def _empty_handler(_signal, _frame):
    pass


def quote_table_name(name):
    return '"{}"'.format(name)


def start_listening(connection, channels):
    names = (quote_table_name(each) for each in channels)
    listens = "; ".join(["LISTEN {}".format(n) for n in names])

    with connection.cursor() as curs:
        curs.execute(listens)


def construct_signals(arg):
    # function exists to consolidate and scope pylint directive
    return signal.Signals(arg)  # pylint: disable=no-member


def await_pg_notifications(
    conn_string: str,
    channels: Optional[List[str]] = None,
    timeout: float = 5.0,
    yield_on_timeout: bool = False,
    handle_signals: Optional[List[int]] = None,
    exit_event: Optional[Event] = None,
) -> Iterator[Union[int, Optional[Notify]]]:
    """Subscribe to PostgreSQL notifications, and handle them
    in infinite-loop style.

    Args:
        conn_string (str): connection string to PG DB
        channels (Optional[List[str]], optional): List of channel names to listen to. Defaults to None.
        timeout (float, optional): Timeout interval. Defaults to 5.0.
        yield_on_timeout (bool, optional): Should the function yield on timeout. Defaults to False.
        handle_signals (Optional[List[int]], optional): List of OS signal numbers to handle. Defaults to None.
        exit_event (Optional[Event], optional): Event that indicates that polling for new notifications should stop. Defaults to None.

    Yields:
        Iterator[Union[int, Optional[Notify]]]: Can yield one of three types:
            1: None, in case of timeout
            2: int, in case of a handled signal
            3: Notify, in case of successful notification reception
    """

    check.str_param(conn_string, "conn_string")
    channels = None if channels is None else check.list_param(channels, "channels", of_type=str)
    check.float_param(timeout, "timeout")
    check.bool_param(yield_on_timeout, "yield_on_timeout")

    conn = get_conn(conn_string)

    if channels:
        start_listening(conn, channels)

    signals_to_handle = handle_signals or []
    original_handlers = {}

    try:
        if signals_to_handle:
            original_handlers = {s: signal.signal(s, _empty_handler) for s in signals_to_handle}
            wakeup = get_wakeup_fd()
            listen_on = [conn, wakeup]
        else:
            listen_on = [conn]
            wakeup = None

        while True and not (exit_event and exit_event.is_set()):
            try:
                r, w, x = select.select(listen_on, [], [], max(0, timeout))
                if (r, w, x) == ([], [], []):
                    if yield_on_timeout:
                        yield None

                if wakeup is not None and wakeup in r:
                    signal_byte = os.read(wakeup, 1)
                    signal_int = int.from_bytes(signal_byte, sys.byteorder)
                    yield signal_int

                if conn in r:
                    conn.poll()

                    notify_list, conn.notifies = conn.notifies, []
                    for notif in notify_list:
                        yield notif

            except select.error as e:
                if e.errno == errno.EINTR:
                    pass
                else:
                    raise
    finally:
        conn.close()
        for s in signals_to_handle or []:
            if s in original_handlers:
                # Commenting out to get pylint to pass
                # https://github.com/dagster-io/dagster/issues/2510
                # signal_name = construct_signals(s).name
                signal.signal(s, original_handlers[s])
