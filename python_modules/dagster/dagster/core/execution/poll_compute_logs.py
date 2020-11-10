from __future__ import print_function

import os
import sys
import time
from collections import namedtuple

from dagster.serdes import whitelist_for_serdes
from dagster.serdes.ipc import ipc_write_stream
from dagster.utils import delay_interrupts, pop_delayed_interrupts

POLLING_INTERVAL = 0.1


@whitelist_for_serdes
class WindowsTailProcessStartedEvent(namedtuple("WindowsTailProcessStartedEvent", "")):
    pass


def current_process_is_orphaned(parent_pid):
    parent_pid = int(parent_pid)
    if sys.platform == "win32":
        import psutil  # pylint: disable=import-error

        try:
            parent = psutil.Process(parent_pid)
            return parent.status() != psutil.STATUS_RUNNING
        except psutil.NoSuchProcess:
            return True

    else:
        return os.getppid() != parent_pid


def tail_polling(filepath, stream=sys.stdout, parent_pid=None):
    """
    Tails a file and outputs the content to the specified stream via polling.
    The pid of the parent process (if provided) is checked to see if the tail process should be
    terminated, in case the parent is hard-killed / segfaults
    """
    with open(filepath, "r") as file:
        for block in iter(lambda: file.read(1024), None):
            if block:
                print(block, end="", file=stream)  # pylint: disable=print-call
            else:
                if pop_delayed_interrupts() or (
                    parent_pid and current_process_is_orphaned(parent_pid)
                ):
                    return
                time.sleep(POLLING_INTERVAL)


def execute_polling(args):
    if not args or len(args) != 3:
        return

    filepath = args[0]
    parent_pid = int(args[1])
    ipc_output_file = args[2]

    # Signal to the calling process that we have started and are
    # ready to receive the signal to terminate once execution has finished
    with ipc_write_stream(ipc_output_file) as ipc_stream:
        ipc_stream.send(WindowsTailProcessStartedEvent())

    tail_polling(filepath, sys.stdout, parent_pid)


if __name__ == "__main__":
    with delay_interrupts():
        execute_polling(sys.argv[1:])
