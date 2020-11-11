from dagster.grpc.client import DagsterGrpcClient
from dagster.grpc.server_watcher import create_grpc_watch_thread
from dagster.grpc.server import open_server_process
from dagster.serdes.ipc import interrupt_ipc_subprocess_pid
from dagster.utils import find_free_port
import time


def test_run_grpc_watch_thread():
    client = DagsterGrpcClient(port=8080)
    shutdown_event, watch_thread = create_grpc_watch_thread(client)

    watch_thread.start()
    shutdown_event.set()
    watch_thread.join()


def test_grpc_watch_thread_server_update():
    port = find_free_port()

    called = {}

    def on_updated():
        called["yup"] = True

    # Create initial server
    server_process = open_server_process(port=port, socket=None)

    try:
        # Start watch thread
        client = DagsterGrpcClient(port=port)
        watch_interval = 1
        shutdown_event, watch_thread = create_grpc_watch_thread(
            client, on_updated=on_updated, watch_interval=watch_interval
        )
        watch_thread.start()

        # Wait three seconds, simulate restart server, wait three seconds
        time.sleep(watch_interval * 2)
        client.update_server_id()
        time.sleep(watch_interval * 2)

        shutdown_event.set()
        watch_thread.join()

        assert called
    finally:
        interrupt_ipc_subprocess_pid(server_process.pid)


def test_grpc_watch_thread_server_reconnect():
    port = find_free_port()
    fixed_server_id = "fixed_id"

    called = {}
    should_not_call = {}

    def on_disconnect():
        called["on_disconnect"] = True

    def on_reconnected():
        called["on_reconnected"] = True

    def should_not_be_called():
        should_not_call["yup"] = True

    # Create initial server
    server_process = open_server_process(port=port, socket=None, fixed_server_id=fixed_server_id)

    # Start watch thread
    client = DagsterGrpcClient(port=port)
    watch_interval = 1
    shutdown_event, watch_thread = create_grpc_watch_thread(
        client,
        on_disconnect=on_disconnect,
        on_reconnected=on_reconnected,
        on_updated=should_not_be_called,
        on_error=should_not_be_called,
        watch_interval=watch_interval,
    )
    watch_thread.start()

    # Wait three seconds, simulate restart server, wait three seconds
    time.sleep(watch_interval * 2)
    interrupt_ipc_subprocess_pid(server_process.pid)
    time.sleep(watch_interval * 2)
    server_process = open_server_process(port=port, socket=None, fixed_server_id=fixed_server_id)
    time.sleep(watch_interval * 2)

    shutdown_event.set()
    watch_thread.join()

    assert called["on_disconnect"]
    assert called["on_reconnected"]
    assert not should_not_call


def test_grpc_watch_thread_server_error():
    port = find_free_port()
    fixed_server_id = "fixed_id"

    called = {}
    should_not_call = {}

    def on_disconnect():
        called["on_disconnect"] = True

    def on_error():
        called["on_error"] = True

    def should_not_be_called():
        should_not_call["yup"] = True

    # Create initial server
    server_process = open_server_process(port=port, socket=None, fixed_server_id=fixed_server_id)

    # Start watch thread
    client = DagsterGrpcClient(port=port)
    watch_interval = 1
    max_reconnect_attempts = 3
    shutdown_event, watch_thread = create_grpc_watch_thread(
        client,
        on_disconnect=on_disconnect,
        on_reconnected=should_not_be_called,
        on_updated=should_not_be_called,
        on_error=on_error,
        watch_interval=watch_interval,
        max_reconnect_attempts=max_reconnect_attempts,
    )
    watch_thread.start()

    # Wait three seconds, simulate restart server, wait three seconds
    time.sleep(watch_interval * 2)
    interrupt_ipc_subprocess_pid(server_process.pid)
    time.sleep(watch_interval * max_reconnect_attempts + 1)

    shutdown_event.set()
    watch_thread.join()

    assert called["on_disconnect"]
    assert called["on_error"]
    assert not should_not_call


def test_grpc_watch_thread_server_complex_cycle():
    # Server goes down, comes back up as the same server three times, then goes away and comes
    # back as a new server

    port = find_free_port()
    fixed_server_id = "fixed_id"

    events = []

    def on_disconnect():
        events.append("on_disconnect")

    def on_reconnected():
        events.append("on_reconnected")

    def on_updated():
        events.append("on_updated")

    def on_error():
        events.append("on_error")

    # Create initial server
    server_process = open_server_process(port=port, socket=None, fixed_server_id=fixed_server_id)

    # Start watch thread
    client = DagsterGrpcClient(port=port)
    watch_interval = 1
    shutdown_event, watch_thread = create_grpc_watch_thread(
        client,
        on_disconnect=on_disconnect,
        on_reconnected=on_reconnected,
        on_updated=on_updated,
        on_error=on_error,
        watch_interval=watch_interval,
    )
    watch_thread.start()

    time.sleep(watch_interval * 2)
    for _ in range(3):
        # Wait three seconds, simulate restart server, wait three seconds
        interrupt_ipc_subprocess_pid(server_process.pid)
        time.sleep(watch_interval * 2)
        server_process = open_server_process(
            port=port, socket=None, fixed_server_id=fixed_server_id
        )
        time.sleep(watch_interval * 2)

    interrupt_ipc_subprocess_pid(server_process.pid)
    server_process = open_server_process(port=port, socket=None)
    time.sleep(watch_interval * 2)

    shutdown_event.set()
    watch_thread.join()

    assert events == ["on_disconnect", "on_reconnected"] * 3 + ["on_disconnect", "on_updated"]


def test_grpc_watch_thread_server_complex_cycle_2():
    # Server goes down, comes back up as the same server three times, then goes away entirely

    port = find_free_port()
    fixed_server_id = "fixed_id"

    events = []

    def on_disconnect():
        events.append("on_disconnect")

    def on_reconnected():
        events.append("on_reconnected")

    def on_updated():
        events.append("on_updated")

    def on_error():
        events.append("on_error")

    # Create initial server
    server_process = open_server_process(port=port, socket=None, fixed_server_id=fixed_server_id)

    # Start watch thread
    client = DagsterGrpcClient(port=port)
    watch_interval = 1
    max_reconnect_attempts = 2
    shutdown_event, watch_thread = create_grpc_watch_thread(
        client,
        on_disconnect=on_disconnect,
        on_reconnected=on_reconnected,
        on_updated=on_updated,
        on_error=on_error,
        watch_interval=watch_interval,
        max_reconnect_attempts=max_reconnect_attempts,
    )
    watch_thread.start()

    time.sleep(watch_interval * 2)
    for _ in range(3):
        # Wait three seconds, simulate restart server, wait three seconds
        interrupt_ipc_subprocess_pid(server_process.pid)
        time.sleep(watch_interval * 2)
        server_process = open_server_process(
            port=port, socket=None, fixed_server_id=fixed_server_id
        )
        time.sleep(watch_interval * 2)

    interrupt_ipc_subprocess_pid(server_process.pid)
    time.sleep(watch_interval * max_reconnect_attempts + 1)

    shutdown_event.set()
    watch_thread.join()

    assert events == ["on_disconnect", "on_reconnected"] * 3 + ["on_disconnect", "on_error"]


def test_run_grpc_watch_without_server():
    # Starting a thread for a server that never existed should immediately error out

    client = DagsterGrpcClient(port=8080)
    watch_interval = 1
    max_reconnect_attempts = 1

    called = {}
    should_not_call = {}

    def on_disconnect():
        called["on_disconnect"] = True

    def on_error():
        called["on_error"] = True

    def should_not_be_called():
        should_not_call["yup"] = True

    shutdown_event, watch_thread = create_grpc_watch_thread(
        client,
        on_disconnect=on_disconnect,
        on_reconnected=should_not_be_called,
        on_updated=should_not_be_called,
        on_error=on_error,
        watch_interval=watch_interval,
        max_reconnect_attempts=max_reconnect_attempts,
    )

    watch_thread.start()
    time.sleep(watch_interval * max_reconnect_attempts + 1)

    shutdown_event.set()
    watch_thread.join()

    assert called["on_disconnect"]
    assert called["on_error"]
    assert not should_not_call
