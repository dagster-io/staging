import io
import os
import pickle
import sys
import time
from queue import Empty, Queue
from threading import Thread

from dagster.core.execution.plan.external_step import PICKLED_EVENTS_FILE_NAME, run_step_from_ref
from dagster.core.instance import DagsterInstance
from google.cloud import storage  # type: ignore[attr-defined]

DONE = object()


def main(project_id, step_run_ref_bucket, gcs_dir_key):
    client = storage.client.Client(project=project_id)
    bucket = client.get_bucket(step_run_ref_bucket)

    buffer = io.BytesIO()

    bucket.blob(gcs_dir_key).download_to_file(buffer)
    buffer.seek(0)

    step_run_ref = pickle.loads(buffer.read())

    print(f"### {project_id}")
    print(f"### {step_run_ref_bucket}")
    print(f"### {gcs_dir_key}")
    print(f"### {step_run_ref}")

    events_gcs_key = os.path.dirname(gcs_dir_key) + "/" + PICKLED_EVENTS_FILE_NAME

    def put_events(events):
        file_obj = io.BytesIO(pickle.dumps(events))
        bucket.blob(events_gcs_key).upload_from_file(file_obj)

    # Set up a thread to handle writing events back to the plan process, so execution doesn't get
    # blocked on remote communication
    events_queue = Queue()
    event_writing_thread = Thread(
        target=event_writing_loop,
        kwargs=dict(events_queue=events_queue, put_events_fn=put_events),
    )
    event_writing_thread.start()

    with DagsterInstance.ephemeral() as instance:
        try:
            for event in run_step_from_ref(step_run_ref, instance):
                events_queue.put(event)
        finally:
            events_queue.put(DONE)
            event_writing_thread.join()


def event_writing_loop(events_queue, put_events_fn):
    """
    Periodically check whether the step has posted any new events to the queue.  If they have,
    write ALL events (not just the new events) to an GCS bucket.

    This approach was motivated by a few challenges:
    * We can't expect a process on EMR to be able to hit an endpoint in the plan process, because
      the plan process might be behind someone's home internet.
    * We can't expect the plan process to be able to hit an endpoint in the process on EMR, because
      EMR is often behind a VPC.
    """
    all_events = []

    done = False
    got_new_events = False
    time_posted_last_batch = time.time()
    while not done:
        try:
            event_or_done = events_queue.get(timeout=1)
            if event_or_done == DONE:
                done = True
            else:
                all_events.append(event_or_done)
                got_new_events = True
        except Empty:
            pass

        enough_time_between_batches = time.time() - time_posted_last_batch > 1
        if got_new_events and (done or enough_time_between_batches):
            put_events_fn(all_events)
            got_new_events = False
            time_posted_last_batch = time.time()


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2], sys.argv[3])
