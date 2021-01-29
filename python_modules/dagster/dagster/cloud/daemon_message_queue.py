import time
from contextlib import contextmanager

import redis
from dagster.serdes import serialize_dagster_namedtuple
from kombu import Connection, Exchange, Queue
from kombu.messaging import Producer


class DaemonMessageQueue:
    def __init__(self):
        self.task_queue = Queue(
            "dagster_daemon_requests",
            Exchange("dagster_daemon_requests"),
            routing_key="dagster_daemon_requests",
        )

    @contextmanager
    def _conn(self):
        with Connection("redis://localhost:6379/") as conn:
            yield conn

    def drain_requests(self, event_callback):
        with self._conn() as conn:
            consumer = conn.Consumer(self.task_queue)
            consumer.register_callback(event_callback)
            with consumer:
                print("Waiting")
                conn.drain_events()
                print("I WAITED")

    def send_request(self, request_id, request_api_name, request_args):
        with self._conn() as conn:
            with conn.channel() as channel:
                producer = Producer(channel)
                producer.publish(
                    {
                        "request_id": request_id,
                        "request_api_name": request_api_name,
                        "request_body": serialize_dagster_namedtuple(request_args),
                    },
                    retry=True,
                    exchange=self.task_queue.exchange,
                    routing_key=self.task_queue.routing_key,
                    declare=[self.task_queue],
                )

    def wait_for_response(self, request_id):
        r = redis.Redis(host="localhost", port=6379, db=0)

        while True:  # Add Timeout
            result = r.get("daemon-response-" + request_id)

            if result:
                return result

            time.sleep(0.1)


if __name__ == "__main__":
    from dagster.core.definitions.dependency import SolidHandle

    import uuid

    my_request_id = str(uuid.uuid4())

    queue = DaemonMessageQueue()
    queue.send_request(
        my_request_id, "FOOO", SolidHandle("hi", parent=None),
    )
    print("SENT")

    print("WAITING FOR RESPONSE TO COME BACK....")
    my_result = queue.wait_for_response(my_request_id)

    print("GOT THE RESULT: " + str(my_result))
