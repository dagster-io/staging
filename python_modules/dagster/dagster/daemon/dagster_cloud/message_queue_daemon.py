import redis
from dagster.cloud.daemon_message_queue import DaemonMessageQueue
from dagster.daemon.daemon import DagsterDaemon
from dagster.daemon.types import DaemonType


class MessageQueueDaemon(DagsterDaemon):
    def __init__(self, instance):
        self._message_queue = DaemonMessageQueue()
        super(MessageQueueDaemon, self).__init__(instance, 1)

    @classmethod
    def daemon_type(cls):
        return DaemonType.MESSAGE_QUEUE

    def event_callback(self, body, message):
        print("RECEIVED: " + str(body))
        message.ack()

        # this will send a response back over dagster-cloud graphql

        # But for now! We will directly do the thing that the graphql API
        # will someday do.

        r = redis.Redis(host="localhost", port=6379, db=0)
        r.set("daemon-response-" + body["request_id"], "YOUR RESPONSE HERE!!")

    def run_iteration(self):
        queue = self._message_queue

        # Should we just leave this open on startup (yes)
        queue.drain_requests(self.event_callback)
        yield
