import logging
import unittest
import uuid
import json
import threading
import time

from hamcrest.core.core.allof import all_of
from apache_beam.io.gcp.tests.pubsub_matcher import PubSubMessageMatcher
from apache_beam.runners.runner import PipelineState
from apache_beam.testing.pipeline_verifiers import PipelineStateMatcher
from apache_beam.testing.test_pipeline import TestPipeline


import beam_example.pipeline


PUBSUB_WAIT_TIME_S = 3 * 60
PIPELINE_WAIT_TIME_MS = 120 * 1000

INPUT_TOPIC = "integration_test_topic_input"
OUTPUT_TOPIC = "integration_test_topic_output"
INPUT_SUB = "integration_test_subscription_input"
OUTPUT_SUB = "integration_test_subscription_output"


TEST_CASE = [
    {"id": "x", "timestamp": 1, "data": "A"},
    {"id": "x", "timestamp": 2, "data": "B"},
    {"id": "x", "timestamp": 3, "data": "C"},
    {"id": "x", "timestamp": 4, "data": "D"},
]

EXPECTED_OUTPUT = [
    {"id": "x", "seq": ["A"]},
    {"id": "x", "seq": ["A", "B"]},
    {"id": "x", "seq": ["A", "B", "C"]},
    {"id": "x", "seq": ["A", "B", "C", "D"]},
]


class StreamingIT(unittest.TestCase):
    def setUp(self):
        self.test_pipeline = TestPipeline(is_integration_test=True)
        self.project = self.test_pipeline.get_option("project")
        self.setup_pubsub()

    def setup_pubsub(self):
        self.uuid = str(uuid.uuid4())
        from google.cloud import pubsub

        self.pub_client = pubsub.PublisherClient()
        self.input_topic = self.pub_client.create_topic(
            name=self.pub_client.topic_path(self.project, INPUT_TOPIC + self.uuid)
        )
        self.output_topic = self.pub_client.create_topic(
            name=self.pub_client.topic_path(self.project, OUTPUT_TOPIC + self.uuid)
        )

        self.sub_client = pubsub.SubscriberClient()
        self.input_sub = self.sub_client.create_subscription(
            name=self.sub_client.subscription_path(self.project, INPUT_SUB + self.uuid),
            topic=self.input_topic.name,
        )
        self.output_sub = self.sub_client.create_subscription(
            name=self.sub_client.subscription_path(
                self.project, OUTPUT_SUB + self.uuid
            ),
            topic=self.output_topic.name,
            ack_deadline_seconds=60,
        )

    def tearDown(self):
        for sub in [self.input_sub, self.output_sub]:
            self.sub_client.delete_subscription(subscription=sub.name)

        for topic in [self.input_topic, self.output_topic]:
            self.pub_client.delete_topic(topic=topic.name)

    def _inject_events(self, events):
        time.sleep(3)
        for msg in events:
            time.sleep(1)
            self.pub_client.publish(
                self.input_topic.name, json.dumps(msg).encode("utf-8")
            )
            print(msg)

    def test_streaming_sequencer_it(self):
        expected_pub_sub_messages = [
            json.dumps(msg).encode("utf-8") for msg in EXPECTED_OUTPUT
        ]
        state_verifier = PipelineStateMatcher(PipelineState.RUNNING)
        pubsub_msg_verifier = PubSubMessageMatcher(
            self.project,
            self.output_sub.name,
            expected_pub_sub_messages,
            timeout=PUBSUB_WAIT_TIME_S,
        )
        extra_opts = {
            "input_subscription": self.input_sub.name,
            "output_topic": self.output_topic.name,
            "wait_until_finish_duration": PIPELINE_WAIT_TIME_MS,
            "on_success_matcher": all_of(state_verifier, pubsub_msg_verifier),
        }

        event_publisher = threading.Thread(target=self._inject_events, args=[TEST_CASE])

        event_publisher.daemon = True
        event_publisher.start()

        beam_example.pipeline.run(
            self.test_pipeline.get_full_options_as_args(**extra_opts)
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.DEBUG)
    unittest.main()
