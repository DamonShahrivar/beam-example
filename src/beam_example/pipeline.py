import json
import typing
import logging
from typing import Tuple
from typing import List
from typing import TypeVar

import apache_beam as beam
from apache_beam.transforms.combiners import Top
from apache_beam.transforms import window
from apache_beam.transforms import trigger
from apache_beam.options.pipeline_options import StandardOptions

from beam_example.options import ExampleOptions


K = TypeVar("K")
V = TypeVar("V")

DAY_IN_SECONDS = 3600 * 24


@beam.typehints.with_input_types(Tuple[K, V])
@beam.typehints.with_output_types(Tuple[K, List[V]])
class CombineSequencesByKey(beam.PTransform):
    def __init__(self, key, n=20, sequence_time_to_live_s=DAY_IN_SECONDS):
        super().__init__()
        self.key = key
        self.top_n = n
        self.duration = sequence_time_to_live_s

    def expand(self, pcoll):
        return (
            pcoll
            | "EventWindowing"
            >> beam.WindowInto(
                window.Sessions(self.duration),
                trigger=trigger.Repeatedly(trigger.AfterCount(1)),
                accumulation_mode=trigger.AccumulationMode.ACCUMULATING,
            )
            | "LastSequence" >> Top().PerKey(self.top_n, key=self.key, reverse=True)
        )


class Event(typing.NamedTuple):
    timestamp: int
    data: str


class User(typing.NamedTuple):
    id: str


@beam.typehints.with_input_types(Tuple[User, List[Event]])
@beam.typehints.with_output_types(str)
class FormatOutput(beam.DoFn):
    def process(self, element):
        event_history = [e.data for e in element[1]]
        yield json.dumps({"id": element[0].id, "seq": event_history})


# debugging class
class PrintFn(beam.DoFn):
    def __init__(self, label):
        self.label = label

    def process(
        self, element, timestamp=beam.DoFn.TimestampParam, window=beam.DoFn.WindowParam
    ):
        # Log at INFO level each element processed.
        logging.info("[%s]: %s %s %s", self.label, element, window, timestamp)
        yield element


def run(argv=None):
    pipeline_options = ExampleOptions(argv)
    pipeline_options.view_as(StandardOptions).streaming = True
    with beam.Pipeline(options=pipeline_options) as p:
        if pipeline_options.input_topic:
            messages = p | "ReadPubSub" >> beam.io.ReadFromPubSub(
                topic=pipeline_options.input_topic
            )
        else:
            messages = p | "ReadPubSub" >> beam.io.ReadFromPubSub(
                subscription=pipeline_options.input_subscription
            )

        def parse_input(element: bytes):
            msg = json.loads(element)
            return (
                User(id=msg["id"]),
                Event(timestamp=msg["timestamp"], data=msg["data"]),
            )

        combiner = CombineSequencesByKey(key=lambda e: e.timestamp)

        output = (
            messages
            | "decode" >> beam.Map(lambda e: e.decode("utf-8"))
            | "parse" >> beam.Map(parse_input).with_output_types(Tuple[User, Event])
            | "inputDebug" >> beam.ParDo(PrintFn("input"))
            | "aggregate" >> combiner
            | "format" >> beam.ParDo(FormatOutput())
            | "outputDebug" >> beam.ParDo(PrintFn("output"))
            | "encode" >> beam.Map(lambda e: e.encode("utf-8")).with_output_types(bytes)
        )

        output | beam.io.WriteToPubSub(pipeline_options.output_topic)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
