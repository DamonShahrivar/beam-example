from apache_beam.options.pipeline_options import PipelineOptions

class ExampleOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        input = parser.add_mutually_exclusive_group(required=True)

        input.add_argument(
            "--input_topic",
            help=(
                "Input PubSub topic of the form " '"projects/<PROJECT>/topics/<TOPIC>".'
            ),
        )

        input.add_argument(
            "--input_subscription",
            help=(
                "Input PubSub subscription of the form "
                '"projects/<PROJECT>/subscriptions/<SUBSCRIPTION>."'
            ),
        )

        parser.add_argument(
            "--output_topic",
            required=True,
            help=(
                "Output PubSub topic of the form "
                '"projects/<PROJECT>/topics/<TOPIC>".'
            ),
        )