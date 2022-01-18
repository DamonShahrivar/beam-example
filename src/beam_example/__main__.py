import sys
import logging

import beam_example.pipeline


if __name__ == "__main__":
    logging.getLogger().setLevel(level=logging.INFO)
    beam_example.pipeline.run(sys.argv)
