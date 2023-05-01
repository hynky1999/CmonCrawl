import logging
import sys

"""
By having a logger defined as variables we can import them in other modules
Instead of getting them by name
"""


all_purpose_logger = logging.getLogger("all_purpose_logger")
all_purpose_logger.propagate = False
all_purpose_logger.setLevel(logging.INFO)
if not all_purpose_logger.hasHandlers():
    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter(
            "%(asctime)s - %(filename)s:%(lineno)d - %(levelname)s - %(message)s"
        )
    )
    all_purpose_logger.addHandler(handler)


metadata_logger = logging.getLogger("metadata_logger")
metadata_logger.setLevel(logging.WARN)
metadata_logger.propagate = False
if not metadata_logger.hasHandlers():
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        logging.Formatter(
            "%(asctime)s - %(filename)s:%(lineno)d : %(levelname)s - %(message)s -> ADD_INFO: %(domain_record)s"
        )
    )
    metadata_logger.addHandler(handler)
