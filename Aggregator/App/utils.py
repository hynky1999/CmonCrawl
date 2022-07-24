import logging


all_purpose_logger = logging.getLogger("agg_all_purpose_logger")
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
