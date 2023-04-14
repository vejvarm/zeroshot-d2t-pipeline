import logging
import pathlib


def setup_logger(name=__name__, loglevel=logging.DEBUG, handlers=None, output_log_file: pathlib.Path or str = None):
    if handlers is None:
        handlers = [logging.StreamHandler()]
    if output_log_file:
        file_handler = logging.FileHandler(output_log_file, mode="w", encoding="utf-8")
        handlers.append(file_handler)

    logger = logging.getLogger(name)
    logger.setLevel(loglevel)

    formatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                                   datefmt='%d/%m/%Y %I:%M:%S %p')

    for handler in handlers:
        handler.setLevel(loglevel)
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    # disable PUT INFO responses from ElasticSearch search command
    logging.getLogger('elastic_transport.transport').setLevel(logging.WARNING)

    return logger