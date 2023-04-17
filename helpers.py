import logging
import pathlib

from typing import Union, Sequence
from ordered_set import OrderedSet
from elasticsearch import Elasticsearch
from flags import ELASTIC_HOST, ELASTIC_USER, ELASTIC_PASSWORD, ELASTIC_CERTS, ELASTIC_INDEX_ENT, ELASTIC_INDEX_REL


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


def connect_to_elasticsearch(user=ELASTIC_USER, password=ELASTIC_PASSWORD):
    """Connect to Elasticsearch client using urls and credentials from args.py"""
    return Elasticsearch(
        ELASTIC_HOST,
        ca_certs=ELASTIC_CERTS,
        basic_auth=(user, password),  # refer to args.py --elastic_password for alternatives
        retry_on_timeout=True,
    )


def _uppercase_sequence(sequence: Union[Sequence[str], OrderedSet[str]], tp):
    if not isinstance(sequence, tp):
        return sequence

    new_sequence = []
    for ent in sequence:
        try:
            new_sequence.append(ent.upper())
        except AttributeError:
            raise AttributeError("Entity in sequence is not a string!")
    return tp(new_sequence)


def uppercase(f):

    def wrap(entry, *args, **kwargs):
        if isinstance(entry, str):
            entry = entry.upper()
        else:
            entry = _uppercase_sequence(entry, OrderedSet)
            entry = _uppercase_sequence(entry, list)
            entry = _uppercase_sequence(entry, tuple)

        return f(entry, *args, **kwargs)

    return wrap


def get_id_by_label(label: str, es_client: Elasticsearch, index: str) -> str:

    response = es_client.search(index=index, query={"term": {"label": label.lower()}})

    if response and response["hits"]["total"]["value"] > 0:
        eid = response["hits"]["hits"][0]["_id"]
        print(f"{eid}")
        return response["hits"]["hits"][0]["_id"]
    else:
        print(f"Warning! ID not found, returning original label {label}")
        return label


@uppercase
def _get_label(gid: str, index: str, es_client: Elasticsearch):
    """Generic get label for given gid in given index if it exists"""
    if not es_client.exists(index=index, id=gid):
        print(f"get_label in ESActionOperator: entity with {gid} doesn't exist in {index}.")
        print(f"returning {gid}")
        return gid

    return es_client.get(index=index, id=gid)['_source']['label']


@uppercase
def get_entity_label(eid: str, es_client: Elasticsearch):
    """Get entity label for given entity (eid) if it exists"""
    index = ELASTIC_INDEX_ENT
    return _get_label(eid, index, es_client)


@uppercase
def get_relation_label(rid: str, es_client: Elasticsearch):
    """Get relation label for given relation (rid) if it exists"""
    index = ELASTIC_INDEX_REL
    return _get_label(rid, index, es_client)
