import logging
import pathlib
import time

from typing import Union, Sequence

import elastic_transport
import elasticsearch
from ordered_set import OrderedSet
from elasticsearch import Elasticsearch, NotFoundError, ConnectionError
from wikidata.client import Client as WDClient
from wikidata.entity import EntityId
from urllib.error import HTTPError
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


def connect_to_elasticsearch(user=ELASTIC_USER, password=ELASTIC_PASSWORD, num_tries=5):
    """Connect to Elasticsearch client using urls and credentials from args.py"""
    client = Elasticsearch(
        ELASTIC_HOST,
        ca_certs=ELASTIC_CERTS,
        basic_auth=(user, password),  # refer to args.py --elastic_password for alternatives
        retry_on_timeout=True,
    )

    for i in range(num_tries):
        if client.ping():
            return client
        else:
            time.sleep(0.5)

    print("Couldn't connect to elasticsearch database, returning None")
    return None


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
def _get_label(gid: str, id2label_dict: dict, index: str, es_client: Elasticsearch, wd_client: WDClient, logger: logging.Logger):
    """Generic get label for given gid in given index if it exists"""
    if id2label_dict is not None:
        try:
            return id2label_dict[gid]
        except KeyError:
            logger.info(f"entity/relation with {gid} is not in given entity dictionary. Trying Elasticsearch index.")

    if es_client is not None and es_client.exists(index=index, id=gid):
        return es_client.get(index=index, id=gid)['_source']['label']

    logger.info(f"entity with {gid} doesn't exist in {index}. Trying to fetch from online WikiData DB.")
    return _get_english_label_from_wikidata(gid, wd_client, logger)


@uppercase
def _get_english_label_from_wikidata(gid: str or EntityId, wd_client: WDClient, logger: logging.Logger) -> str:
    if not str(gid).startswith(("P", "Q")):
        raise NameError("id must start with 'P' (for predicate/relation) or 'Q' (entity)")

    try:
        # try fetching the entity from WD database
        entity = wd_client.get(EntityId(gid), load=True)
    except HTTPError as err:
        logger.warning(f"{err} (returning original id: {gid})")
        return gid

    # return the predicate label in english
    return entity.label['en']


@uppercase
def get_entity_label(eid: str, ent_dict: dict = None, es_client: Elasticsearch = None,
                     wd_client: WDClient = WDClient(), logger=setup_logger(__name__)):
    """Get entity label for given entity (eid) if it exists"""
    if not str(eid).startswith("Q"):
        raise NameError("id of entity must start with 'Q'")
    index = ELASTIC_INDEX_ENT
    return _get_label(eid, ent_dict, index, es_client, wd_client, logger)


@uppercase
def get_relation_label(rid: str, rel_dict: dict = None, es_client: Elasticsearch = None,
                       wd_client: WDClient = WDClient(), logger=setup_logger(__name__)):
    """Get relation label for given relation (rid) if it exists"""
    if not str(rid).startswith("P"):
        raise NameError("id of predicate (relation) must start with 'P'")
    index = ELASTIC_INDEX_REL
    return _get_label(rid, rel_dict, index, es_client, wd_client, logger)
