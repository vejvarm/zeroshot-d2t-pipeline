import json
import logging
from typing import Dict, List
from ordered_set import OrderedSet
from elasticsearch import Elasticsearch


LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.WARNING)

def connect_to_elasticsearch(user='elastic', password='1jceIiR5k6JlmSyDpNwK'):
    """Connect to Elasticsearch client using urls and credentials from args.py"""
    return Elasticsearch(
        'https://localhost:9200',
        ca_certs=f'{ROOT_PATH}/{args.elastic_certs.removeprefix("./")}',
        basic_auth=(user, password),  # refer to args.py --elastic_password for alternatives
        retry_on_timeout=True,
    )

CLIENT = connect_to_elasticsearch()


def lookup_labels(obj_set: OrderedSet[str], es_index="csqa_wikidata_rel"):
    """

    # special cases:
    #   entity doesn't exist: {..., (None, None), ...}
    #   types list for entity is empty: {..., ('label', []), ...}

    :param obj_set: (OrderedSet) objects to get from entity index
    :param es_index: (str) index on which to run this operation
    :return:
    """
    res_dict = dict()
    if not obj_set or '' in obj_set:
        LOGGER.info(f"in self._get_by_ids: obj_set is {obj_set}, returning empty dictionary")
        return res_dict

    res = self.client.mget(index=es_index,
                           ids=list(obj_set))

    LOGGER.debug(f"res in self._get_by_ids: {res}")

    for hit in res['docs']:
        _id = hit['_id']
        if hit['found']:
            label = hit['_source']['label']
            types = hit['_source']['types']
            res_dict[_id] = (label, types)
        else:
            LOGGER.info(f'in _get_by_ids: Entity with id "{_id}" was NOT found in label&type documents.')
            res_dict[_id] = (None, None)

    LOGGER.debug(f"res_dict in self._get_by_ids: {res_dict}")

    return res_dict


def lookup_labels(triple_ids: List[str]) -> List[str]:
    return aop._get_by_ids(triple_ids, es_index="csqa_wikidata_rel")

def create_json_files(templates: Dict, triples: Dict, aop) -> None:


    def transform_triples_to_json(triples_dict: Dict, templates_dict: Dict) -> List[Dict]:
        transformed_data = []
        for triple_set in triples_dict:
            for data_triple in triple_set['triples']:
                relation_id = data_triple.pred
                label = lookup_labels([relation_id])[0]
                template = templates_dict[label][0]
                text = template.replace('<subject>', data_triple.subj).replace('<object>', data_triple.obj)
                transformed_data.append({
                    'sents': [text],
                    'text': text
                })
        return transformed_data

    for subset in ['train', 'dev', 'test']:
        output_data = transform_triples_to_json(triples[subset], templates)
        output = {'data': output_data}
        with open(f'{subset}.json', 'w') as outfile:
            json.dump(output, outfile, indent=2)

# Call the function with the required arguments
create_json_files(templates, triples, aop)
