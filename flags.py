import os
from pathlib import Path

NOT_SEPARATOR_FLAG = "<&NOT>"
RDF_SEPARATOR = "|"  # '<&SEP>'  # NOTE: legacy '|' has problems as it is present in some entity labels

ROOT_PATH = Path(os.path.dirname(__file__))
ELASTIC_USER = 'elastic'
ELASTIC_PASSWORD = '1jceIiR5k6JlmSyDpNwK'  # Freya
ELASTIC_HOST = 'https://localhost:9200'
ELASTIC_CERTS = ROOT_PATH.joinpath('es_certs').joinpath('http_ca.crt')

ELASTIC_INDEX_ENT = "csqa_wikidata_ent_full"
ELASTIC_INDEX_REL = "csqa_wikidata_rel"
