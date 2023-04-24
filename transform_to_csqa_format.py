import json
import logging
import pathlib
import re

from urllib.error import HTTPError
from wikidata.client import Client as WDClient
from wikidata.entity import EntityId, Entity
from ordered_set import OrderedSet
from helpers import get_entity_label, get_relation_label, setup_logger
from flags import WIKIDATA_ENT_DICT_PATH, WIKIDATA_REL_DICT_PATH
from tqdm import tqdm

IN_ROOT = pathlib.Path("/media/freya/kubuntu-data/datasets/d2t/v2/wikidata_1stage_v2")
OUT_ROOT = IN_ROOT

log_file = OUT_ROOT.joinpath("transform_to_csqa_format.log")
LOGGER = setup_logger(__name__, logging.WARNING, output_log_file=log_file)

# Example JSON data (replace with your actual data)
json_data = [
    {
        "active_set": [
            "i(Q6726776,P136,Q182415)",
            "i(Q6726776,P495,Q884)"
        ]
    },
    {
        "active_set": [
            "i(Q4703598,P735,Q18916867)",
            "i(Q4703598,P69,Q4614)",
            "i(Q4703598,P27,Q30)",
            "i(Q4703598,P21,Q6581097)",
            "i(Q4703598,P106,Q42973)"
        ]
    }
]


def fetch_direct_parent(entity_id, client: WDClient):
    try:
        entity = client.get(entity_id, load=True)
    except HTTPError as err:
        LOGGER.warning(f"(Returning x{entity_id})... Error fetching entity {entity_id} from WikiData: {err}.")
        return f"x{entity_id}"
    instance_of = Entity(EntityId('P31'), client)
    subclass_of = Entity(EntityId('P279'), client)
    try:
        parent = entity.getlist(instance_of)[0]  # get first entity which is 'instance of' object
    except IndexError:
        parent = entity.getlist(subclass_of)[0]  # (fallback) get first entity which is 'subclass of' object
    return parent.id


def update_table_format(active_set, ent_dict: dict[str: str], rel_dict: dict[str: str]):
    table_format = []
    sid_set = set()

    for entry in active_set:
        match = re.match(r'i\((Q\d+),(P\d+),(Q\d+)\)', entry)
        if match:
            sid, pid, oid = match.groups()

            s_label = get_entity_label(sid, ent_dict)
            p_label = get_relation_label(pid, rel_dict)
            o_label = get_entity_label(oid, ent_dict)

            # if new unique sid is present in the current active set, add its name to the table format
            if sid not in sid_set:
                table_format.extend([["name", s_label.split(" ")]])
                sid_set.add(sid)

            table_format.extend([[p_label, o_label.split(" ")]])

    return table_format


def transform_to_csqa_format(entries: list[(dict, str), ...], client: WDClient, ent_dict: dict[str: str],
                             rel_dict: dict[str: str], out_file_path: pathlib.Path, total_entries: int=None):
    """
    Transforms the input data into CSQA format and writes the result to a file.

    :param entries: A list of tuples, where each tuple contains a dictionary of JSON data and a string
                    representing a D2T sentence.
    :param client: A Wikidata Client object to fetch direct parent entities.
    :param ent_dict: A dictionary mapping entity IDs to their labels.
    :param rel_dict: A dictionary mapping relation IDs to their labels.
    :param out_file_path: The output file path where the transformed data will be written.
    :param total_entries: total number of entries in the entries iterator/generator
    :return: None
    """

    with out_file_path.open("a") as out_file:
        turn_position = 0
        for entry in tqdm(entries, total=total_entries):
            json_data = entry[0]  # dict
            d2t_sentence = entry[1]
            object_entities = OrderedSet()
            relations = OrderedSet()
            for triple in json_data['active_set']:
                triple_parts = triple[2:-1].split(',')
                object_entities.add(triple_parts[0])
                object_entities.add(triple_parts[2])
                relations.add(triple_parts[1])

            type_list = []
            type_spurious = False
            for ent in object_entities:
                parent_id = fetch_direct_parent(ent, client)
                if parent_id.startswith("x"):
                    type_spurious = True
                type_list.append(parent_id)
            table_format = update_table_format(json_data['active_set'], ent_dict, rel_dict)

            user_entry = {
                "ques_type_id": 2,
                "question-type": "Simple Insert (Direct)",
                "description": "Simple Insert|Mult. Entity",
                "entities_in_utterance": list(object_entities),
                "relations": list(relations),
                "type_list": type_list,
                "type_spurious": type_spurious,
                "speaker": "USER",
                "utterance": d2t_sentence.strip(),
                "turn_position": turn_position
            }

            system_entry = {
                "all_entities": list(object_entities),
                "speaker": "SYSTEM",
                "entities_in_utterance": list(object_entities),
                "utterance": json_data["text"],
                "active_set": json_data['active_set'],
                "table_format": table_format
            }

            turn_position += 1

            out_file.write(json.dumps(user_entry) + "\n")
            out_file.write(json.dumps(system_entry) + "\n")


if __name__ == "__main__":
    client = WDClient()
    local_ent_dict = json.load(WIKIDATA_ENT_DICT_PATH.open())
    local_rel_dict = json.load(WIKIDATA_REL_DICT_PATH.open())

    data_source_folder = IN_ROOT
    output_folder = OUT_ROOT.joinpath("csqa")
    output_folder.mkdir(parents=True, exist_ok=True)

    splits = ["dev", ]  # "test", "train"]
    for split in splits:
        json_data = json.load(data_source_folder.joinpath(f"{split}.json").open())["data"]
        d2t_data = data_source_folder.joinpath(f"{split}.out").open().readlines()
        output_file_path = output_folder.joinpath(f"{split}.jsonl")  # jsonlines file

        entries = zip(json_data, d2t_data)  # each entry has entry has tuple(dict(), str) format
        total_entries = len(json_data)
        print(f"Input entries total: {total_entries}")
        transform_to_csqa_format(entries, client, local_ent_dict, local_rel_dict, output_file_path, total_entries)
