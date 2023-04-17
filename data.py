#!/usr/bin/env python3

import json
import csv
import os
import logging
import random
import sys
from pathlib import Path
from collections import defaultdict, namedtuple
from functools import partial

from tqdm import tqdm

from elasticsearch import Elasticsearch
from flags import NOT_SEPARATOR_FLAG, RDF_SEPARATOR, ELASTIC_INDEX_ENT, ELASTIC_INDEX_REL
from utils import webnlg_parsing
from helpers import setup_logger, connect_to_elasticsearch, get_id_by_label, get_entity_label, get_relation_label


logger = setup_logger(__name__, logging.WARNING, output_log_file="data_processing.log")


class DataTriple:

    def __init__(self, sid: str, rid: str, oid: str, es_client: Elasticsearch = None):
        """ data class for rdf tripples which stores given sid, rid, oid as is given
                and if es_client is given, try to get labels for given ids
        """
        self.sid = sid
        self.rid = rid
        self.oid = oid

        if es_client is None:
            self.subj = sid
            self.pred = rid
            self.obj = oid
        else:
            self.subj = get_entity_label(sid, es_client)
            self.pred = get_relation_label(rid, es_client)
            self.obj = get_entity_label(oid, es_client)


class DataEntry:
    """
    A single D2T dataset example: a set of triples & its possible lexicalizations
    """
    def __init__(self, triples, lexs):
        self.triples = triples
        self.lexs = lexs

    def __repr__(self):
        return str(self.__dict__)


class D2TDataset:
    def __init__(self):
        self.data = {split: [] for split in ["train", "dev", "test"]}
        self.fallback_template = "The <predicate> of <subject> is <object> ."

    def load_from_dir(self, path, template_path, splits):
        """
        Load the dataset
        """
        raise NotImplementedError

    def load_templates(self, templates_filename):
        """
        Load existing templates from a JSON file
        """

        if not templates_filename:
            logger.warning(f"Templates will not be loaded")
            return
            
        logger.info(f"Loaded templates from {templates_filename}")
        with open(templates_filename) as f:
            self.templates = json.load(f)


class WebNLG(D2TDataset):
    dataset_name="webnlg"

    def __init__(self):
        super().__init__()

    def get_template(self, triple):
        """
        Return the template for the triple
        """
        pred = triple.pred

        if pred in self.templates:
            # Using just a single template
            assert len(self.templates[pred]) == 1
            template = self.templates[pred][0]
        else:
            logger.warning(f"No template for {pred}, using a fallback")
            template = self.fallback_template

        return template

    def load_from_dir(self, path, template_path, splits):
        """
        Load the dataset
        """
        self.load_templates(template_path)

        for split in splits:
            logger.info(f"Loading {split} split")
            data_dir = os.path.join(path, split)
            err = 0
            xml_entryset = webnlg_parsing.run_parser(data_dir)

            for xml_entry in xml_entryset:
                triples = [DataTriple(e.subject, e.predicate, e.object)
                                for e in xml_entry.modifiedtripleset]
                lexs = self._extract_lexs(xml_entry.lexEntries, triples)

                if not any([lex for lex in lexs]):
                    err += 1
                    continue

                entry = DataEntry(triples=triples, lexs=lexs)
                self.data[split].append(entry)

            if err > 0:
                logger.warning(f"Skipping {err} entries without lexicalizations...")

    def _extract_lexs(self, lex_entries, triples):
        """
        Use `orderedtripleset` in the WebNLG dataset to determine the "ground-truth" order
        of the triples (based on human references).
        """
        lexs = []

        for entry in lex_entries:
            order, agg = self._extract_ord_agg(triples, entry.orderedtripleset)
            lex = {
                "text": entry.text,
                "order": order,
                "agg": agg
            }
            lexs.append(lex)

        return lexs

    def _extract_ord_agg(self, triples, ordered_triples):
        """
        Determine the permutation indices and aggregation markers from
        the ground truth.
        """
        # if ordered triples do not match the actual triples -> fail
        ordered_triples_flattened = [x for sent in ordered_triples for x in sent]
        if len(ordered_triples_flattened) != len(triples):
            return None, None

        order = []

        for t in triples:
            for i, o in enumerate(ordered_triples_flattened):
                if t.subj == o.subject and \
                   t.pred == o.predicate and \
                   t.obj == o.object:
                   order.append(i)
                   break
            else:
                # ordered triples do not match the actual triples
                return None, None

        agg = []

        for i, triples_in_sent in enumerate(ordered_triples):
            if triples_in_sent:
                agg += [i] * len(triples_in_sent)

        return order, agg


class E2E(D2TDataset):
    dataset_name="e2e"

    def __init__(self):
        super().__init__()

    def load_from_dir(self, path, template_path, splits):
        """
        Load the dataset
        """
        self.load_templates(template_path)

        for split in splits:
            logger.info(f"Loading {split} split")
            triples_to_lex = defaultdict(list)

            with open(os.path.join(path, f"{split}.csv")) as csv_file:
                csv_reader = csv.reader(csv_file, delimiter=',', quotechar='"')

                # skip header
                next(csv_reader)
                err = 0

                for i, line in enumerate(csv_reader):
                    triples = self._mr_to_triples(line[0])

                    # probably a corrupted sample
                    if not triples or len(triples) == 1:
                        err += 1
                        # cannot skip for dev and test
                        if split == "train":
                            continue

                    lex = {"text" : line[1]}
                    triples_to_lex[triples].append(lex)

                # triples are not sorted, complete entries can be created only after the dataset is processed
                for triples, lex_list in triples_to_lex.items():
                    entry = DataEntry(triples, lex_list)
                    self.data[split].append(entry)

            logger.warn(f"{err} corrupted instances")


    def _mr_to_triples(self, mr):
        """
        Transforms E2E meaning representation into RDF triples.
        """
        triples = []

        # cannot be dictionary, slot keys can be duplicated
        items = [x.strip() for x in mr.split(",")]
        subj = None

        keys = []
        vals = []

        for item in items:
            key, val = item.split("[")
            val = val[:-1]

            keys.append(key)
            vals.append(val)

        name_idx = None if "name" not in keys else keys.index("name")
        eatType_idx = None if "eatType" not in keys else keys.index("eatType")

        # primary option: use `name` as a subject
        if name_idx is not None:
            subj = vals[name_idx]
            del keys[name_idx]
            del vals[name_idx]

            # corrupted case hotfix
            if not keys:
                keys.append("eatType")
                vals.append("restaurant")

        # in some cases, that does not work -> use `eatType` as a subject
        elif eatType_idx is not None:
            subj = vals[eatType_idx]
            del keys[eatType_idx]
            del vals[eatType_idx]
        # still in some cases, there is not even an eatType 
        #-> hotfix so that we do not lose data
        else:
            # logger.warning(f"Cannot recognize subject in mr: {mr}")
            subj = "restaurant"

        for key, val in zip(keys, vals):
            triples.append(DataTriple(subj, key, val))

        # will be used as a key in a dictionary
        return tuple(triples)


    def get_template(self, triple):
        """
        Return the template for the triple
        """
        if triple.pred in self.templates:
            templates = self.templates[triple.pred]
            # special templates for familyFriendly yes / no
            if type(templates) is dict and triple.obj in templates:
                template = templates[triple.obj][0]
            else:
                template = templates[0]
        else:
            template = self.fallback_template

        return template


class WikiData(D2TDataset):
    dataset_name="wikidata"

    def __init__(self):
        super().__init__()
        self.es_client = connect_to_elasticsearch()
        self.get_ent_id = partial(get_id_by_label, es_client=self.es_client, index=ELASTIC_INDEX_ENT)
        self.get_rel_id = partial(get_id_by_label, es_client=self.es_client, index=ELASTIC_INDEX_REL)
        self.get_ent_label = partial(get_entity_label, es_client=self.es_client)
        self.get_rel_label = partial(get_relation_label, es_client=self.es_client)

    def get_template(self, triple):
        """
        Return the template for the triple
        """
        pred = triple.pred

        if pred in self.templates:
            # Note: Sampling one of available templates from list
            template = random.sample(self.templates[pred], 1)[0]
        else:
            logger.warning(f"No template for {pred}, using a fallback")
            template = self.fallback_template

        return template

    # TODO: this is stage III, implement this after stage I and II are finished
    def load_from_dir(self, path, template_path, splits):
        """
              will fill self.data[split] with 'entry' objects
                'entry' == DataEntry(triples, lexs)
                    'triples' == list of 'DataTriple' objects
                        'DataTriple' == namedtuple('DataTriple', ['subj', 'pred', 'obj'])
                    'lexs' == lexicon of correct answers (references) ... we don't need this!, just set to ''
        """
        self.load_templates(template_path)

        for split in splits:
            logger.info(f"Loading {split} split")
            data_dir = os.path.join(path, split)
            err = 0

            entryset = self._load_jsons_from_dir(data_dir)

            for entry_list in entryset:
                triples = [DataTriple(e[0], e[1], e[2], self.es_client) for e in entry_list]  # Refactor: populate triples with triples from entryset wrapped by DataTriple
                lexs = self._extract_lexs(entry_list, triples)

                if not any([lex for lex in lexs]):
                    err += 1
                    continue

                entry = DataEntry(triples=triples, lexs=lexs)  # populate with lists of DataTriple
                self.data[split].append(entry)  # populate data for the current split

            if err > 0:
                logger.warning(f"Skipping {err} entries without lexicalizations...")

    def _load_jsons_from_dir(self, data_dir: str or Path) -> list[list[tuple]]:
        """ Loads all json files from data_dir and parses their content into one list of lists of string tuples
        containing their sid, rid, and oid.
            The input json files have structure:
            {"data": [["(sid [RDF_SEPARATOR] rid [RDF_SEPARATOR] oid)", "(sid [RDF_SEPARATOR] rid [RDF_SEPARATOR] oid)", ...]]}
            The output list has structure:
            [[("sid", "rid", "oid"), ("sid", "rid", "oid"), ...], [...], ...]
        """
        data_d = Path(data_dir)
        files = data_d.glob("**/*.json")

        final_list = []
        for file in tqdm(files):
            # print(file)
            try:
                data = json.load(file.open("r"))["data"]
            except json.JSONDecodeError as err:
                print(f"{data} ({err})")  # TODO: solve this

            # Create a temporary list to store tuples of sid, rid, and oid for each file
            temp_list = []

            for item in data[0]:
                # Split the string using RDF_SEPARATOR as split str and strip any leading/trailing whitespace
                try:
                    sid, rid, oid = map(str.strip, item.split(f" {RDF_SEPARATOR} "))
                except ValueError as err:
                    # there are more than 3 RDF_SEPARATOR string sequences in item
                    # NOTE: legacy in case RDF_SEPARATOR == "|"
                    logger.warning(f"Split error at: f: {file}, item: {item} ({err}) (checking for NOT_SEPARATOR_FLAG)")
                    not_sep_item = item.replace(f"{NOT_SEPARATOR_FLAG}_{RDF_SEPARATOR}", "<<&placeholder>>")
                    try:
                        sid, rid, oid = [s.replace("<<&placeholder>>", f"{NOT_SEPARATOR_FLAG}_{RDF_SEPARATOR}") for s in
                                            map(str.strip, not_sep_item.split(f" {RDF_SEPARATOR} "))]
                    except ValueError as err:
                        logger.warning(
                            f"Split error at: f: {file}, item: {item} ({err}) NOT RESOLVED! Falling back to legacy")
                        oid = item.rsplit(f" {RDF_SEPARATOR} ")[0]
                        rid = item.rsplit(f" {RDF_SEPARATOR} ")[1]
                        sid = '+'.join(item.rsplit(f" {RDF_SEPARATOR} ")[1:])
                # Create a tuple and add it to the temporary list
                temp_list.append((sid, rid, oid))

            # Add the temporary list to the final list
            final_list.append(temp_list)

        return final_list

    def _extract_lexs(self, lex_entries, triples):
        """
        Use `orderedtripleset` in the WebNLG dataset to determine the "ground-truth" order
        of the triples (based on human references).
        """
        lexs = []
        # print(lex_entries)
        for entry in lex_entries:
            order, agg = self._extract_ord_agg(triples)
            s, r, o = entry
            lex = {
                "text": f'{s} {RDF_SEPARATOR} {r} {RDF_SEPARATOR} {o}',
                "active_set": f"i({s},{r},{o})",
                "order": order,
                "agg": agg
            }
            lexs.append(lex)

        return lexs

    def _extract_ord_agg(self, triples):
        """
        Determine the permutation indices and aggregation markers from
        the ground truth.
        """
        order = list(range(len(triples)))
        agg = list(range(len(triples)))
        return order, agg    # NOTE: no need here


def get_dataset_class(dataset_class):
    """
    A wrapper for easier introduction of new datasets.
    Returns class "MyDataset" for a parameter "--dataset mydataset"
    """
    try:
        # case-insensitive
        # print(f"\n\n{globals().values()}\n")
        available_classes = get_available_classes()
        return available_classes[dataset_class.lower()]
    except AttributeError:
        logger.error(f"Unknown dataset: '{dataset_class}'. Please create \
            a class with an attribute dataset_name='{dataset_class}' in 'data.py'.")
        return None

def get_available_classes():
    result = {}
    current_module = sys.modules[__name__]
    for name in dir(current_module):
        obj = getattr(current_module, name)
        if isinstance(obj, type) and issubclass(obj, D2TDataset) and obj != D2TDataset and hasattr(obj, "dataset_name"):
            result[obj.dataset_name.lower()] = obj
    return result


# test out the WikiData class:
if __name__ == "__main__":
    wk = WikiData()

    splits = ["dev"]
    path_to_data = "data/d2t/wikidata/data"
    path_to_templates = "templates/templates-wikidata.json"

    wk.load_from_dir(path_to_data, path_to_templates, splits)

    print(wk.templates)
    print(wk.data['dev'])
