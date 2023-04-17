#!/usr/bin/env python3

import os
import argparse
import logging
import data
import json
import random
import re
import numpy as np
from utils.tokenizer import Tokenizer
from data import DataTriple
from collections import defaultdict
from flags import RDF_SEPARATOR

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO, datefmt='%H:%M:%S')
logger = logging.getLogger(__name__)

class Preprocessor:
    def __init__(self, dataset, out_dirname):
        self.dataset = dataset
        self.out_dirname = out_dirname
        self.tokenizer = Tokenizer()

    def fill_template(self, template, triple):
        """
        Fills a template with the data from the triple
        """
        for item, placeholder in [
            (triple.subj, "<subject>"), 
            (triple.pred, "<predicate>"), 
            (triple.obj, "<object>")
        ]:
            template = template.replace(placeholder, 
                self.tokenizer.normalize(item, 
                    remove_quotes=True, 
                    remove_parentheses=True)
                )
        return template


    def create_examples(self, entry, dataset, shuffle, keep_separate_sents):
        """
        Generates training examples from an entry in the dataset
        """
        examples = []
        lexs = entry.lexs
        triples = entry.triples
        sentences = []

        for t in entry.triples:
            template = dataset.get_template(t)
            sentence = self.fill_template(template, t)
            sentence = self.tokenizer.detokenize(sentence)
            sentences.append(sentence)

        if shuffle:
            random.shuffle(sentences)

        if keep_separate_sents:
            inp = sentences
        else:
            inp = " ".join(sentences)

        for lex in entry.lexs:
            example = {
                "sents": inp,
                "text": lex["text"],
            }
            if "active_set" in lex.keys():
                example["active_set"] = [lex["active_set"]]
            examples.append(example)

        return examples

    def extract_refs(self, out_dirname, split):
        with open(f"{out_dirname}/{split}.ref", "w") as f:
            data = self.dataset.data[split]

            for entry in data:
                for lex in entry.lexs:
                    f.write(lex["text"] + "\n")
                f.write("\n")


    # def extract_mrs(self, out_dirname, split):
    #     with open(f"{out_dirname}/{split}.ref", "w") as f:
    #         data = self.dataset.data[split]

    #         for entry in data:
    #             # same mr for all lexs
    #             mr = entry.lexs[0]["orig_mr"]
    #             f.write(mr + "\n")

    def extract_agg(self, entry, dataset):
        examples = []
        triples = entry.triples

        if len(triples) == 1:
            return examples

        # there is one way to aggregate sentences for every possible order
        for lex in entry.lexs:
            if not lex["order"] or not lex["agg"]:
                continue
            
            # get the "reordering indices" for the given order
            order = np.argsort(lex["order"])
            # reorder the triples
            triples_reordered = np.array(triples)[order].tolist()
            triples_reordered = [DataTriple(*x) for x in triples_reordered]
            sentences = []
            prev_sent = 0
            agg = []

            for i, t in enumerate(triples_reordered):
                template = dataset.get_template(t)
                sentence = self.fill_template(template, t)
                sentence = self.tokenizer.detokenize(sentence)
                sentences.append(sentence)

                if i < len(triples_reordered) - 1:
                    if lex["agg"][i+1] != prev_sent:
                        agg.append(1)
                        prev_sent = lex["agg"][i+1]
                    else:
                        agg.append(0)

            example = {
                "sents" : sentences,
                "sep" : agg
            }
            examples.append(example)
            
        return examples


    def extract_order(self, split, extract_copy_baseline):
        with open(os.path.join(self.out_dirname, f"{split}.order.out"), "w") as f:
            for i, entry in enumerate(data):
                if len(entry.triples) == 1:
                    # skip trivial examples
                    continue

                if extract_copy_baseline:
                    order = list(range(len(entry.triples)))
                    f.write(" ".join([str(x) for x in order]) + "\n")
                    continue

                entry_ok = False

                for lex in entry.lexs:
                    order = lex["order"]

                    if order:
                        assert len(order) == len(entry.triples)
                        entry_ok = True
                        f.write(" ".join([str(x) for x in order]) + "\n")

                if not entry_ok:
                    # no valid order for the entry
                    # -> generate a default order
                    order = list(range(len(entry.triples)))
                    f.write(" ".join([str(x) for x in order]) + "\n")

                f.write("\n")
        return

    def process(self, split, shuffle, extract_copy_baseline, extract_order, extract_agg, keep_separate_sents, keep_one_example_only, concat_meta_fields=False):
        """
        Processes and outputs training data for the sentence fusion model
        """ 
        output = {"data" : []}
        data = self.dataset.data[split]

        if extract_order:
            self.extract_order(self, split, extract_copy_baseline)
            return
        
        for i, entry in enumerate(data):
            if extract_agg:
                examples = self.extract_agg(entry, dataset)
            else:
                examples = self.create_examples(entry, dataset, shuffle, keep_separate_sents)

            if examples:
                if concat_meta_fields:
                    examples[0]["text"] = RDF_SEPARATOR.join(e["text"] for e in examples)
                    if "active_set" in examples[0].keys():
                        examples[0]["active_set"] = [e["active_set"] for e in examples]

                if (split == "train" or extract_agg) and not keep_one_example_only:
                    # keep all examples
                    examples = examples
                else:
                    # keep just one example per tripleset
                    examples = examples[0:1]

            for example in examples:
                output["data"].append(example)

        with open(os.path.join(self.out_dirname, f"{split}.json"), "w") as f:
            json.dump(output, f, indent=4, ensure_ascii=False)

        if extract_copy_baseline and split != "train":
            with open(os.path.join(self.out_dirname, f"{split}_triples.out"), "w") as f:
                for example in output["data"]:
                    f.write(example["text"] + "\n")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", type=str, required=True,
        help="Name of the dataset to preprocess.")
    parser.add_argument("--dataset_dir", type=str, default=None,
        help="Path to the dataset")
    parser.add_argument("--templates", type=str, default=None,
        help="Path to the JSON file with templates")
    parser.add_argument("--output", type=str, required=True,
        help="Name of the output directory")
    parser.add_argument("--output_refs", type=str, default=None,
        help="Name of the output directory for references.")
    parser.add_argument('--splits', type=str, nargs='+', default=["train", "dev", "test"],
                    help='Dataset splits (e.g. train dev test)')
    parser.add_argument("--seed", type=int, default=42,
        help="Random seed.")
    parser.add_argument("--shuffle", action="store_true",
        help="Shuffle input sentences.")
    parser.add_argument("--keep_separate_sents", action="store_true",
        help="Keep a list of individual sentences as the input.")
    parser.add_argument("--extract_copy_baseline", action="store_true",
        help="Extract inputs to a separate file for the copy baseline.")
    parser.add_argument("--extract_order", action="store_true",
        help="Extract ordering information (evaluation, WebNLG only).")
    parser.add_argument("--extract_agg", action="store_true",
        help="Extract aggregation information (evaluation, WebNLG only).")
    parser.add_argument("--keep_one_example_only", action="store_true",
        help="No matter the split, keep only one example")
    parser.add_argument("--concat_meta_fields", action="store_true",
        help="Concatenate text from all examples into example[0]. Good to combine with --keep_one_example_only")
    args = parser.parse_args()
    random.seed(args.seed)

    logger.info(args)

    dataset_name = args.dataset

    # Load data
    logger.info(f"Loading dataset {dataset_name}")
    dataset = data.get_dataset_class(dataset_name)()
    path = args.dataset_dir

    try:
        dataset.load_from_dir(path=path, template_path=args.templates, splits=args.splits)
    except FileNotFoundError as err:
        logger.error(f"Dataset not found in {path}")
        raise err
        
    # Create output directory
    try:
        out_dirname = os.path.join(args.output)
        os.makedirs(out_dirname, exist_ok=True)
    except OSError as err:
        logger.error(f"Output directory {out_dirname} can not be created")
        raise err

    os.makedirs(out_dirname, exist_ok=True)

    preprocessor = Preprocessor(dataset=dataset, 
        out_dirname=out_dirname,
    )

    for split in args.splits:
        preprocessor.process(split, 
                             shuffle=args.shuffle,
                             extract_copy_baseline=args.extract_copy_baseline,
                             extract_order=args.extract_order,
                             extract_agg=args.extract_agg,
                             keep_separate_sents=args.keep_separate_sents,
                             keep_one_example_only=args.keep_one_example_only,
                             concat_meta_fields=args.concat_meta_fields
        )

    if args.output_refs:
        os.makedirs(args.output_refs, exist_ok=True)

        for split in args.splits:
            logger.info(f"Extracting {split} references")
            preprocessor.extract_refs(args.output_refs, split)

    logger.info(f"Preprocessing finished.")