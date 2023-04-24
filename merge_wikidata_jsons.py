import pathlib
import os
import json

from tqdm import tqdm
from flags import ROOT_PATH


def merge_json_files_in_folder(input_folder, output_file, limit=None):
    merged_data = []
    files_processed = 0

    for file in tqdm(os.listdir(input_folder)):
        if limit and files_processed >= limit:
            break

        if file.endswith(".json"):
            with open(os.path.join(input_folder, file), 'r') as f:
                json_data = json.load(f)
                merged_data.extend(json_data["data"])

            files_processed += 1

    with open(output_file, 'w') as output_f:
        json.dump({"data": merged_data}, output_f)


def main():
    input_root = ROOT_PATH.joinpath("data/d2t/wikidata/v2")
    output_root = ROOT_PATH.joinpath("data/d2t/wikidata/v2_merged")
    splits = ["dev", "test", "train"]
    limit = None

    for split in splits:
        for n in range(1, 8):
            input_folder = os.path.join(input_root, split, f"{n}triples")
            output_folder = os.path.join(output_root, split, f"{n}triples")

            # Create the output_folder if it doesn't exist
            if not os.path.exists(output_folder):
                os.makedirs(output_folder)

            output_file = os.path.join(output_folder, "merged.json")

            if os.path.exists(input_folder):
                merge_json_files_in_folder(input_folder, output_file, limit=limit)
                print(f"Merged JSON files from {input_folder} to {output_file}")


if __name__ == "__main__":
    main()
