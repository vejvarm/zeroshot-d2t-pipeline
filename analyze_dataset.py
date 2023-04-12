import json
from pathlib import Path

DATA_DIR = Path("data")
dataset_folder = DATA_DIR.joinpath("wikidata_1stage-v2")


if __name__ == "__main__":
    splits = ["dev", "test", "train"]
    for split in splits:
        data_dict = json.load(dataset_folder.joinpath(f"{split}.json").open("r"))
        print(f"SPLIT: {split}:")
        print(f"\t{len(data_dict['data'])}")
