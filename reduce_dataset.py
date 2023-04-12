import json
from pathlib import Path

DATA_DIR = Path("data")
dataset_folder = DATA_DIR.joinpath("wikidata_1stage-v2")

if __name__ == "__main__":
    max_num_entries = 1000
    splits = ["test", ]  # ["dev", "test", "train"]
    for split in splits:
        data_dict = json.load(dataset_folder.joinpath(f"{split}.json").open("r"))
        total_num_entries = len(data_dict['data'])
        print(f"SPLIT: {split}:")
        print(f"\ttotal:{total_num_entries}")

        reduced_data_dict = {'data': []}
        num_entries = min(total_num_entries, max_num_entries)
        reduced_data_dict['data'] = data_dict['data'][:num_entries]

        with dataset_folder.joinpath(f"{split}-reduced.json").open("w") as f:
            json.dump(reduced_data_dict, f, indent=4)

        print(f"\treduced: {len(reduced_data_dict['data'])}")
