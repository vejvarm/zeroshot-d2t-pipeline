import pathlib
import os
import json

from tqdm import tqdm
from flags import ROOT_PATH


def main():
    input_root = pathlib.Path("/media/freya/kubuntu-data/datasets/d2t/v2/wikidata_1stage_v2/chunks")
    output_root = input_root.parent
    splits = ["train", ]

    output_root.mkdir(parents=True, exist_ok=True)

    for split in tqdm(splits):
        json_files_for_split = list(input_root.glob(f"{split}_chunk*.json"))
        print(f"num chunks for {split}: {len(json_files_for_split)}")
        merged_data = {"data": []}
        for chunk_path in tqdm(json_files_for_split):
            chunk_data = json.load(chunk_path.open())["data"]
            merged_data["data"].extend(chunk_data)

        json.dump(merged_data, output_root.joinpath(f"{split}.json").open("w"), indent=4)


if __name__ == "__main__":
    main()
