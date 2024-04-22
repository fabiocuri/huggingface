import os
import random
import subprocess
import uuid

import pandas as pd
import yaml
from tqdm import tqdm
from transformers import AutoModel

if __name__ == "__main__":
    config = yaml.load(
        open("../config.yaml"),
        Loader=yaml.FullLoader,
    )

    bucket = config["files"]["path"]
    model_num_params_info = config["files"]["model-num-params-info"]

    # Load params file
    params_df = pd.read_csv(f"gs://{bucket}/{model_num_params_info}")

    # Load local cache files
    os.makedirs(
        "../tmp",
        exist_ok="True",
    )
    csv_files = [file for file in os.listdir("../tmp") if file.endswith(".csv")]

    if csv_files:
        concatenated_df = pd.concat(
            [pd.read_csv(f"../tmp/{file}") for file in csv_files]
        )
        concatenated_df = pd.concat(
            [
                params_df,
                concatenated_df,
            ]
        )
        concatenated_df.to_csv(
            f"gs://{bucket}/{model_num_params_info}",
            index=False,
        )

        params_df = concatenated_df.copy()

        for file in csv_files:
            file_path = os.path.join(
                "../tmp/",
                file,
            )
            os.remove(file_path)
            print(f"Deleted: {file}")

    params_df.fillna(
        "",
        inplace=True,
    )
    params_df = params_df[params_df["num_params"] != ""]
    params_df["num_params"] = params_df["num_params"].astype(int)
    params_df.index = list(range(len(params_df)))

    # Load num params for missing models...

    df_models = pd.read_parquet("../models.parquet")

    # Only get parameters for models with a number of downloads higher than "min-downloads".
    df_models = df_models[df_models["downloads"] >= int(config["min-downloads"])]

    model_indices = {m: True for m in params_df["model_name"]}
    new_models = [m for m in df_models["model_name"] if m not in model_indices]
    random.shuffle(new_models)

    for model in tqdm(new_models):
        try:
            model_txt = AutoModel.from_pretrained(
                model,
                trust_remote_code=True,
            )
            num_params = int(model_txt.num_parameters())
            subprocess.run(["clear"])
            print(num_params)

        except:
            num_params = None

        new_params = pd.DataFrame(
            {
                "num_params": [num_params],
                "model_name": [model],
            }
        )

        subprocess.run(
            [
                "sudo",
                "rm",
                "-rf",
                "/home/fabio/.cache",
            ]
        )

        new_params.fillna(
            "",
            inplace=True,
        )
        new_params.index = list(range(len(new_params)))

        random_identifier_str = str(uuid.uuid4())

        new_params.to_csv(
            f"../tmp/{random_identifier_str}.csv",
            index=None,
        )
