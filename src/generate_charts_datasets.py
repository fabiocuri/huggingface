import ast
from collections import Counter

import numpy as np
import pandas as pd
import yaml
from google.cloud import bigquery, storage

from handlers import read_txt_from_bucket, upload_table_to_bigquery

if __name__ == "__main__":
    config = yaml.load(
        open("../config.yaml"),
        Loader=yaml.FullLoader,
    )

    bigquery_client = bigquery.Client()
    dataset_ref = bigquery_client.dataset("huggingface")

    client = storage.Client()
    bucket = client.bucket("huggingface-data")

    df_datasets = pd.read_parquet("../datasets.parquet")

    print("Generating chart for Percentage of open source datasets by language ...")

    df_plot = df_datasets.copy()
    df_plot = df_plot[["language"]]
    df_plot = df_plot.rename(columns={"language": "Language"})

    c = Counter(list(df_plot["Language"])).most_common()

    df_plot = pd.DataFrame(c)
    df_plot.columns = [
        "Language",
        "Percentage",
    ]

    df_first_10 = df_plot[:10]
    df_other = df_plot[10:]

    total = sum(list(df_other["Percentage"]))

    df_first_10.loc[len(df_first_10.index)] = [
        "Other",
        total,
    ]

    n = sum(list(df_first_10["Percentage"]))

    df_first_10["Percentage"] = [
        np.round(
            x * 100 / n,
            1,
        )
        for x in df_first_10["Percentage"]
    ]

    upload_table_to_bigquery(
        bigquery_client,
        dataset_ref,
        df_first_10,
        "Percentage of open source datasets by language",
    )

    print(
        "Generating chart for Open source datasets by language and application task ..."
    )

    df_plot = df_datasets.copy()
    df_plot = df_plot[df_plot["language"] != ""]
    df_plot = df_plot[df_plot["multilinguality"] != ""]
    df_plot = df_plot[df_plot["task_categories"] != ""]

    df_plot = df_plot.rename(
        columns={
            "language": "Language",
            "multilinguality": "Dataset Type",
            "task_categories": "Intended Use",
        }
    )
    df_plot = df_plot[
        df_plot["Dataset Type"].isin(
            [
                "multilingual",
                "monolingual",
                "translation",
            ]
        )
    ]

    dataset_type_mapping = {
        "multilingual": "Multilingual (contains two or more languages)",
        "monolingual": "Monolingual (contains only one language)",
        "translation": "Translation (enables translation between languages)",
    }
    df_plot["Dataset Type"] = df_plot["Dataset Type"].map(dataset_type_mapping)

    intendeduse2application_tasks = ast.literal_eval(
        read_txt_from_bucket(
            bucket,
            config["files"]["map_intended_use_to_application_task"],
        )
    )

    df_plot["Application Task"] = df_plot["Intended Use"].map(
        intendeduse2application_tasks
    )
    df_plot["Intended Use"] = (
        df_plot["Intended Use"]
        .str.replace(
            "-",
            " ",
        )
        .str.capitalize()
    )
    df_plot = df_plot[df_plot["Intended Use"] != "Other"]
    df_plot = df_plot[
        [
            "Language",
            "Dataset Type",
            "Intended Use",
            "Application Task",
        ]
    ]

    r = list()

    for language in list(set(df_plot["Language"])):
        for dst in list(set(df_plot["Dataset Type"])):
            for iu in list(set(df_plot["Intended Use"])):
                subset = df_plot[df_plot["Language"] == language]
                subset = subset[subset["Dataset Type"] == dst]
                subset = subset[subset["Intended Use"] == iu]

                if len(subset) == 0:
                    kws = []
                    diff = 0

                else:
                    kws = list(subset["Application Task"])
                    diff = int(len(set(kws)))

                r.append(
                    [
                        language,
                        dst,
                        iu,
                        kws,
                        diff,
                    ]
                )

    df_plot = pd.DataFrame(
        r,
        columns=[
            "Language",
            "Dataset Type",
            "Intended Use",
            "Application Task",
            "Number of concepts",
        ],
    )

    df_plot = df_plot.explode("Application Task")

    upload_table_to_bigquery(
        bigquery_client,
        dataset_ref,
        df_plot,
        "Open source datasets by language and application task",
    )
