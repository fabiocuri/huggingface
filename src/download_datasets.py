import iso639
import pandas as pd
from huggingface_hub import HfApi
from tqdm.contrib import tzip

from handlers import try_except_func

if __name__ == "__main__":
    api = HfApi()
    datasets = list(api.list_datasets())

    df = pd.DataFrame(
        [
            {
                "dataset_name": dataset.id,
                **dataset.__dict__,
            }
            for dataset in datasets
        ]
    )

    df_datasets = pd.concat(
        [
            pd.DataFrame(
                {
                    parts[0]: [parts[1]]
                    for parts in [element.split(":") for element in tags]
                    if len(parts) > 1
                },
                index=[dataset_name],
            ).assign(dataset_name=dataset_name)
            for dataset_name, tags in tzip(
                df["dataset_name"],
                df["tags"],
            )
        ]
    )

    df_datasets.fillna(
        "",
        inplace=True,
    )
    df_datasets = df_datasets[df_datasets["language"] != ""]
    df_datasets = df_datasets[
        ~df_datasets["language"].isin(
            [
                "code",
                "multilingual",
            ]
        )
    ]

    df_datasets["language"] = [
        try_except_func(
            iso639.Language.match,
            language,
        )
        for language in df_datasets["language"]
    ]

    df_datasets["language"] = [
        language.name if language != "" else "" for language in df_datasets["language"]
    ]

    df_datasets = df_datasets[df_datasets["language"] != ""]

    num_languages = len(set(df_datasets["language"]))

    print(f"There are {num_languages} languages in Hugging Face datasets.")

    df_datasets.index = list(range(len(df_datasets)))

    df_datasets.to_parquet(
        "../datasets.parquet",
        index=None,
    )
