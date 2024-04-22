import pandas as pd
from huggingface_hub import HfApi

if __name__ == "__main__":
    api = HfApi()
    models = list(api.list_models())

    data = [
        {
            key: getattr(
                model,
                key,
                "",
            )
            for key in model.__dict__
        }
        for model in models
    ]

    df_models = pd.DataFrame(data)
    df_models.dropna(
        subset=["id"],
        inplace=True,
    )
    df_models.rename(
        columns={"id": "model_name"},
        inplace=True,
    )
    df_models.fillna(
        "",
        inplace=True,
    )

    # Only consider models from April 2022, because all models created before
    # this timestamp were not tracked.

    df_models["created_at"] = pd.to_datetime(df_models["created_at"])
    df_models["date_only"] = df_models["created_at"].dt.date
    df_models = df_models[df_models["date_only"] >= pd.to_datetime("2022-04-01").date()]
    df_models["month"] = df_models["created_at"].astype(str)
    df_models["month"] = df_models["month"].str[:7]

    df_models.index = list(range(len(df_models)))

    df_models.to_parquet(
        "../models.parquet",
        index=None,
    )
