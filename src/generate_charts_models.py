import pandas as pd
import yaml
from google.cloud import bigquery, storage

from handlers import upload_table_to_bigquery

if __name__ == "__main__":
    config = yaml.load(
        open("../config.yaml"),
        Loader=yaml.FullLoader,
    )

    bucket_name = config["files"]["path"]
    dataset_name = config["files"]["dataset"]

    bigquery_client = bigquery.Client()
    dataset_ref = bigquery_client.dataset(dataset_name)

    client = storage.Client()
    bucket = client.bucket(bucket_name)

    df_models = pd.read_parquet("../models.parquet")

    print("Generating chart for Evolution of new AI models ...")

    def filter_models(
        df,
        tags,
    ):
        filtered_models = df[df["pipeline_tag"].isin(tags)]
        return (
            filtered_models.groupby("month").size().reset_index(name="Number of models")
        )

    not_LM_models = filter_models(
        df_models[df_models["pipeline_tag"] != ""],
        set(df_models["pipeline_tag"]) - set(config["language-model-tags"]),
    )
    not_LM_models["Model type"] = "Other models"

    LM_models = filter_models(
        df_models[df_models["pipeline_tag"] != ""],
        config["language-model-tags"],
    )
    LM_models["Model type"] = "Language models"

    df_plot = pd.concat(
        [
            not_LM_models,
            LM_models,
        ],
        ignore_index=True,
    )

    upload_table_to_bigquery(
        bigquery_client,
        dataset_ref,
        df_plot,
        "Evolution of new AI models",
    )

    print("Generating chart for Evolution of new AI models per intended use ...")

    df_plot = df_models[df_models["pipeline_tag"] != ""].copy()
    df_plot = df_plot[
        [
            "pipeline_tag",
            "month",
        ]
    ]
    df_plot = (
        df_plot.groupby(
            [
                "month",
                "pipeline_tag",
            ]
        )
        .size()
        .reset_index(name="count")
    )
    df_plot = df_plot.rename(
        columns={
            "month": "Date",
            "pipeline_tag": "Intended Use",
        }
    )
    df_plot["Intended Use"] = (
        df_plot["Intended Use"]
        .str.replace(
            "-",
            " ",
        )
        .str.capitalize()
    )

    upload_table_to_bigquery(
        bigquery_client,
        dataset_ref,
        df_plot,
        "Evolution of new AI models per intended use",
    )

    print("Generating chart for Average number of parameters of new AI models ...")

    bucket = config["files"]["path"]
    model_num_params_info = config["files"]["model-num-params-info"]

    df_models = df_models[df_models["downloads"] >= int(config["min-downloads"])]
    params_df = pd.read_csv(f"gs://{bucket}/{model_num_params_info}")

    df_models = pd.merge(
        df_models,
        params_df,
        on="model_name",
        how="left",
    )
    df_models.fillna(
        "",
        inplace=True,
    )
    df_models = df_models[df_models["num_params"] != ""]
    df_models = df_models[df_models["pipeline_tag"] != ""]
    df_models["num_params"] = df_models["num_params"].astype(int)
    df_models.index = list(range(len(df_models)))

    def filter_and_aggregate(
        df,
        tags,
    ):
        filtered_models = df[df["pipeline_tag"].isin(tags) & (df["num_params"] != "")]
        mean_n_params_by_month = filtered_models.groupby("month")["num_params"].mean()
        return pd.DataFrame(
            {
                "Date": mean_n_params_by_month.index,
                "Average n. parameters": mean_n_params_by_month.values,
            }
        )

    not_LM_models = filter_and_aggregate(
        df_models,
        set(df_models["pipeline_tag"]) - set(config["language-model-tags"]),
    )

    not_LM_models = not_LM_models.sort_values(by="Date")

    def moving_average(data):
        return data.rolling(window=3, min_periods=1).mean()

    not_LM_models["Moving Average"] = moving_average(
        not_LM_models["Average n. parameters"]
    )

    not_LM_models["Model type"] = "Other models"

    LM_models = filter_and_aggregate(
        df_models,
        set(config["language-model-tags"]),
    )

    LM_models = LM_models.sort_values(by="Date")

    def moving_average(data):
        return data.rolling(window=3, min_periods=1).mean()

    LM_models["Moving Average"] = moving_average(LM_models["Average n. parameters"])

    LM_models["Model type"] = "Language models"

    df_plot = pd.concat(
        [
            not_LM_models,
            LM_models,
        ],
        ignore_index=True,
    )

    upload_table_to_bigquery(
        bigquery_client,
        dataset_ref,
        df_plot,
        "Average number of parameters of new AI models",
    )

    print("Generating chart for Average training costs of new AI models ...")

    df_plot["flops_for_training"] = (
        6 * df_plot["Moving Average"] * 20 * df_plot["Moving Average"]
    )
    df_plot["actual_flops_for_training"] = (
        df_plot["flops_for_training"] / config["training-cost-info"]["MFU"]
    )
    df_plot["training_time_in_gpu_seconds"] = (
        df_plot["actual_flops_for_training"] / config["training-cost-info"]["GPU_FLOPs"]
    )
    df_plot["training_cost"] = (
        df_plot["training_time_in_gpu_seconds"]
        * config["training-cost-info"]["COST_GPU_HOUR"]
        / 3600
    )

    upload_table_to_bigquery(
        bigquery_client,
        dataset_ref,
        df_plot,
        "Average training costs of new AI models",
    )
