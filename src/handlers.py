import re
from datetime import date

import pandas as pd
from google.cloud import bigquery


def try_except_func(
    func,
    *args,
    **kw,
):
    try:
        return func(
            *args,
            **kw,
        )
    except Exception as e:
        print(
            *args,
            e,
        )
        return ""


def read_txt_from_bucket(
    bucket,
    blob_name,
):
    blob = bucket.blob(blob_name)

    return blob.download_as_bytes().decode("utf-8")


def upload_table_to_bigquery(
    bigquery_client,
    dataset_ref,
    df,
    table_name,
):
    df = df.copy()
    df.columns = df.columns.str.strip()
    df.columns = df.columns.str.replace(
        r"[^A-Za-z0-9_\s]",
        "",
        regex=True,
    )
    df.columns = df.columns.str.strip()
    df.columns = df.columns.str.replace(
        " ",
        "_",
    )
    df.fillna(
        "",
        inplace=True,
    )

    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].astype(str)

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
    )

    table_ref = dataset_ref.table(table_name)

    job = bigquery_client.load_table_from_dataframe(
        df,
        table_ref,
        job_config=job_config,
    )

    job.result()


def read_table_as_dataframe(
    bigquery_client,
    dataset_ref,
    table_name,
):
    table_ref = dataset_ref.table(table_name)
    table = bigquery_client.get_table(table_ref)
    rows = bigquery_client.list_rows(table)

    data = [row.values() for row in rows]
    column_names = [field.name for field in rows.schema]

    # Creating DataFrame
    df = pd.DataFrame(
        data,
        columns=column_names,
    )
    return df


def get_last_update_files(
    config,
    bucket,
    field,
    date_pattern,
):
    field = config["files"][field]
    path = config["files"]["path"]

    today = str(date.today())

    candidates = bucket.list_blobs()
    candidates = [blob.name for blob in candidates if field in blob.name]

    candidates_dates = [
        re.search(
            date_pattern,
            candidate,
        ).group()
        for candidate in candidates
    ]
    candidates_dates = sorted(
        candidates_dates,
        key=lambda x: tuple(
            map(
                int,
                x.split("-"),
            )
        ),
        reverse=True,
    )
    last_update_date = candidates_dates[0]

    last_updated_file = f"{path}/{field}-{last_update_date}.csv"
    today_file = f"{path}/{field}-{today}.csv"

    return (
        last_updated_file,
        today_file,
    )
