import concurrent.futures
import datetime
from pprint import pprint

import analyses_config
import pandas as pd
import util_alert
from backend_processing import delete_bq_partition, load_bq_schema
from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from loguru import logger

# V0 ANALYSIS #


def sql_escaping(list_as_str: str) -> str:
    # Converts a list string like 1,2,3 into '1','2','3'
    items = [item.strip() for item in list_as_str.split(",")]
    quoted_items = [f"'{item}'" for item in items]
    return ", ".join(quoted_items)


def parse_filter_csv(csv_fp):
    filter = pd.read_csv(csv_fp, delimiter=",")
    # print(filter)

    template = """WHERE {filter1_c} IN ({filter1_v})
AND {filter2_c} IN ({filter2_v})
AND {filter3_c} IN ({filter3_v})"""
    int_columns = ["bedrooms", "1"]

    filters = []
    for row in filter.iterrows():
        r = row[1]
        filter_d = {
            "code": r["filter_code"],
            "desc": r["filter_description"],
        }
        for f in ["filter1", "filter2", "filter3"]:
            filter_c = r[f"{f}_c"]
            filter_v = r[f"{f}_v"]
            if not filter_c in int_columns:
                filter_v = sql_escaping(filter_v)

            filter_d[f] = {"column": filter_c, "values": filter_v}

        # also form the sql templates

        sql = template.format(
            filter1_c=filter_d["filter1"]["column"],
            filter1_v=filter_d["filter1"]["values"],
            filter2_c=filter_d["filter2"]["column"],
            filter2_v=filter_d["filter2"]["values"],
            filter3_c=filter_d["filter3"]["column"],
            filter3_v=filter_d["filter3"]["values"],
        )
        filter_d["filter_clause"] = sql
        filters.append(filter_d)

    return filters


def execute_for_filter(params):
    f = params["filter"]
    bq_d = params["bq_d"]
    full_sql = bq_d["sql_template"].format(
        partition_date=bq_d["partition_date"],
        filter_code=f["code"],
        filter_description=f["desc"],
        filter_clause=f["filter_clause"],
    )
    # pprint(full_sql)
    logger.info(f"Execute for code: {f['code']}")
    row = bq_execute_query(
        full_sql,
        dataset_id=bq_d["dataset"],
        table_id=bq_d["table"],
        creds_fp=bq_d["creds_fp"],
    )
    return row


# V1 ANALYSIS VIZ-GROUP#


def execute_for_sql(params):
    bq_d = params["bq_d"]
    full_sql = bq_d["sql_template"].format(
        partition_date=bq_d["partition_date"],
        first_partition_date=params["first_partition_date"],
    )
    pprint(full_sql)
    # logger.info(f"Execute for code: {f['code']}")
    row = bq_execute_query(
        full_sql,
        dataset_id=bq_d["dataset"],
        table_id=bq_d["table"],
        creds_fp=bq_d["creds_fp"],
    )
    return row


# HELPER BQ #
def retrieve_analysis_config(
    table_name: str,
) -> analyses_config.AnalysisConfig:
    analysis_config = getattr(analyses_config, f"{table_name}_config")
    return analysis_config


def bq_execute_query(sql, dataset_id, table_id, creds_fp):
    # Initialize a BigQuery client
    client = bigquery.Client.from_service_account_json(creds_fp)
    try:
        client.get_table(f"{dataset_id}.{table_id}")
    except NotFound:
        logger.warning("Table does not exist")
        return False

    # Run the query
    query_job = client.query(sql)
    res = query_job.result()
    df = res.to_dataframe()
    # logger.info(f"Retrieved {len(df)} rows")
    # print(df)
    return df


def bq_insert_df(df, dataset_id, table_id, schema, creds_fp):
    # Convert DataFrame to BigQuery table format
    client = bigquery.Client.from_service_account_json(creds_fp)
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        schema=schema,
        write_disposition="WRITE_APPEND",
    )

    # Insert DataFrame into BigQuery
    job = client.load_table_from_dataframe(
        df, f"{dataset_id}.{table_id}", job_config=job_config
    )

    # Wait for the job to complete
    job.result()


def run(cmd_arg, config):

    try:

        t0 = cmd_arg.t
        t1 = cmd_arg.t1

        if not t1:
            date_list = [t0.strftime("%Y-%m-%d")]
        else:
            date_list = date_list = [
                (t0 + datetime.timedelta(days=i)).strftime("%Y-%m-%d")
                for i in range((t1 - t0).days + 1)
            ]

        logger.info(f"Running for partition dates: {date_list}")

        analysis_config = retrieve_analysis_config("agg_viz_group")
        # filters = parse_filter_csv("filter_table.csv") # Not needed for V1
        for partition_date in date_list:
            dataset = config["bq_dataset"]
            table = config["bq_raw_table"]
            creds_fp = config["gcs_sa_creds"]
            analysis_table = analysis_config.dest_table

            bq_d = {
                "dataset": dataset,
                "table": table,
                "partition_date": partition_date,
                "creds_fp": creds_fp,
                "sql_template": analyses_config.SQL_AGG_VIZ_GROUP,  # TODO: move this into a SQL attribute of the config if there are more
            }

            # Prepare the params list # This is only for the old agg sqls
            # params_ls = []
            # for f in filters:
            #     params = {}
            #     params["filter"] = f
            #     params["bq_d"] = bq_d
            #     params_ls.append(params)

            # # Excecute in parallel
            # with concurrent.futures.ThreadPoolExecutor() as executor:
            #     # Submit tasks and get futures
            #     futures = [
            #         executor.submit(execute_for_filter, params) for params in params_ls
            #     ]

            # # Collect results as they complete
            # results = []
            # for future in concurrent.futures.as_completed(futures):
            #         try:
            #             result = future.result()
            #             results.append(result)
            #         except Exception as exc:
            #             logger.error(f"Generated an exception: {exc}")
            # df_partition = pd.concat(results)

            params = {
                "bq_d": bq_d,
                "first_partition_date": "2024-09-03",
            }
            result = execute_for_sql(params)
            df_partition = result

            logger.info(
                f"Attempting delete of {dataset}.{analysis_table} partition: {partition_date}"
            )
            delete_bq_partition(dataset, analysis_table, partition_date, creds_fp)

            logger.info(
                f"Writing df of shape: {df_partition.shape} to {dataset}.{analysis_table}"
            )

            bq_schema = load_bq_schema(
                analysis_config.dest_bq, analysis_config.primary_key
            )

            bq_insert_df(df_partition, dataset, analysis_table, bq_schema, creds_fp)
            logger.info(f"Insert success.")
    except Exception as e:
        logger.error(e)
        util_alert.send_telegram(
            f'{config["job"]}.{cmd_arg.step}', partition_date, "ERROR", e
        )
        exit(1)


if __name__ == "__main__":
    filters = parse_filter_csv("filter_table.csv")
    pprint(filters)

    t0 = "2024-09-04"
    t1 = "2024-09-14"

    date_list = [
        "2024-09-04",
        "2024-09-05",
        "2024-09-06",
        "2024-09-07",
        "2024-09-08",
        "2024-09-09",
        "2024-09-10",
        "2024-09-11",
        "2024-09-12",
        "2024-09-13",
        "2024-09-14",
    ]

    dfs = []
    for partition_date in date_list:
        dataset = "pg_listings"
        table = "listings_raw"
        # partition_date = "2024-09-14"
        creds_fp = "creds/gcp_sa.json"

        bq_d = {
            "dataset": dataset,
            "table": table,
            "partition_date": partition_date,
            "creds_fp": creds_fp,
            "sql_template": analyses_config.SQL_AGG_STATISTICS_FILTER_GROUP,
        }

        # Prepare the params list
        params_ls = []
        for f in filters:
            params = {}
            params["filter"] = f
            params["bq_d"] = bq_d
            params_ls.append(params)

        # Excecute in parallel
        with concurrent.futures.ThreadPoolExecutor() as executor:
            # Submit tasks and get futures
            futures = [
                executor.submit(execute_for_filter, params) for params in params_ls
            ]

            # Collect results as they complete
            results = []
            for future in concurrent.futures.as_completed(futures):
                try:
                    result = future.result()
                    results.append(result)
                except Exception as exc:
                    print(f"Generated an exception: {exc}")

        df = pd.concat(results)

        print(df)
        df.to_csv(f"tmp/aggregate_{partition_date}", index=False)
        dfs.append(df)
    df_full = pd.concat(dfs)
    df_full.to_csv(f"tmp/aggregate_ALL", index=False)
