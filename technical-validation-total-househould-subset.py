"""
Use: python total-household-subset-technical-validation.py <data source path> <resutlts path>
"""

# python
import datetime
from typing import List, Tuple, Union
import glob
import os
from os.path import join, exists
from os import makedirs
from datetime import datetime, timedelta
from dateutil import relativedelta
import sys
import logging

# data-science
import pandas as pd
import numpy as np
import dask as dk
import dask.dataframe as dd
from dask.system import CPU_COUNT
from dask.distributed import Client, LocalCluster

print("Pandas version {}".format(pd.__version__), flush=True)


BASE_DATA_SOURCE = sys.argv[1] if sys.argv[1] else exit()
RESULTS_DEST   = sys.argv[2] if sys.argv[2] else exit()

# If destination directory does not exist, create it
if not exists(RESULTS_DEST):
    makedirs(RESULTS_DEST)


if __name__ == "__main__":

    # READ: https://stackoverflow.com/a/47776937/1674908
    pd.options.mode.chained_assignment = None

    print("Generating Dask Client...", flush=True)
    client = Client(
        # address="localhost:8786",
        # direct_to_workers=True,
        processes=True,
        # n_workers=int(CPU_COUNT/1.5)+1,
        silence_logs=logging.ERROR,
        local_directory=f"/scratch/{os.environ.get('SLURM_JOB_USER')}/{os.environ.get('SLURM_JOBID')}"
    )
    # READ: https://github.com/dask/dask/issues/4001#issuecomment-647463811
    dk.config.set({"optimization.fuse.ave-width": 20})

    # dk.config.set(scheduler='threads')

    print("Number of CPUs detected: {}".format(CPU_COUNT), flush=True)
    # Load consumption data
    df_consum = dd.read_parquet(
        BASE_DATA_SOURCE + '/consum-parquet',
    )

    # print(df_consum.head(npartitions=1), flush=True)
    # percentile = df_consum[["value"]][df_consum.value > 0].quantile(
    #     [.8, .9, .95, .975, .98, .985, .99, .995, 1]
    # ).compute()
    # print(percentile)

    # df_consum = df_consum[df_consum.value <= percentile[0.985]]
    df_consum = df_consum[df_consum.value <= 1.13]  # result of percentile calculated before

    df_consum_date_count = df_consum.groupby(
        [df_consum.id, df_consum.datetime.dt.date],
        observed=True,
    ).value.count().reset_index()

    df_consum_date_count.to_parquet(
        RESULTS_DEST + "/consumption_date_count-parquet",
        engine='pyarrow',
        compression='snappy',
        write_index=False
    )
    df_consum_date_count.compute().to_csv(
        RESULTS_DEST + "/consumption_date_count.csv",
        sep=",",
        header=True,
        index=False,
        float_format="%.3f",
    )

    print("Building query to calculate 15-min mean consumption with consum-filter-95...", flush=True)
    df_consum_15min = df_consum[["datetime", "value"]].set_index(
        "datetime"
    ).resample("15min").mean()
    # print("Executing the query...", flush=True)
    df_consum_15min_mean = df_consum_15min[["value"]].groupby(
        [df_consum_15min.index.hour, df_consum_15min.index.minute],
        observed=True,
    ).mean()

    print("Building query to calculate weekday mean consumption with consum-filter-95...", flush=True)
    df_consum_weekday = df_consum[["datetime", "value"]].groupby(
        [df_consum.datetime.dt.weekday],
        observed=True,
    ).mean()

    # print(df_consum_15min_mean.head())
    print("Computing consumption 15-min...", flush=True)
    df_consum_15min_mean.compute().to_csv(
        RESULTS_DEST + "/consumption_15min_mean.csv",
        sep=",",
        header=True,
        index=True,
        float_format="%.3f",
    )

    # print(df_consum_weekday.head())
    print("Computing consumption weekday...", flush=True)
    df_consum_weekday.compute().to_csv(
        RESULTS_DEST + "/consumption_weekday_mean.csv",
        sep=",",
        header=True,
        index=True,
        float_format="%.3f",
    )

    print("FIN.")
