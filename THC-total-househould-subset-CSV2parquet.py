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
        processes=True,
        silence_logs=logging.ERROR
    )
    # READ: https://github.com/dask/dask/issues/4001#issuecomment-647463811
    dk.config.set({"optimization.fuse.ave-width": 20})

    print("Number of CPUs detected: {}".format(CPU_COUNT), flush=True)
    N_WORKERS = int(CPU_COUNT/1.5)+1
    print("Generating {} workers...".format(N_WORKERS), flush=True)
    print("Generando LocalCluster...", flush=True)
    cluster = LocalCluster(
        processes=True,
        n_workers=N_WORKERS,
        threads_per_worker=2,
        local_directory=f"/scratch/{os.environ.get('SLURM_JOB_USER')}/{os.environ.get('SLURM_JOBID')}"
    )
    print("Generando Client...", flush=True)
    client = Client(cluster)
    print("Scaling cluster...", flush=True)
    cluster.scale(CPU_COUNT)

    # Load consumption data
    df_consum = dd.read_csv(
        # BASE_DATA_SOURCE + '/consumption_data_*.csv',
        BASE_DATA_SOURCE + '/consumption_data_*.csv',
        delimiter=',',
        header=0,
        names=["datetime", "id", "value"],
        parse_dates=["datetime"],
        date_parser=lambda epoch: pd.Timestamp(int(epoch), unit='s', tz='America/Montevideo'),
        # date_parser=lambda epoch: pd.to_datetime(epoch, unit='s').tz_localize('UTC').tz_convert('America/Montevideo'),
        dtype={'id': np.int32, 'value': np.float32},
    )

    print("Writing parquet files...", flush=True)
    df_consum.to_parquet(
        RESULTS_DEST,
        engine='pyarrow',
        compression='snappy',
        write_index=False
    )

    client.close()

    print("FIN.")
