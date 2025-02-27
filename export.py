# from prefect import task, get_run_logger
import os
import numpy as np
import pandas as pd
from pathlib import Path
from tiled.client import from_profile
import time as ttime

tiled_client = from_profile("nsls2")["smi"]
tiled_client_raw = tiled_client["raw"]



@task
def export_amptek(uid):
    logger = get_run_logger()
    run = tiled_client_raw[uid]
    if "amptek_mca_spectrum" in run.primary.data:
        # retreive information from the start document
        cycle = run.metadata["start"]["cycle"]
        project = run.metadata["start"]["project_name"]
        datasession = run.metadata["start"]["data_session"]


        newDir = "/nsls2/data/smi/proposals/%s/%s/projects/%s/user_data/Amptek/"% (cycle, datasession, project)       
        
        common_column="amptek_energy_channels"
        columns=["amptek_mca_spectrum"]
        xr = run.primary.read()
        if columns is None:
            columns = list(xr.keys())

        all_columns = [common_column] + columns
        xr = xr[all_columns]

        num_events = xr.dims["time"]

        before_loop_time = ttime.time()
        logger.info(f"Start exporting of {num_events} spectra to {newDir}")
        for i in range(num_events):
            file = os.path.join(
                newDir,
                f"{run.metadata['start']['sample_name']}-{run.metadata['start']['uid'].split('-')[0]}-{i+1:05d}.csv",
            )

            data = getattr(xr, common_column)[i]
            for j in range(len(columns)):
                data = np.vstack([data, getattr(xr, columns[j])[i]])
            data = data.T

            df = pd.DataFrame(data=data, columns=all_columns)

            start_time = ttime.time()
            logger.info(f"Exporting data with shape {data.shape} to {file}...")
            df.to_csv(file, index=False)
            logger.info(f"Exporting to {file} took " f"{ttime.time() - start_time:.5f}s\n")

        logger.info(
            f"Exporting of {num_events} spectra took "
            f"{ttime.time() - before_loop_time:.5f}s"
        )

