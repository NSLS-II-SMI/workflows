from prefect import task, flow, get_run_logger
from prefect.task_runners import ConcurrentTaskRunner
from data_validation import read_all_streams
from linker import get_symlink_pairs


@task
def log_completion():
    logger = get_run_logger()
    logger.info("Complete")


@flow(task_runner=ConcurrentTaskRunner())
def end_of_run_workflow(stop_doc):
    logger = get_run_logger()
    uid = stop_doc["run_start"]

    # Launch validation and linker concurrently.
    det_map = {"900KW": "WAXS", "1M": "SAXS"}
    linker_task = get_symlink_pairs.submit(uid, det_map=det_map)
    logger.info("Launched linker task")
    validation_task = read_all_streams.submit(uid, beamline_acronym="smi")
    logger.info("Launched validation task")

    # Wait for completion.
    logger.info("Waiting for tasks to complete")
    validation_task.result()
    linker_task.result()

    log_completion()
