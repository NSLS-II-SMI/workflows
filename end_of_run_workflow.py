from prefect import task, flow, get_run_logger
from prefect.task_runners import ConcurrentTaskRunner
from data_validation import data_validation
from linker import linker


@task
def log_completion():
    logger = get_run_logger()
    logger.info("Complete")


@flow(task_runner=ConcurrentTaskRunner())
def end_of_run_workflow(stop_doc):
    uid = stop_doc["run_start"]
    data_validation(uid)
    linker(uid)
    log_completion()
