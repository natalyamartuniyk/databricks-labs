import logging
from datetime import timedelta

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs
from databricks.sdk.service.jobs import Run, RunResultState

logger = logging.getLogger(__name__)


def submit_notebook(
    w: WorkspaceClient,
    cluster_id: str,
    notebook_path: str,
    parameters: dict[str, str],
    run_name: str = "lab9-notebook-run",
):
    return w.jobs.submit(
        run_name=run_name,
        tasks=[
            jobs.SubmitTask(
                task_key="notebook",
                existing_cluster_id=cluster_id,
                notebook_task=jobs.NotebookTask(
                    notebook_path=notebook_path,
                    base_parameters=parameters,
                ),
            )
        ],
    )


def wait_for_run(waiter, timeout_minutes: int = 30) -> Run:

    def log_state(run: Run) -> None:
        logger.info("Run %s state: %s", run.run_id, run.state.life_cycle_state.value)

    return waiter.result(
        timeout=timedelta(minutes=timeout_minutes),
        callback=log_state,
    )


def report_run(w: WorkspaceClient, run: Run) -> bool:
    result = run.state.result_state
    result_text = result.value if result else "no result"

    logger.info(
        "Run %s finished: %s / %s",
        run.run_id, run.state.life_cycle_state.value, result_text,
    )
    logger.info("Run page: %s", run.run_page_url)

    if result == RunResultState.SUCCESS:
        return True

    logger.error("Run %s failed: %s", run.run_id, run.state.state_message)
    for task in run.tasks or []:
        output = w.jobs.get_run_output(run_id=task.run_id)
        if output.error:
            logger.error("Task '%s' failed: %s", task.task_key, output.error)
    return False
