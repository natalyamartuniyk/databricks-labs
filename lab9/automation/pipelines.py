import logging
import time
from itertools import islice

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.pipelines import PipelineState, UpdateInfoState

logger = logging.getLogger(__name__)

TERMINAL_STATES = {
    UpdateInfoState.COMPLETED,
    UpdateInfoState.FAILED,
    UpdateInfoState.CANCELED,
}

BUSY_STATES = {
    PipelineState.DEPLOYING,
    PipelineState.STARTING,
    PipelineState.RUNNING,
    PipelineState.STOPPING,
    PipelineState.RECOVERING,
    PipelineState.RESETTING,
}


def find_pipeline_id(w: WorkspaceClient, name: str) -> str:
    """Return the id of the only pipeline with exactly this name."""
    matches = list(w.pipelines.list_pipelines(filter=f"name LIKE '{name}'"))
    if len(matches) != 1:
        raise ValueError(f"Expected one pipeline named '{name}', found {len(matches)}")
    return matches[0].pipeline_id


def start_pipeline_update(w: WorkspaceClient, pipeline_id: str) -> str:
    """Start an incremental update if the pipeline is free. Return the update id."""
    pipeline = w.pipelines.get(pipeline_id=pipeline_id)
    if pipeline.state in BUSY_STATES:
        raise RuntimeError(
            f"Pipeline '{pipeline.name}' is busy (state: {pipeline.state.value})"
        )

    response = w.pipelines.start_update(pipeline_id=pipeline_id, full_refresh=False)
    logger.info("Started update %s of pipeline '%s'", response.update_id, pipeline.name)
    return response.update_id


def wait_for_update(
    w: WorkspaceClient,
    pipeline_id: str,
    update_id: str,
    timeout_minutes: int = 40,
    poll_seconds: int = 15,
) -> UpdateInfoState:
    """Poll the update until it reaches a terminal state. Stop it on timeout."""
    deadline = time.monotonic() + timeout_minutes * 60

    while True:
        update = w.pipelines.get_update(pipeline_id=pipeline_id, update_id=update_id).update
        state = update.state
        logger.info("Pipeline update %s state: %s", update_id, state.value)

        if state in TERMINAL_STATES:
            return state

        if time.monotonic() > deadline:
            logger.error("Timeout after %s min, stopping the pipeline", timeout_minutes)
            w.pipelines.stop(pipeline_id=pipeline_id)
            raise TimeoutError(f"Pipeline update {update_id} did not finish in time")

        time.sleep(poll_seconds)


def report_update(
    w: WorkspaceClient,
    pipeline_id: str,
    update_id: str,
    state: UpdateInfoState,
) -> bool:
    """Log the result of a finished update. Return True only on COMPLETED."""
    if state == UpdateInfoState.COMPLETED:
        logger.info("Pipeline update %s completed", update_id)
        return True

    logger.error("Pipeline update %s finished with state %s", update_id, state.value)

    error_events = w.pipelines.list_pipeline_events(
        pipeline_id=pipeline_id, filter="level='ERROR'"
    )
    for event in islice(error_events, 20):
        if event.origin and event.origin.update_id == update_id:
            logger.error("  %s", event.message)
    return False