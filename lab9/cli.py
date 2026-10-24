import sys
import argparse
import logging
from datetime import datetime
from pathlib import Path

import yaml
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunLifeCycleState

from automation.pipelines import (
    find_pipeline_id,
    report_update,
    start_pipeline_update,
    wait_for_update,
)
from automation.client import get_client, whoami
from automation.jobs import find_job_id, report_run, run_job, submit_notebook, wait_for_run
from automation.clusters import create_cluster, delete_cluster, wait_until_running

logger = logging.getLogger("cli")

CONFIG_PATH = Path(__file__).parent / "config.yml"

def load_config(target: str) -> dict:
    with open(CONFIG_PATH) as f:
        config = yaml.safe_load(f)
    if target not in config: 
        raise ValueError(f"Unknown target '{target}', expected one of {list(config)}")
    return config[target]


def cmd_run_pipeline(w: WorkspaceClient, config: dict, args) -> bool:
    pipeline_id = find_pipeline_id(w, config["pipeline_name"])
    logger.info("Pipeline '%s' has id %s", config["pipeline_name"], pipeline_id)
    update_id = start_pipeline_update(w, pipeline_id)
    state = wait_for_update(w, pipeline_id, update_id)
    return report_update(w, pipeline_id, update_id, state)

def cmd_run_job(w: WorkspaceClient, config: dict, args) -> bool:
    job_id = find_job_id(w, config["job_name"])
    run_waiter = run_job(w, job_id)
    logger.info("Started job %s, run %s", job_id, run_waiter.run_id)
    run = wait_for_run(run_waiter, timeout_minutes=60)
    return report_run(w, run)

def cmd_run_notebook(w: WorkspaceClient, config: dict, args) -> bool:
    cluster_cfg = config["cluster"]
    notebook_cfg = config["notebook"]
    name = f"{cluster_cfg['name_prefix']}-{datetime.now():%Y%m%d-%H%M%S}"
    name = f"{cluster_cfg['name_prefix']}-{datetime.now():%Y%m%d-%H%M%S}"

    waiter = create_cluster(
        w,
        name=name,
        node_type_id=cluster_cfg["node_type_id"],
        spark_version=cluster_cfg["spark_version"],
        policy_id=cluster_cfg["policy_id"],
    )
    cluster_id=waiter.cluster_id
    logger.info("Creating cluster '%s' (%s)", name, cluster_id)

    try:
        wait_until_running(waiter)
        run_waiter = submit_notebook(
            w, cluster_id, notebook_cfg["path"], notebook_cfg["parameters"]
        )
        logger.info("Submitted notebook run %s", run_waiter.run_id)
        run = wait_for_run(run_waiter)
        return report_run(w, run)
    finally:
        delete_cluster(w, cluster_id)
        logger.info("Cluster %s deleted", cluster_id)


def cmd_e2e(w: WorkspaceClient, config: dict, args) -> bool:
    if not cmd_run_job(w, config, args):
        logger.error("ETL job failed: data quality checks are skipped")
        return False
    return cmd_run_notebook(w, config, args)

def cmd_status(w: WorkspaceClient, config: dict, args) -> bool:
    run = w.jobs.get_run(run_id=args.run_id)
    if run.state.life_cycle_state != RunLifeCycleState.TERMINATED:
        logger.info("Run %s is %s", run.run_id, run.state.life_cycle_state.value)
        logger.info("Run page: %s", run.run_page_url)
        return True
    return report_run(w, run)


def cmd_cleanup(w: WorkspaceClient, config: dict, args) -> bool:
    prefix = config["cluster"]["name_prefix"]
    me = whoami(w)
    deleted = 0
    for cluster in w.clusters.list():
        if cluster.cluster_name.startswith(prefix) and cluster.creator_user_name == me:
            delete_cluster(w, cluster.cluster_id)
            logger.info("Deleted leftover cluster '%s'", cluster.cluster_name)
            deleted += 1
    logger.info("Cleanup finished: %s cluster(s) deleted", deleted)
    return True




def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Databricks platform automation (Lab 9)")
    parser.add_argument(
        "--target", default="dev", help="section of config.yml (default: dev)"
    )
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser(
        "run-pipeline", help="trigger the pipeline and wait for it"
    ).set_defaults(func=cmd_run_pipeline)
    commands.add_parser(
        "run-job", help="trigger the ETL job and wait for it"
    ).set_defaults(func=cmd_run_job)
    commands.add_parser(
        "run-notebook", help="temporary cluster + DQ notebook + cleanup"
    ).set_defaults(func=cmd_run_notebook)
    commands.add_parser(
        "e2e", help="ETL job, then DQ notebook on a temporary cluster"
    ).set_defaults(func=cmd_e2e)
    status = commands.add_parser("status", help="show the state of a run")
    status.add_argument("run_id", type=int, help="id of the run")
    status.set_defaults(func=cmd_status)
    commands.add_parser(
        "cleanup", help="delete leftover temporary clusters"
    ).set_defaults(func=cmd_cleanup)

    return parser


def main() -> int:
    args = build_parser().parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    config = load_config(args.target)
    w = get_client()
    logger.info(
        "Workspace: %s | user: %s | target: %s",
        w.config.host, whoami(w), args.target,
    )

    try:
        ok = args.func(w, config, args)
    except Exception:
        logger.exception("Command '%s' failed with an error", args.command)
        return 1

    if ok:
        logger.info("Command '%s' succeeded", args.command)
        return 0

    logger.error("Command '%s' failed", args.command)
    return 1

if __name__ == "__main__":
    sys.exit(main())



