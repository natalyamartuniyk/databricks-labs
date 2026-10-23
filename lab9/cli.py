import sys
import argparse
import logging
from pathlib import Path

import yaml
from databricks.sdk import WorkspaceClient

from automation.pipelines import (
    find_pipeline_id,
    report_update,
    start_pipeline_update,
    wait_for_update,
)
from automation.client import get_client, whoami

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


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Databricks platform automation (Lab 9)")
    parser.add_argument(
        "--target", default="dev", help="section of config.yml (default: dev)"
    )
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser(
        "run-pipeline", help="trigger the pipeline and wait for it"
    ).set_defaults(func=cmd_run_pipeline)
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



