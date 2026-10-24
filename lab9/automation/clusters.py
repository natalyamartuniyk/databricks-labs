from datetime import timedelta

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import compute
from databricks.sdk.service.compute import ClusterDetails

from automation.client import whoami

import logging

logger = logging.getLogger(__name__)

def create_cluster(
        w: WorkspaceClient,
        name: str,
        node_type_id: str,
        spark_version: str,
        policy_id: str | None = None,
        autotermination_minutes: int = 20,
):
    return w.clusters.create(
        cluster_name=name,
        spark_version=spark_version,
        node_type_id=node_type_id,
        num_workers=0,
        spark_conf={
            "spark.databricks.cluster.profile": "singleNode",
            "spark.master": "local[*]",
        },
        custom_tags={"ResourceClass": "SingleNode"},
        data_security_mode=compute.DataSecurityMode.SINGLE_USER,
        single_user_name=whoami(w),
        autotermination_minutes=autotermination_minutes,
        policy_id=policy_id,
    )

def wait_until_running(waiter, timeout_minutes: int = 20) -> ClusterDetails:

    def log_state(cluster: ClusterDetails) -> None:
        logger.info("Cluster %s state: %s", cluster.cluster_id, cluster.state.value)

    return waiter.result(
        timeout=timedelta(minutes=timeout_minutes),
        callback=log_state,
    )

def delete_cluster(w: WorkspaceClient, cluster_id: str) -> None:
    w.clusters.permanent_delete(cluster_id=cluster_id)  