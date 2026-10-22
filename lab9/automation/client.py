from databricks.sdk import WorkspaceClient

def get_client() -> WorkspaceClient:
    return WorkspaceClient()

def whoami(w: WorkspaceClient) -> str:
    me = w.current_user.me()
    return me.user_name