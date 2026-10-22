from airflow.hooks.base import BaseHook

import requests

class JsonPlaceHolderHook(BaseHook):
    """Hook to interact with the public JSONPlaceholder API."""

    conn_name_attr = "jsonplaceholder_api_conn_id"
    default_conn_name = "jsonplaceholder"

    def __init__(self, conn_id: str = default_conn_name):
        super().__init__()
        self.conn_id = conn_id
        self._session = None

    def get_conn(self) -> requests.Session:
        if self._session:
            return self._session

        conn = self.get_connection(self.conn_id)  # reads from Airflow Connections
        self._session = requests.Session()
        self._session.headers.update({
            "Base-URL": conn.host,
        })
        return self._session

    def get_all_posts(self) -> dict:
        session = self.get_conn()
        conn = self.get_connection(self.conn_id)
        response = session.get(f"{conn.host}/posts")
        response.raise_for_status()
        return response.json()