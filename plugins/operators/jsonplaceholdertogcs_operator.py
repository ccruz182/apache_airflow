from airflow.models import BaseOperator
from hooks.jsonplaceholder_hook import JsonPlaceHolderHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook

import json

class JsonPlaceholderToGCSOperator(BaseOperator):
    template_fields = ("gcs_path", "bucket")  # ← these support Jinja templating

    def __init__(
        self,
        bucket: str,
        gcs_path: str,
        jph_conn_id: str = "jsonplaceholder",
        gcp_conn_id: str = "google_cloud_default",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.bucket = bucket
        self.gcs_path = gcs_path
        self.jph_conn_id = jph_conn_id
        self.gcp_conn_id = gcp_conn_id

    def execute(self, context):
        jph_hook = JsonPlaceHolderHook(conn_id=self.jph_conn_id)
        posts = jph_hook.get_all_posts()

        gcs_hook = GCSHook(gcp_conn_id=self.gcp_conn_id)
        gcs_hook.upload(
            bucket_name=self.bucket,
            object_name=self.gcs_path,
            data=json.dumps(posts).encode("utf-8"),
            mime_type="application/json",
        )
        self.log.info(f"Uploaded to gs://{self.bucket}/{self.gcs_path}")

        return f"gs://{self.bucket}/{self.gcs_path}"  # available via .output