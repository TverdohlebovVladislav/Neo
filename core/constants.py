import json
from msilib import schema
from airflow.models.connection import Connection

connection_ds = Connection(
    conn_id="ds",
    conn_type="postgres",
    description="Connect to DS schema",
    host="postgres",
    login="airflow",
    password="airflow",
    # schema=""
    extra=json.dumps(dict(this_param="some val", that_param="other val*")),
)