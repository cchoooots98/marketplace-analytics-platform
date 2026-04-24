"""Bootstrap an Airflow admin user via FAB security manager.

Workaround for apache-airflow-providers-fab 3.6.1 on Airflow 3.x: the
`airflow users create` CLI calls appbuilder.sm.find_role(), but in Airflow 3.x
appbuilder.sm is AirflowSecurityManagerV2 (which lacks find_role). Constructing
FabAirflowSecurityManagerOverride directly against the same DB avoids the broken
code path while writing to the same FAB tables that `airflow db migrate` created.
"""

import os
import sys

from airflow.configuration import conf
from flask import Flask
from flask_appbuilder import AppBuilder
from flask_sqlalchemy import SQLAlchemy

from airflow.providers.fab.auth_manager.security_manager.override import (
    FabAirflowSecurityManagerOverride,
)

db_url = conf.get("database", "sql_alchemy_conn")

app = Flask(__name__)
app.config.update(
    SQLALCHEMY_DATABASE_URI=db_url,
    SECRET_KEY=os.environ.get("SECRET_KEY", "init-bootstrap"),
    WTF_CSRF_ENABLED=False,
    AUTH_TYPE=1,
)

db = SQLAlchemy(app)

with app.app_context():
    ab = AppBuilder(
        app,
        db.session,
        security_manager_class=FabAirflowSecurityManagerOverride,
    )
    sm = ab.sm
    sm.sync_roles()

    username = os.environ.get("AIRFLOW_ADMIN_USERNAME", "airflow")
    password = os.environ.get("AIRFLOW_ADMIN_PASSWORD", "airflow")
    email = os.environ.get("AIRFLOW_ADMIN_EMAIL", "airflow@example.com")

    role = sm.find_role("Admin")
    if not role:
        print("ERROR: Admin role not found after sync_roles", file=sys.stderr)
        sys.exit(1)

    if sm.find_user(username=username):
        print(f"User '{username}' already exists, skipping creation.")
    else:
        sm.add_user(username, "Merchant", "Pulse", email, role, password)
        print(f"User '{username}' created successfully.")
