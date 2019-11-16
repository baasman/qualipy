import os
import dash

from flask import Flask
from flask.helpers import get_root_path
from flask_login import login_required
import flask_migrate
from alembic import command
from alembic.config import Config as AlembicConfig
from alembic.runtime.migration import MigrationContext
from sqlalchemy_utils import database_exists

from web.config import BaseConfig, read_project_config
from qualipy.web.app.models import User


def create_app():
    server = Flask(__name__)
    server.config.from_object(BaseConfig)
    server.config.update(**read_project_config(os.environ["CONFIG_DIR"]))

    register_dashapps(server)
    register_extensions(server)
    register_blueprints(server)
    register_cache(server)

    return server


def register_dashapps(app):
    from qualipy.web.app.metric_tracker.layout import generate_layout
    from qualipy.web.app.metric_tracker.callbacks import register_callbacks

    meta_viewport = {
        "name": "viewport",
        "content": "width=device-width, initial-scale=1, shrink-to-fit=no",
    }

    metric_tracker_app = dash.Dash(
        __name__,
        server=app,
        url_base_pathname="/dashboard/",
        assets_folder=get_root_path(__name__) + "/dashboard/assets/",
        meta_tags=[meta_viewport],
    )

    with app.app_context():

        # metric tracker
        metric_tracker_layout = generate_layout(["None"], ["None"], [], 1000000)
        metric_tracker_app.title = "Metric Tracker"
        metric_tracker_app.layout = metric_tracker_layout
        metric_tracker_app.config["suppress_callback_exceptions"] = True
        register_callbacks(metric_tracker_app)

    _protect_dashviews(metric_tracker_app)


def _protect_dashviews(dashapp):
    for view_func in dashapp.server.view_functions:
        if view_func.startswith(dashapp.config.url_base_pathname):
            dashapp.server.view_functions[view_func] = login_required(
                dashapp.server.view_functions[view_func]
            )


def register_extensions(server):
    from qualipy.web.app.extensions import db
    from qualipy.web.app.extensions import login
    from qualipy.web.app.extensions import migrate

    # db
    db.init_app(server)
    migrate.init_app(server, db)

    register_db_migrations(server, migrate, db)

    login.init_app(server)
    login.login_view = "main.login"


def register_blueprints(server):
    from qualipy.web.app.webapp import main

    server.register_blueprint(main)


def register_cache(server):
    from qualipy.web.app.caching import cache

    cache.init_app(server)


def register_db_migrations(server, migrate, db):
    with server.app_context():
        conn = db.engine.connect()
        context = MigrationContext.configure(conn)
        db_revision = context.get_current_revision()

    if server.config["DB_AUTO_CREATE"] and not database_exists(
        server.config["SQLALCHEMY_DATABASE_URI"]
    ):
        with server.app_context():
            print("creating db")
            db.create_all()
            flask_migrate.init(server.config["MIGRATIONS_DIR"])
            command.stamp(migrate.get_config(server.config["MIGRATIONS_DIR"]), "head")
            add_admin(db)

    if server.config["DB_AUTO_UPGRADE"]:
        with server.app_context():
            command.upgrade(migrate.get_config(server.config["MIGRATIONS_DIR"]), "head")


def add_admin(db):
    admin = User(username="admin")
    admin.set_password("admin")
    db.session.add(admin)
    db.session.commit()
