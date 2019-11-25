from flask import Blueprint, redirect, render_template, request, url_for, session
from flask_login import current_user, login_required, login_user, logout_user
from flask import current_app as capp
from flask import current_app as app
from werkzeug.urls import url_parse

from qualipy.web.app.extensions import db
from qualipy.web.app.forms import LoginForm
from qualipy.web.app.models import User
from qualipy.web.app.tables import AnomalyTable, Anomaly
from qualipy.anomaly_detection import anomaly_data_all_projects
from qualipy.web.app.caching import (
    cache_dataframe,
    get_cached_dataframe,
    set_session_anom_data_name,
)

import os
import json


main = Blueprint("main", __name__)


@main.route("/", methods=["GET"])
@main.route("/index", methods=["GET"])
def index():
    config_dir = os.environ["CONFIG_DIR"]
    with open(os.path.join(config_dir, "projects.json"), "r") as f:
        projects = json.loads(f.read())
    anom_data_session = set_session_anom_data_name(session["_id"])
    anom_data = get_cached_dataframe(anom_data_session)
    if anom_data is None:
        anom_data = anomaly_data_all_projects(
            project_names=list(projects.keys()),
            db_url=capp.config["QUALIPY_DB"],
            config_dir=config_dir,
        )
        cache_dataframe(anom_data, anom_data_session)
    rows = anom_data.to_dict(orient="records")
    anom_table = AnomalyTable(rows)

    return render_template(
        "index.html", title="Home Page", projects=projects, anom_table=anom_table
    )


@main.route("/project_request/<project_name>")
def handle_project_request(project_name):
    config_dir = os.environ["CONFIG_DIR"]
    with open(os.path.join(config_dir, "projects.json"), "r") as f:
        projects = json.loads(f.read())
    session["project_name"] = project_name
    session["schema"] = projects[project_name]["schema"]
    return redirect(url_for("main.render_dashboard"))


@main.route("/dashboard/")
def render_dashboard():
    return redirect("/dashboard")


@main.route("/login/", methods=["GET", "POST"])
def login():
    if current_user.is_authenticated:
        return redirect(url_for("main.index"))

    form = LoginForm()
    if form.validate_on_submit():
        user = User.query.filter_by(username=form.username.data).first()
        if user is None or not user.check_password(form.password.data):
            error = "Invalid username or password"
            return render_template("login.html", form=form, error=error)

        login_user(user, remember=form.remember_me.data)
        next_page = request.args.get("next")
        if not next_page or url_parse(next_page).netloc != "":
            next_page = url_for("main.index")
        return redirect(next_page)

    return render_template("login.html", title="Sign In", form=form)


@main.route("/logout/")
@login_required
def logout():
    logout_user()

    return redirect(url_for("main.index"))