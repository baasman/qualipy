from flask import Blueprint, redirect, render_template, request, url_for, session
from flask_login import current_user, login_required, login_user, logout_user
from flask import current_app as capp
from flask import current_app as app
from werkzeug.urls import url_parse

from qualipy.web.app.forms import LoginForm
from qualipy.web.app.models import User
from qualipy.web.app.tables import AnomalyTable
from qualipy.anomaly_detection import anomaly_data_all_projects
from qualipy.web.app.caching import (
    cache_dataframe,
    get_cached_dataframe,
    set_session_anom_data_name,
    get_and_cache_anom_table,
)

import os
import json
from uuid import uuid4


main = Blueprint("main", __name__)


@main.route("/", methods=["GET"])
@main.route("/index", methods=["GET"])
def index():
    config_dir = capp.config["CONFIG_DIR"]
    if current_user.is_authenticated:
        with open(os.path.join(config_dir, "projects.json"), "r") as f:
            projects = json.loads(f.read())
            session["all_projects"] = list(projects.keys())
        anom_data = get_and_cache_anom_table(all_projects=True)
        rows = anom_data.to_dict(orient="records")
        anom_table = AnomalyTable(
            rows, classes=["table", "table-striped"], table_id="anom-table-main"
        )
    else:
        return redirect(url_for("main.login"))

    return render_template(
        "index.html", title="Home Page", projects=projects, anom_table=anom_table
    )


@main.route("/project_request/<project_name>")
def handle_project_request(project_name):
    config_dir = capp.config["CONFIG_DIR"]
    with open(os.path.join(config_dir, "projects.json"), "r") as f:
        projects = json.loads(f.read())
    session["project_name"] = project_name
    session["schema"] = projects[project_name]["schema"]
    session["all_projects"] = list(projects.keys())
    return redirect(url_for("main.render_dashboard"))


@main.route("/dashboard/")
def render_dashboard():
    return redirect("/dashboard")


@main.route("/login/", methods=["GET", "POST"])
def login():
    if current_user.is_authenticated:
        if "_id" not in session:
            session["_id"] = str(uuid4())
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
        session["_id"] = str(uuid4())
        return redirect(next_page)

    return render_template("login.html", title="Sign In", form=form, projects={})


@main.route("/logout/")
@login_required
def logout():
    logout_user()

    return redirect(url_for("main.index"))
