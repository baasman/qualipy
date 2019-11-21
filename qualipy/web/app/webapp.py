from flask import Blueprint, redirect, render_template, request, url_for, session
from flask_login import current_user, login_required, login_user, logout_user
from flask import current_app as app
from werkzeug.urls import url_parse

from qualipy.web.app.extensions import db
from qualipy.web.app.forms import LoginForm
from qualipy.web.app.models import User

import os
import json


main = Blueprint("main", __name__)


@main.route("/", methods=["GET", "POST"])
def index():
    config_dir = os.environ["CONFIG_DIR"]
    with open(os.path.join(config_dir, "projects.json"), "r") as f:
        projects = json.loads(f.read())

    if request.method == "POST":
        button_pressed = list(request.form.to_dict(flat=False).keys())[0]
        session["project_name"] = button_pressed
        session["schema"] = projects[button_pressed]["schema"]
        return redirect(url_for("main.render_dashboard"))

    return render_template("index.html", title="Home Page", projects=projects)


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
