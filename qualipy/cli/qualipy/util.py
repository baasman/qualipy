import click

import qualipy as qpy


def clear_data_cli(config_dir, project_name, recreate=True, confirm=True):
    project = qpy.Project(
        config_dir=config_dir, project_name=project_name, re_init=True
    )
    print(
        f"Preparing to delete table {project_name} as specified in config dir {config_dir}"
    )
    if confirm:
        if click.confirm(
            "Do you wish to continue? Warning - this will permanently delete data"
        ):
            project.delete_data(recreate=recreate)
            if not recreate:
                project.delete_from_project_config()
    else:
        project.delete_data(recreate=recreate)
        if not recreate:
            project.delete_from_project_config()