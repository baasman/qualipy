from qualipy.helper.auto_qpy import setup_auto_qpy, auto_qpy_profiler
import qualipy as qpy


def qualipy_pipeline(configuration_directory, project_name):

    eye_state = qpy.datasets.load_dataset("eye_state")

    project = setup_auto_qpy(
        data=eye_state,
        configuration_dir=configuration_directory,
        project_name=project_name,
    )
    auto_qpy_profiler(eye_state, project)


if __name__ == "__main__":
    config_dir = "~/eye-state-from-helper"
    project_name = "eye_state_helper"
    qualipy_pipeline(configuration_directory=config_dir, project_name=project_name)