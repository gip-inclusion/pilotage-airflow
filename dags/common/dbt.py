import os


def get_default_args():
    return {
        "cwd": os.getenv("AIRFLOW_BASE_DIR"),
    }
