from airflow.models import Variable


def bucket_connection():
    host = Variable.get("HOST_BUCKET_C2")
    password = Variable.get("PASSWORD_BUCKET_C2")
    user = Variable.get("USER_BUCKET_C2")
    return host, user, password
