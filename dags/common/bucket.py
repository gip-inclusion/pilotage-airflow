from airflow.models import Variable


def bucket_connection():
    hostbucket = Variable.get("HOSTBUCKET_C2")
    passwordbucket = Variable.get("PASSWORDBUCKET_C2")
    userbucket = Variable.get("USERBUCKET_C2")
    return hostbucket, userbucket, passwordbucket
