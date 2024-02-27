# build me as docker build . -t my-airflow
# run me as docker run -ti -p 8080:8080 --env-file=.env my-airflow

FROM apache/airflow:2.6.2-python3.10

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    git \
    vim \
    s3cmd \
    && apt-get autoremove -yqq --purge \
    && apt-get clean -y \
    && rm -rf /var/lib/apt/lists/*


USER airflow

# TODO(vperron): Find a better versioning scheme for the requirements
# than the manually frozen requirements-ci.txt containing dev dependencies.
COPY requirements-ci.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt && rm requirements.txt
RUN pip install --no-cache-dir dbt-fal==1.5.4 dbt-core==1.5.1

COPY --chown=airflow:root . ./
COPY --chown=airflow:root .s3cfg /home/airflow

COPY custom-prod-entrypoint.sh /
ENTRYPOINT ["/usr/bin/dumb-init", "--", "/custom-prod-entrypoint.sh"]
