FROM apache/airflow:2.9.3-python3.10

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

COPY requirements/base.txt requirements.txt
RUN pip install --no-cache-dir --requirement requirements.txt

COPY --chown=airflow:root . ./

COPY custom-prod-entrypoint.sh /entrypoint.sh
ENTRYPOINT ["/usr/bin/dumb-init", "--", "/entrypoint.sh"]
