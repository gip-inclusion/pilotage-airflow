# build me as docker build -t . my-airflow
# run me as docker run -ti -p 8080:8080 --env-file=.env my-airflow

FROM apache/airflow:2.5.1-python3.10

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    git \
    && apt-get autoremove -yqq --purge \
    && apt-get clean -y \
    && rm -rf /var/lib/apt/lists/*


USER airflow

COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt \
    && rm requirements.txt

COPY airflow/ .

CMD ["bash", "-c", "./entrypoint.sh"]
