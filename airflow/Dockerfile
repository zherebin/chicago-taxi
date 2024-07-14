FROM apache/airflow:2.9.2

ENV AIRFLOW_HOME=/opt/airflow

USER airflow
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

USER root
ARG CLOUDSDK_INSTALL_DIR=/usr/local
ARG CLOUDSDK_CORE_DISABLE_PROMPTS=1
RUN curl --silent https://sdk.cloud.google.com | bash > /dev/null
ENV PATH=${PATH}:${CLOUDSDK_INSTALL_DIR}/google-cloud-sdk/bin

COPY scripts scripts
RUN chmod +x scripts/entrypoint.sh

WORKDIR $AIRFLOW_HOME

USER airflow