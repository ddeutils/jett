FROM rockylinux:9.3

RUN dnf install -y \
      gcc \
    && dnf clean all

ENV UV_COMPILE_BYTECODE=1
ENV PIP_NO_CACHE_DIR=1
ENV PIP_DISABLE_PIP_VERSION_CHECK=1

COPY --from=ghcr.io/astral-sh/uv:0.8.7 /uv /uvx /bin/

ARG AIRFLOW_VERSION=2.7.1
ARG PYTHON_VERSION=3.10

RUN uv venv /opt/venv --python=${PYTHON_VERSION}

ENV VIRTUAL_ENV=/opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# WARNING: Install pendulum first because Airflow version 2.7 does not compatible
#   if it more than 3.0.0
RUN uv pip install "pendulum<3.0.0" && \
    uv pip install "apache-airflow[google]==${AIRFLOW_VERSION}" \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# NOTE: Force change version of necessary version of deps package for DAG Tool.
RUN uv pip install \
      "PyYAML==6.0.2" \
      "python-dotenv==1.1.1" \
      "pydantic==2.11.7" \
      "click==8.1.8" \
      "ddeutil-io[yaml,toml]==0.2.17" \
      "requests==2.32.4"

COPY ./jett /opt/airflow/plugins/jett

WORKDIR /opt/airflow
