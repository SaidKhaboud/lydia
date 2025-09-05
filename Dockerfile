# Use the official Apache Airflow image
FROM apache/airflow:2.11.0-python3.11

# Install Python packages
USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    && apt-get clean

USER airflow

# Copy the requirements file into the container
WORKDIR /airflow
COPY pyproject.toml pyproject.toml
COPY uv.lock uv.lock


# Install Python packages from the requirements file
RUN pip install --no-cache-dir --upgrade pip setuptools wheel uv
RUN uv pip install .

ENV PYTHONPATH="${PYTHONPATH}:/airflow/airflow"