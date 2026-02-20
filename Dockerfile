# Define ARGS (Defaults, overridden in Git CI)
ARG AIRFLOW_VERSION=3.1.7

# ------------------------------------------
# Install apps and packages
# ------------------------------------------
FROM apache/airflow:${AIRFLOW_VERSION}

USER root
WORKDIR /app

# Add Nubonyxia cert
COPY scripts/files/customCA.crt /usr/local/share/ca-certificates/customCA.crt
RUN update-ca-certificates

# Install packages
COPY requirements.txt .
RUN pip install --no-cache-dir uv
RUN uv pip install --system -r requirements.txt
# RUN pip install --no-cache-dir -r requirements.txt

# Switch back to airflow user
USER airflow
