# Define ARGS (Defaults, overridden in Git CI)
ARG AIRFLOW_VERSION=3.1.6

# ------------------------------------------
# Install apps and packages
# ------------------------------------------
FROM apache/airflow:${AIRFLOW_VERSION}

WORKDIR /app

# Switch back to airflow user
USER airflow

# Install packages
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
