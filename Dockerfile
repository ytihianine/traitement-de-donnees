# Define ARGS (Defaults, overridden in Git CI)
ARG AIRFLOW_VERSION=3.1.7

# ------------------------------------------
# Install apps and packages
# ------------------------------------------
FROM apache/airflow:${AIRFLOW_VERSION}

USER root
WORKDIR /app

COPY customCA.crt /usr/local/share/ca-certificates/customCA.crt
RUN update-ca-certificates

ENV SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt
ENV CURL_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt
ENV REQUESTS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt
ENV AWS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt

# Switch back to airflow user
USER airflow

# Install packages
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
