FROM public.ecr.aws/docker/library/python:3.12-slim

WORKDIR /app

# Download the Amazon RDS CA bundle for verified TLS connections
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl \
    && curl -sSL -o /app/rds-ca-bundle.pem https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem \
    && apt-get purge -y curl && apt-get autoremove -y && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY cdc_reader.py .

# Run as non-root user
RUN useradd -r -s /bin/false cdcuser && chown -R cdcuser:cdcuser /app
USER cdcuser

CMD ["python", "-u", "cdc_reader.py"]
