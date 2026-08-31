FROM apache/airflow:3.3.1

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Salin project dbt ke dalam image dengan kepemilikan yang benar
COPY --chown=airflow:0 dbt/ /opt/airflow/dbt/

# Buat profil dummy dan hasilkan manifest.json
RUN mkdir -p /tmp/dummy_profile && \
    cat <<EOF > /tmp/dummy_profile/profiles.yml
lion_dwh:
  target: prod
  outputs:
    prod:
      type: clickhouse
      schema: dummy
      host: dummy
      user: dummy
      password: dummy
      port: 9000
EOF

# Build-time parsing: manifest.json dibuat sekali di sini agar Cosmos tidak perlu
# menjalankan 'dbt parse' berulang di scheduler. Ditaruh di direktori terpisah agar
# tidak tertimpa bind mount ./airflow-dbt/dbt dari host.
# Catatan: 'dbt deps' sengaja tidak dipanggil -- proyek ini tidak punya packages.yml.
# Tambahkan kembali bila kelak ada dependensi paket dbt.
RUN mkdir -p /opt/airflow/dbt_manifest && \
    cd /opt/airflow/dbt/lion_parcel_project && \
    dbt parse --profiles-dir /tmp/dummy_profile --target-path /opt/airflow/dbt_manifest
