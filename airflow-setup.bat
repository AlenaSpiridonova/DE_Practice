@echo off
if not exist docker-compose.yaml (
    curl -O https://airflow.apache.org/docs/apache-airflow/2.8.1/docker-compose.yaml
)
echo AIRFLOW_UID=501 > .env
powershell -Command "(Get-Content docker-compose.yaml) | ForEach-Object { $_ -replace '^(\s+AIRFLOW__CORE__LOAD_EXAMPLES:).+$','$1 ''false'''} | Set-Content docker-compose.yaml"
mkdir dags logs plugins config
docker-compose up airflow-init
docker compose up