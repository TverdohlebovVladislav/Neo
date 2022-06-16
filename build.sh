echo -e "AIRFLOW_UID=$(id -u)" > .env
# docker build . -f Dockerfile -t airflow-etl
docker-compose up airflow-init
docker-compose down 
docker-compose build 
docker-compose up -d
