FROM postgres:16

RUN apt-get update && apt-get install -y postgresql-contrib

COPY ./docker/init-db.sql /docker-entrypoint-initdb.d/