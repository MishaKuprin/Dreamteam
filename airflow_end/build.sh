#!/usr/bin/env bash

docker build . -f Dockerfile_spark -t airflow-etl

docker-compose -f docker-compose-spark.yml up -d --build