#!/bin/bash
eval "cd $AIRFLOW_HOME"
eval ". ubuntu_airflow_venv/bin/activate"
eval "airflow webserver"