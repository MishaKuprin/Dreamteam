#!/bin/bash
eval "sudo service postgresql start"
eval "cd $AIRFLOW_HOME\r"
sh "./ubuntu_airflow_venv/bin/activate"
eval "airflow scheduler"
