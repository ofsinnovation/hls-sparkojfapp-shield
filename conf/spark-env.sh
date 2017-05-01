#!/usr/bin/env bash


export SPARK_LOCAL_IP=$HEROKU_PRIVATE_IP

export SPARK_PUBLIC_DNS=$HEROKU_DNS_DYNO_NAME

export LD_LIBRARY_PATH=/app/spark-home/lib