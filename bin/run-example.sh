#!/bin/bash

## ENV
source ${HOME}/spark/conf/spark-env.sh
SPARK_SUBMIT=${HOME}/spark/bin/spark-submit
APP_NAME="org.apache.spark.example.Word2VecTrainer"

## paramters
__SPARK_MASTER="spark://${SPARK_MASTER_IP}:7077"
__MAX_CPU_CORES="160"

$SPARK_SUBMIT  \
    --master "$__SPARK_MASTER" \
    --class $APP_NAME \
    --total-executor-cores $__MAX_CPU_CORES \
    $__JAR "$__SPARK_MASTER" $__MAX_CPU_CORES
