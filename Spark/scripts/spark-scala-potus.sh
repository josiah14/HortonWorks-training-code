#!/bin/bash

cd /usr/lib/spark

./bin/spark-submit --class spark.ScalaWhiteHouseVisitorAnalysis --master yarn-client --num-executors 3 --driver-memory 512m --executor-memory 512m --executor-cores 1 /root/spark-app.jar 
