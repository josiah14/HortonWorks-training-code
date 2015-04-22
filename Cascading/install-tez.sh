#!/bin/bash

hadoop fs -mkdir -p /apps/tez-0.5.0
hadoop fs -put -f tez/tez-0.5.0.tar.gz /apps/tez-0.5.0
if [[ ! -d /opt/tez ]];
then
  mkdir /opt/tez
  tar -C /opt/tez -zxf tez/tez-0.5.0-minimal.tar.gz
fi
