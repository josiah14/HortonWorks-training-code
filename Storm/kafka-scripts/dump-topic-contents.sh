#!/bin/bash

kafka-console-consumer.sh --zookeeper namenode:2181,resourcemanager:2181,hiveserver:2181 --topic sentences --from-beginning
