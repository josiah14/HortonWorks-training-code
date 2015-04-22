#!/bin/bash

kafka-topics.sh --create --topic sentences --replication-factor 3 --partitions 4 --zookeeper namenode:2181,resourcemanager:2181,hiveserver:2181

