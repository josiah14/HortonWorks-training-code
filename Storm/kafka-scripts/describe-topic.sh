#!/bin/bash

kafka-topics.sh --describe --topic sentences --zookeeper namenode:2181,resourcemanager:2181,hiveserver:2181

