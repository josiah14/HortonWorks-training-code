#!/bin/bash

hadoop fs -mkdir -p cstocks
hadoop fs -mkdir -p cdividends
hadoop fs -put -f NYSE_daily_prices_A.csv cstocks
hadoop fs -put -f NYSE_dividends_A.csv cdividends

