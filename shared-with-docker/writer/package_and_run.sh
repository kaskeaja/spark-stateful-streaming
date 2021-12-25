#!/bin/bash
set -e

sbt compile
sbt package
./run_in_spark.sh