#!/bin/bash
set -e

~/spark-3.1.2-bin-hadoop3.2/bin/spark-submit --class DeltaLakeSource.DeltaLakeWriter --packages io.delta:delta-core_2.12:1.0.0 --master local[1] target/scala-2.12/deltalakewriter_2.12-0.1.jar
