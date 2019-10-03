#!/usr/bin/env bash

# presentation point:  --roadsCount 50000 --pointsCount 8000 --meshIntervalMeters 10
# A (naive): --mesh false --withIndex false
# B (index): --mesh false --withIndex true
# C (mesh): --mesh true --withIndex true

java -jar target/geoBenchmark.jar --testName singleTest --mesh false --withIndex false --roadsCount 50000 --pointsCount 8000  --meshIntervalMeters 10 --cores 8 --roadPartitions 1 --indexType RTREE --spatialPartitioning RTREE
