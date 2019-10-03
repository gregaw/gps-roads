#!/usr/bin/env bash
for i in  10000 # 20000 50000 100000 # 200000
do
for j in 1000 # 2000 4000 8000 #1000 3000
do
for cores in 1 #4 8
do

for roadPartitions in 1 #4
do


for indexType in RTREE #QUADTREE
do
for spatialPartition in RTREE
do
for index in true false
do
    java -jar target/geoBenchmark.jar --roadsCount $i --pointsCount $j --cores $cores --withIndex $index --roadPartitions $roadPartitions --indexType $indexType --spatialPartitioning $spatialPartition
done
done
done


for interval in 10 #100 1000
do
    java -Xmx12G -jar target/geoBenchmark.jar --roadsCount $i --pointsCount $j --cores $cores --roadPartitions $roadPartitions --mesh true --meshIntervalMeters $interval
done


done
done
done
done

