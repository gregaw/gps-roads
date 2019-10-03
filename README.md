# GeoSpark - mapping GPS to road graph
(optimising delivery optimisations)

##Setup and run (for Mac)

- build the project with sbt assembly
- download data using ./getdata.sh (you will need about 200MB disk space)
- run the benchmark using ./batchRun.sh
- store the results using ./namerun.sh TESTNAME
- analyse results.csv or reports.csv

##FAQ
- Why do you use RDD rather than DataFrames geo-spark interface?

Firstly, we benchmark the use of indices and discretized space models (Mesh) so it doesn't matter too much if we're using RDDs or DataFrames.
Secondly, GeoSpark themselves suggest to do just that: "we are not able to expose all features to SparkSQL" in https://datasystemslab.github.io/GeoSpark/tutorial/benchmark/.