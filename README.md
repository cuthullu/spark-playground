
Basic spark boiler plate to run some examples. 

Each of `NumberCount`, `WordCount`, and `CSVReader` contain some code and tasks that can be completed. 

## To run - 

click run in the editor/IDE

### With spark-submit

Package with 

```
mvn clean package
```

Submit with 

```
spark-submit \
  --name spark-demo \
  --class com.scottlogic.pod.spark.playground.SparkPlayground \
  ./spark-playground/target/spark-playground-1.0-SNAPSHOT.jar
```


### To spin up with a local cluster 

Install spark locally with SDKMan  `sdk install spark 3.5.0` (version needs to match spark versions in the pom)
and ensure this is the default/set version for your shells for the following commands

In separate shell instances.

Start a master instance with-

`spark-class org.apache.spark.deploy.master.Master --host localhost`

Then spin up a couple of worker nodes, running the following in their own shells for each node. 

`spark-class org.apache.spark.deploy.worker.Worker spark://localhost:7077  --host localhost`

Modify `SparkPlayground` to use `.master("spark://localhost:7077")` instead of `.master("local[*]")`
