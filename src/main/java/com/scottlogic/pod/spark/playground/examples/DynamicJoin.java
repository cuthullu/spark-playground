package com.scottlogic.pod.spark.playground.examples;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class DynamicJoin {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .set("spark.eventLog.enabled", "true")
                .set("spark.sql.adaptive.enabled", "true") // Flip this to see the effect it has on
                .set("spark.sql.adaptive.autoBroadCastJoinThreshold", "10485760b")
                .set("spark.sql.autoBroadcastJoinThreshold", "10485760b")
                // .set("spark.sql.shuffle.partitions", "10") // Reduced from the default 200
                .setAppName("SparkPlayground")
                .setMaster("spark://localhost:7077");

        SparkSession spark = SparkSession.builder()
                .config(conf)
                .getOrCreate();

        Dataset<Row> peopleOrgsDF = spark.read()
                .option("header", "true")
                .csv("./data/peoples-organisations.csv")
                .toDF();

        peopleOrgsDF.printSchema();

        Dataset<Row> peopleDF = spark.read()
                .option("header", "true")
                .csv("./data/people-100000.csv");

        peopleDF.printSchema();

        peopleDF
                .join(peopleOrgsDF, "User Id")
                // .filter(peopleOrgsDF.col("Organization Id").equalTo("ccBcC32adcbc530"))
                .show(Integer.MAX_VALUE);

    }

    private static Column rand(int size) {
        return functions.floor(functions.rand().$times(size));
    }

}
