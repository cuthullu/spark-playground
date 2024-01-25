package com.scottlogic.pod.spark.playground;

import org.apache.spark.sql.Row;

import java.util.Arrays;
import java.util.concurrent.TimeoutException;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;

public class StreamingText {
    private SparkSession spark;

    StreamingText(SparkSession spark) {
        this.spark = spark;
    }

    public void run() throws TimeoutException, StreamingQueryException {
        // run nc -lk 9999 and input some text
        Dataset<Row> inputData = spark.readStream()
                .format("socket")
                .option("host", "localhost")
                .option("port", 9999)
                .load();

        Dataset<String> words = inputData.as(Encoders.STRING()).flatMap(
                (FlatMapFunction<String, String>) x -> Arrays.asList(x.split(" ")).iterator(), Encoders.STRING());

        Dataset<Row> wordCounts = words.groupBy("value").count();

        StreamingQuery query = wordCounts.writeStream()
                .outputMode("complete")
                .format("console")
                .start();

        query.awaitTermination();

    }
}
