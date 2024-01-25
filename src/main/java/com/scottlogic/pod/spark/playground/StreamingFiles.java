package com.scottlogic.pod.spark.playground;

import org.apache.spark.sql.Row;

import java.util.Arrays;
import java.util.concurrent.TimeoutException;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.Dataset;

public class StreamingFiles {
        private SparkSession spark;

        StreamingFiles(SparkSession spark) {
                this.spark = spark;
        }

        public void run() throws TimeoutException, StreamingQueryException {

                StructType schema = DataTypes.createStructType(Arrays.asList(
                                DataTypes.createStructField("id", DataTypes.IntegerType, true),
                                DataTypes.createStructField("favNumber", DataTypes.IntegerType, true),
                                DataTypes.createStructField("name", DataTypes.StringType, true)

                ));

                Dataset<Row> df = spark.readStream()
                                .schema(schema)
                                .option("multiline", "true")
                                .json("./data/streamData/currentData");

                df.writeStream()
                                .format("console")
                                .outputMode("append")
                                .start();

                Dataset<Row> groupDF = df.select("favNumber")
                                .groupBy("favNumber")
                                .count();

                groupDF.writeStream()
                                .format("console")
                                .outputMode("complete")
                                .start()
                                .awaitTermination();

        }
}
