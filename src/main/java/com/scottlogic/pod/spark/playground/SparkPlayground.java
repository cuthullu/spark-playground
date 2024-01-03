package com.scottlogic.pod.spark.playground;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkPlayground {
    private static final Logger logger = LoggerFactory.getLogger(SparkPlayground.class);

    public static void main(String[] args) {
        // Create a Spark session
        logger.info("Starting spark legend demo app");

        SparkSession spark = SparkSession.builder()
                .appName("SparkPlayground")
                .master("spark://localhost:7077") // You can set your Spark master URL here
                // .config("spark.dynamicAllocation.enabled", "false")
                // .master("local[*]")
                .getOrCreate();

        SparkContext cs = spark.sparkContext();
        JavaSparkContext jcs = new JavaSparkContext(cs);
        
        /*
         * Each of these can be uncommented to run them
         */

        // NumberCount numberCount = new NumberCount(spark, jcs);
        // numberCount.count();

        // WordCount wordCount = new WordCount(spark, jcs);
        // wordCount.count();

        // CSVReader csvReader = new CSVReader(spark, jcs);
        // csvReader.run();

        // // Stop the Spark session
        logger.info("Stopping spark legend demo app");
        spark.stop();
        // keepAlive();
    }

    private static void keepAlive() {
        while (true) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}