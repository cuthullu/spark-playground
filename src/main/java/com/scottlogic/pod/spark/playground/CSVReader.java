package com.scottlogic.pod.spark.playground;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class CSVReader {
    private SparkSession spark;
    private JavaSparkContext jsc;

    public CSVReader(SparkSession spark, JavaSparkContext jsc) {
        this.spark = spark;
        this.jsc = jsc;
    }

    public void run() {

        Dataset<Row> cabsDF = spark.read()
                .csv("./data/people-100000.csv")
                .toDF();

        /*
         * Task: Notice the first row of the table is the header line, fix the above
         * code to read the header properly
         */
        cabsDF.show();
        cabsDF.printSchema();

        /*
         * Task: The schema is current all nullable strings, fix it by creating a new
         * schema that correctly matches the data types
         */

        /*
         * Task: Add an "age" column, calculating the current age of each person
         */

        /*
         * Task: Calculate the combined age all people with the same Job Titles
         */
    }

    public long countCsv(String filePath) {
        return spark.read().csv(filePath).count();
    }
}
