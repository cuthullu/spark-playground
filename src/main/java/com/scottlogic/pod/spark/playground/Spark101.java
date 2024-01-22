package com.scottlogic.pod.spark.playground;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

/*
 * Covers:
 * 
 * Object and array manipulation :-
 * functions.explode(...)
 * .col(...).getField(...)
 * 
 * Data collecting :-
 * - .groupBy(...)
 * - .agg(...)
 * - functions.mean(...) / functions.avg(...)
 * - functions.mode(...)
 * 
 * Data manipulation :-
 * - .when(...)
 * - col(...).lt(...), col(...).leq(...), col(...).gt(...), col(...).geq(...)
 * - .and(...)
 * - .otherwise(...)
 * 
 * Slightly unusual :-
 * - functions.size(...)
 * - functions.slice(...)
 */

public class Spark101 {

    private SparkSession spark;
    private JavaSparkContext jsc;

    Spark101(SparkSession spark, JavaSparkContext jsc) {
        this.spark = spark;
        this.jsc = jsc;
    }

    public void run() {

        Dataset<Row> df = spark.read()
                .option("multiline", "true")
                .json("./data/school-classes.json")
                .toDF();

        df.show();
        df.printSchema();

        /*
         * Task:
         * Using the DF provided, create a new table of the student objects and their
         * school year
         * 
         * The functions that you may/will need to use for this are:
         * - functions.explode(...)
         */

        /*
         * Task:
         * Building off of the table just created, seperate each student object so that
         * you now have a table of student names, ages and school years
         * 
         * The functions that you may/will need to use for this are:
         * - .col(...).getField(...)
         */

        /*
         * Task:
         * Following on, what is the mean age and the mode age of each school year
         * 
         * The functions that you may/will need to use for this are:
         * - functions.groupBy(...)
         * - .agg(...)
         * - functions.mean(...) / functions.avg(...)
         * - functions.mode(...)
         */

        /*
         * Task:
         * Please create a new Table of the school teachers and the Key stage they teach
         * 
         * https://thinkstudent.co.uk/school-year-groups-key-stages/
         * 
         * The functions that you may/will need to use for this are:
         * - .when(...)
         * - col(...).lt(...), col(...).leq(...), col(...).gt(...), col(...).geq(...)
         * - .and(...)
         * - .otherwise
         */

        /*
         * Task:
         * Produce a new table containing the year, class number and class size
         * 
         * The functions that you may/will need to use for this are:
         * - functions.size(...)
         */

        /*
         * Task:
         * Finaly, using functions.slice, create a table of each year number, the class
         * number and the name of the first student listed in each class
         * 
         * The functions that you may/will need to use for this are:
         * - functions.slice(...)
         */

    }

}
