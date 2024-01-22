package com.scottlogic.pod.spark.playground;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

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
         * using the DF provided, create a new table of the student objects and their
         * school
         * year
         * 
         * The functions that you may/will need to use for this are:
         * - functions.explode(...)
         */
        // uses explode to seperate out the list
        Dataset<Row> allStudents = df.select(df.col("yearNum"),
                functions.explode(df.col("students")).alias("students"));
        allStudents.show();

        /*
         * Task:
         * Building off of the table just created, seperate each student object so that
         * you now have a table of student names, ages and school years
         * 
         * The functions that you may/will need to use for this are:
         * - .col(...).getField(...)
         */

        // uses getField to break down the objects

        allStudents = allStudents.select(allStudents.col("yearNum"),
                allStudents.col("students").getField("studentName").as("Name"),
                allStudents.col("students").getField("studentAge").as("Age"));
        allStudents.show();

        /*
         * Task:
         * And finaly, building off of the last table, the mean age of each school year
         * and the mode of each school year
         * 
         * The functions that you may/will need to use for this are:
         * - functions.groupBy(...)
         * - .agg(...)
         * - functions.mean(...) / functions.avg(...)
         * - functions.mode(...)
         */

        // finds averages for each year
        allStudents.groupBy("yearNum").agg(functions.avg(allStudents.col("Age")),
                functions.mode(allStudents.col("Age"))).show();

        /*
         * Task:
         * Produce a new table (from the original DF) with the two columns, teacher name
         * and the Key Stage they
         * teach
         * 
         * https://thinkstudent.co.uk/school-year-groups-key-stages/
         * 
         * The functions that you may/will need to use for this are:
         * - .when(...)
         * - col(...).lt(...), col(...).leq(...), col(...).gt(...), col(...).geq(...)
         * - .and(...)
         * - .otherwise
         */

        Dataset<Row> teacherDf = df.select("teacherName", "yearNum");
        teacherDf = teacherDf.withColumn("KS",
                functions.when(teacherDf.col("yearNum").lt(3), "KS1")
                        .when(teacherDf.col("yearNum").geq(3).and(teacherDf.col("yearNum").leq(6)), "KS2")
                        .when(teacherDf.col("yearNum").gt(6), "KS3").otherwise("Error"));

        teacherDf.show();

        /*
         * Task:
         * Produce a new table containing the year, class number and class size
         * 
         * The functions that you may/will need to use for this are:
         * - functions.size(...)
         */

        df.select("yearNum", "classNum", "students").withColumn("classSize", functions.size(df.col("students")))
                .drop("students").show();

        /*
         * From the original dataframe, using functions.slice, create a table of each
         * year number and the first student listed in the classe
         * 
         * The functions that you may/will need to use for this are:
         * - functions.slice(...)
         */

        // shows the names of each kid and their class
        Dataset<Row> classStudents = df.select(df.col("yearNum"),
                df.col("students").getField("studentName").as("Names"));

        // Takes the element of the list selected (starts at 1) and takes the number of
        // elements selected (length) and provides that list. This could be useful to
        // show/select a specific number of elements
        classStudents.select(classStudents.col("yearNum"),
                functions.explode(functions.slice(classStudents.col("Names"), 1, 1)).as("firstStudent")).show();

    }

}
