package com.scottlogic.pod.spark.playground;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class LearnJoins {

    private SparkSession spark;
    private JavaSparkContext jsc;

    LearnJoins(SparkSession spark, JavaSparkContext jsc) {
        this.spark = spark;
        this.jsc = jsc;
    }

    public void run() {

        Dataset<Row> employees = spark.read()
            .option("header", "true")
            .csv("./data/joins/employees.csv")
            .toDF();

        Dataset<Row> departments = spark.read()
            .option("header", "true")
            .csv("./data/joins/departments.csv")
            .toDF();
            
        Dataset<Row> projects = spark.read()
            .option("header", "true")
            .csv("./data/joins/projects.csv")
            .toDF();

        Dataset<Row> salaries = spark.read()
            .option("header", "true")
            .csv("./data/joins/salaries.csv")
            .toDF();
        
        Dataset<Row> ages = spark.read()
            .option("header", "true")
            .csv("./data/joins/ages.csv")
            .toDF();
        
        /*
         * Task:
         * Produce a table of each employee and the department they work under, including those with no listed department
         */



        /*
         * Task:
         * Get a list of the ids of the employees who don't have a listed salary
         */
        
        
         
        /*
         * Task:
         * Make a table of all departments and the total salary they cost
         */
        


        /*
         * Task:
         * For each project get each department which has at least one employee working on it
         */
        
        
         
        /*
         * Task:
         * Use the ages dataframe to get the name of the employee working on Project M
         */
     
        

        /*
         * Task:
         * experiment with hints to use the various strategies. Verify they have worked using df.explain()
         */



    }    
}
