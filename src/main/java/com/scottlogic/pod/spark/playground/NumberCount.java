package com.scottlogic.pod.spark.playground;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class NumberCount {
    private SparkSession spark;
    private JavaSparkContext jsc;

    NumberCount(SparkSession spark, JavaSparkContext jsc) {
        this.spark = spark;
        this.jsc = jsc;
    }

    public void count() {
        /*
         * Here we create an RDD with a list of numbers
         * An RDD (Resilient Distributed Dataset) is a fundamental data
         * structure that represents a distributed collection of data elements that can
         * be processed in parallel. RDDs are the building blocks of Spark and provide
         * an abstraction for distributed data processing.
         */
        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);

        JavaRDD<Integer> rdd = jsc.parallelize(data);

        /*
         * We can perform transformations and actions on RDDs
         * Like rdd.map, rdd.filter or rdd.reduce
         */

        /*
         * Task: map the values to themselves plus one
         */
        JavaRDD<Integer> rddPlusOne;

        /*
         * Task: total up the values in the rddPlusOne
         */
        int sum = 0;

        System.out.println(sum);

        /*
         * Now we turn the RDD into a Dataset.
         * A Dataset is an abstraction layer on top of RDDs.
         * 
         * It represents a distributed collection of data organized into
         * named columns. It provides a type-safe, object-oriented programming interface
         * and is designed to provide the benefits of both the structured data
         * processing capabilities of DataFrames and the type safety of RDDs.
         */

        String columnName = "value";
        List<StructField> fields = Arrays
                .asList(DataTypes.createStructField(columnName, DataTypes.IntegerType, true));

        StructType schema = DataTypes.createStructType(fields);

        JavaRDD<Row> rowRDD = rdd.map(value -> RowFactory.create(value));
        Dataset<Row> df = spark.createDataFrame(rowRDD, schema);

        df.show();

        /**
         * We can perform the same actions and transformations as on the RDD, but use a
         * different API to do so.
         * 
         * One way is through user defined functions.
         * 
         * Task: Define a Spark Java function return the square of the number and use it
         * to create a new column of squared values
         * 
         */

        UserDefinedFunction squareUDF; // = functions.udf();

        // df = df.withColumn("value_squared_with_udf", ....)
        // df.show();

        /**
         * Task: Now calculate the total of the squared column from the DataSet using
         * the dataset api
         */

        // Row total =
        // System.out.println(total);

        /*
         * While user defined functions allow us to do just about anything, they are a
         * block box to spark.
         * So it cannot optimise them during use, making them poor for performance.
         * 
         * Instead spark provides functions that can do most operations that we could
         * want
         * 
         * Task: Use native spark functions to produce another value_squared_with_spark
         * column
         */

        df = df.withColumn("value_squared_with_function", df.col("value").multiply(df.col("value")));
        df.show();

        /**
         * Task: And recalculate your total. See if they match 
         */

    }
}
