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
        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);

        JavaRDD<Integer> rdd = jsc.parallelize(data);

        /*
         * Task: map the values to themselves plus one
         */
        JavaRDD<Integer> rddPlusOne;

        /*
         * total up the values in the rddPlusOne
         */
        int sum = 0;

        System.out.println(sum);

        /*
         * Now we turn the RDD into a DataSet
         */

        String columnName = "value";
        List<StructField> fields = Arrays
                .asList(DataTypes.createStructField(columnName, DataTypes.IntegerType, true));

        StructType schema = DataTypes.createStructType(fields);

        JavaRDD<Row> rowRDD = rdd.map(value -> RowFactory.create(value));
        Dataset<Row> df = spark.createDataFrame(rowRDD, schema);

        // df.show();


        /**
         * Task: Define a Spark Java function return the square of the number and use it to
         * create a new column of squared values
         */
        UserDefinedFunction squareUDF; // = functions.udf();

        // df = df.withColumn("value_squared", ....)

        /**
         * Task: Calculate the total of the squared column from the DataSet
         */
        // Row total =
        // System.out.println(total);

    }
}
