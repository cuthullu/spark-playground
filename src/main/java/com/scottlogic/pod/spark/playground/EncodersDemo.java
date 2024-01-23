package com.scottlogic.pod.spark.playground;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import com.scottlogic.pod.spark.playground.models.Person;

public class EncodersDemo {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("SparkPlayground").setMaster("local[*]");

        SparkSession spark = SparkSession.builder()
                .config(conf)
                .getOrCreate();

        /*
         * Basic int dataset with int encoder
         */
        List<Integer> intList = Arrays.asList(10, 20, 30, 33);
        Dataset<Integer> ints = spark.createDataset(intList, Encoders.INT());

        ints.map((MapFunction<Integer, Integer>) (Integer i) -> i + 1,
                Encoders.INT()).show();

        int total = ints.reduce((ReduceFunction<Integer>) (Integer x, Integer i) -> x
                + i);

        System.out.println(total);

        /**
         * Encode to a custom class
         */

        String csvColumnName = "User Id";
        String beanFieldName = "userId";
        Dataset<Row> peopleDf = spark
                .read()
                .schema(DataTypes.createStructType(DataSchemas.peopleSchema))
                .option("header", "true")
                .csv("./data/people-100000.csv").select(csvColumnName);

        Dataset<Person> personBeanDataset = peopleDf
                .select(
                        peopleDf.col(csvColumnName).alias(beanFieldName))
                .as(Encoders.bean(Person.class));

        personBeanDataset.show();

        personBeanDataset.filter((FilterFunction<Person>) foo -> foo.getUserId().contains("21")).show();

        /*
         * Return to a generic dataFrame
         */

        Dataset<Row> newPeopleDf = personBeanDataset.toDF();
        newPeopleDf.printSchema(); // userId: string (nullable = true)

        /*
         * Todo: try to get Encoders.javaSerialization and Encoders.kryo examples to
         * work properly
         */

        spark.stop();
    }
}
