package com.scottlogic.pod.spark.playground.examples;

import java.util.Iterator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class DynamicCoalescing {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .set("spark.eventLog.enabled", "true")
                .set("spark.sql.adaptive.coalescePartitions.enabled", "false") // Flip this to see the effect it has on
                                                                               // the number of partitions
                .set("spark.sql.shuffle.partitions", "10") // Reduced from the default 200
                .setAppName("SparkPlayground")
                .setMaster("local[*]");

        SparkSession spark = SparkSession.builder()
                .config(conf)
                .getOrCreate();

        Dataset<Row> orgDf = spark.read()
                .option("header", "true")
                .csv("./data/organisations-100.csv");

        /*
         * adding a founding decade column to give us a column that give a sensible
         * amount of groups.
         * Should result in 6 different decades to group by
         */
        orgDf = orgDf.withColumn("founding_decade",
                functions.concat(orgDf.col("Founded").substr(3, 1), functions.lit("0")));

        orgDf.show();
        System.out.println("Num. partitions when pre-grouping: " + orgDf.rdd().getNumPartitions());

        /*
         * group by the founding decade to force a shuffle
         */
        Dataset<Row> orgDfGrouped = orgDf.groupBy("founding_decade").count();

        System.out.println("Num. partitions post-grouping: " + orgDfGrouped.rdd().getNumPartitions());

        /*
         * prints out the size of each partition
         */
        orgDfGrouped.foreachPartition((ForeachPartitionFunction<Row>) (Iterator<Row> iterator) -> {
            int pSize = 0;
            while (iterator.hasNext()) {
                pSize++;
                iterator.next();
            }
            System.out.println("Partition size: " + pSize);
        });

        /*
         * By flipping "coalescePartitions.enabled" you should see that
         * when disabled: we have 10 partitions total, holding between 0-2 rows each
         * when enabled: we have 1 partition only, holding all 6 rows
         */

    }

}
