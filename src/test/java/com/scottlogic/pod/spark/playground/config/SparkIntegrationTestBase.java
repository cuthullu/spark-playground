package com.scottlogic.pod.spark.playground.config;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.junit.After;

import java.util.Optional;

public class SparkIntegrationTestBase {
    private Optional<SparkConf> sparkConf = Optional.empty();
    private Optional<JavaSparkContext> javaSparkContext = Optional.empty();
    private Optional<SparkSession> sparkSession = Optional.empty();

    protected SparkSession getSpark() {
        if (!sparkSession.isPresent()) {
            sparkSession = Optional.of(createSparkSession());
        }
        return sparkSession.get();
    }

    protected JavaSparkContext getJsc() {
        if (!javaSparkContext.isPresent()) {
            javaSparkContext = Optional.of(createJavaSparkContext());
        }
        return javaSparkContext.get();
    }

    protected SparkConf getConf() {
        if (!sparkConf.isPresent()) {
            sparkConf = Optional.of(createConf());
        }
        return sparkConf.get();
    }

    protected SparkConf createConf() {
        return new SparkConf()
                .setMaster("local[*]")
                .setAppName(getClass().getSimpleName());
    }

    protected SparkSession createSparkSession() {
        return SparkSession.builder().config(getConf())
                .getOrCreate();
    }

    protected JavaSparkContext createJavaSparkContext() {
        return new JavaSparkContext(getSpark().sparkContext());
    }

    @After
    public void stopSparkContext() {
        sparkSession.ifPresent(c -> c.close());
    }

}

