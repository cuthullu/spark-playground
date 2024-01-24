package com.scottlogic.pod.spark.playground.cucumber.steps;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.assertj.core.api.Assertions;

import com.scottlogic.pod.spark.playground.WordCount;
import com.scottlogic.pod.spark.playground.config.SparkIntegrationTestBase;

import io.cucumber.java.Before;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;

public class WordCountSteps extends SparkIntegrationTestBase {
    private String path;
    private WordCount wordCount;
    private SparkSession spark;
    private JavaSparkContext jsc;

    @Before
    public void before() {
        this.spark = getSpark();
        this.jsc = getJsc();
    }

    @Given("a new wordCount")
    public void initiliseWordCount() {
        this.wordCount = new WordCount(spark, jsc);
    }

    @When("I supply the path {string}")
    public void setPath(String path) {
        this.path = path;
    }

    @Then("I should get a count of {long}")
    public void correctCountTest(Long expectedValue) {
        assertEquals(expectedValue, wordCount.countDistinctWords(this.path));
    }

    @Then("I should not get a count of {long}")
    public void incorrectCountTest(Long incorrectValue) {
        assertNotEquals(incorrectValue, wordCount.countDistinctWords(this.path));
    }

    @Then("I should get an exception")
    public void nullCountTest() {
        Assertions.assertThatThrownBy(() -> {
            wordCount.countDistinctWords(null);
        }).isInstanceOf(NullPointerException.class);
    }
}
